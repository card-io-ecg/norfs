use crate::{
    ll::objects::{MetadataObjectHeader, ObjectReader},
    medium::StorageMedium,
    Storage, StorageError,
};

/// File reader
pub struct Reader<M>
where
    M: StorageMedium,
{
    meta: MetadataObjectHeader<M>,
    current_object: Option<ObjectReader<M>>,
}

impl<M> Reader<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    pub(crate) fn new(meta: MetadataObjectHeader<M>) -> Self {
        Self {
            meta,
            current_object: None,
        }
    }

    async fn select_next_object(&mut self, medium: &mut M) -> Result<(), StorageError> {
        self.current_object = if let Some(object) = self.meta.next_object_location(medium).await? {
            Some(ObjectReader::new(object, medium, false).await?)
        } else {
            None
        };

        Ok(())
    }

    /// Reads data from the current position in the file.
    ///
    /// Returns the number of bytes read.
    ///
    /// `storage` must be the same storage that was used to open the file.
    pub async fn read(
        &mut self,
        storage: &mut Storage<M>,
        mut buf: &mut [u8],
    ) -> Result<usize, StorageError> {
        log::debug!("Reader::read(len = {})", buf.len());

        let medium = &mut storage.medium;

        if self.current_object.is_none() {
            self.select_next_object(medium).await?;
        }

        let len = buf.len();

        loop {
            let Some(reader) = self.current_object.as_mut() else {
                // EOF
                break;
            };

            let read = reader.read(medium, buf).await?;
            buf = &mut buf[read..];

            if buf.is_empty() {
                // Buffer is full
                break;
            }

            self.select_next_object(medium).await?;
        }

        Ok(len - buf.len())
    }

    pub async fn read_all(
        &mut self,
        storage: &mut Storage<M>,
        buf: &mut [u8],
    ) -> Result<(), StorageError> {
        log::debug!("Reader::read_all(len = {})", buf.len());

        if !buf.is_empty() {
            let read = self.read(storage, buf).await?;
            if read == 0 {
                return Err(StorageError::EndOfFile);
            }
        }

        Ok(())
    }

    pub async fn read_array<const N: usize>(
        &mut self,
        storage: &mut Storage<M>,
    ) -> Result<[u8; N], StorageError> {
        let mut buf = [0u8; N];

        self.read_all(storage, &mut buf).await?;

        Ok(buf)
    }

    pub async fn read_one(&mut self, storage: &mut Storage<M>) -> Result<u8, StorageError> {
        let buf = self.read_array::<1>(storage).await?;

        Ok(buf[0])
    }
}

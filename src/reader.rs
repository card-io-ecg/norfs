use embedded_io::blocking::ReadExactError;

use crate::{
    debug,
    ll::objects::{MetadataObjectHeader, ObjectReader},
    medium::StorageMedium,
    storable::{LoadError, Loadable},
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
        debug!("Reader::read(len = {})", buf.len());

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
        debug!("Reader::read_all(len = {})", buf.len());

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

    pub async fn read_loadable<'a, T: Loadable>(
        &'a mut self,
        storage: &'a mut Storage<M>,
    ) -> Result<T, LoadError<StorageError>> {
        T::load(&mut BoundReader {
            reader: self,
            storage,
        })
        .await
    }
}

pub struct BoundReader<'a, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    reader: &'a mut Reader<M>,
    storage: &'a mut Storage<M>,
}

impl<'a, M> BoundReader<'a, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, StorageError> {
        self.reader.read(self.storage, buf).await
    }

    pub async fn read_all(&mut self, buf: &mut [u8]) -> Result<(), StorageError> {
        self.reader.read_all(self.storage, buf).await
    }

    pub async fn read_one(&mut self) -> Result<u8, StorageError> {
        self.reader.read_one(self.storage).await
    }

    pub async fn read_array<const N: usize>(&mut self) -> Result<[u8; N], StorageError> {
        self.reader.read_array(self.storage).await
    }
}

impl<M> embedded_io::Io for BoundReader<'_, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    type Error = StorageError;
}

impl<M> embedded_io::asynch::Read for BoundReader<'_, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Reader::read(self.reader, self.storage, buf).await
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError<Self::Error>> {
        match Reader::read_all(self.reader, self.storage, buf).await {
            Ok(_) => Ok(()),
            Err(StorageError::EndOfFile) => Err(ReadExactError::UnexpectedEof),
            Err(e) => Err(ReadExactError::Other(e)),
        }
    }
}

use core::marker::PhantomData;

use crate::{
    ll::{
        blocks::{BlockInfo, BlockType},
        objects::{MetadataObjectHeader, ObjectIterator, ObjectReader, ObjectState},
    },
    medium::StorageMedium,
    reader::Reader,
    Storage, StorageError,
};

pub struct DirEntry<M>
where
    M: StorageMedium,
{
    metadata: MetadataObjectHeader<M>,
}

impl<M> DirEntry<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    // pub fn is_file(&self) -> bool {
    //     self.metadata.object.object_type().unwrap() == ObjectType::FileMetadata
    // }
    //
    // pub fn is_dir(&self) -> bool {
    //     false
    // }

    pub async fn name<'s>(
        &self,
        storage: &mut Storage<M>,
        buffer: &'s mut [u8],
    ) -> Result<&'s str, StorageError> {
        let mut filename_object =
            ObjectReader::new(self.metadata.filename_location, &mut storage.medium, false).await?;

        let read = filename_object.read(&mut storage.medium, buffer).await?;

        if filename_object.remaining() > 0 {
            return Err(StorageError::InsufficientBuffer);
        }

        core::str::from_utf8(&buffer[..read]).map_err(|_| StorageError::FsCorrupted)
    }

    pub async fn open(self) -> Reader<M> {
        Reader::new(self.metadata)
    }

    pub async fn delete(self, storage: &mut Storage<M>) -> Result<(), StorageError> {
        storage.delete_file_at(self.metadata.location()).await
    }

    pub async fn size(&self, storage: &mut Storage<M>) -> Result<usize, StorageError> {
        let mut meta = self.metadata.clone();

        let mut size = 0;
        while let Some(chunk) = meta.next_object_location(&mut storage.medium).await? {
            let data_object = ObjectReader::new(chunk, &mut storage.medium, false).await?;
            size += data_object.len();
        }

        Ok(size)
    }
}

pub struct ReadDir<M>
where
    M: StorageMedium,
{
    current_object: Option<ObjectIterator>,
    _marker: PhantomData<M>,
}

impl<M> ReadDir<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    pub(crate) fn new(storage: &Storage<M>) -> Self {
        let first_block = storage.blocks.blocks(BlockType::Metadata).next();

        let object = first_block.map(|blk| ObjectIterator::new::<M>(blk.0));

        Self {
            current_object: object,
            _marker: PhantomData,
        }
    }

    /// Reads info about the next file.
    ///
    /// This function will return unpredictable results if the file system is modified
    /// (other than deleting files).
    pub async fn next(
        &mut self,
        storage: &mut Storage<M>,
    ) -> Result<Option<DirEntry<M>>, StorageError> {
        while let Some(reader) = self.current_object.as_mut() {
            match reader.next(&mut storage.medium).await? {
                Some(obj) => {
                    if let ObjectState::Finalized = obj.state() {
                        return Ok(Some(DirEntry {
                            metadata: obj.read_metadata(&mut storage.medium).await?,
                        }));
                    }
                }
                None => {
                    // No more objects in this block, move to the next block.
                    // We're iterating through metadata blocks linearly.
                    self.current_object = storage
                        .blocks
                        .blocks
                        .iter()
                        .skip(reader.block_idx() + 1)
                        .position(BlockInfo::is_metadata)
                        .map(ObjectIterator::new::<M>);
                }
            }
        }

        Ok(None)
    }
}

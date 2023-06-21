use crate::{
    hash_path,
    ll::{
        blocks::BlockType,
        objects::{ObjectLocation, ObjectReader, ObjectType, ObjectWriter},
    },
    medium::{StorageMedium, StoragePrivate},
    Storage, StorageError,
};

pub trait FileDataWriter {
    async fn write<M>(
        &mut self,
        writer: &mut Writer<M>,
        storage: &mut Storage<M>,
    ) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:;

    fn estimate_length(&self) -> usize {
        0
    }
}

pub struct Writer<M>
where
    M: StorageMedium,
{
    metadata: ObjectWriter<M>,
    data: Option<ObjectWriter<M>>,
}

impl<M> Writer<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    async fn write_location(
        &mut self,
        storage: &mut Storage<M>,
        location: ObjectLocation,
    ) -> Result<(), StorageError> {
        let (bytes, byte_count) = location.into_bytes::<M>();

        match self
            .metadata
            .write(&mut storage.medium, &bytes[..byte_count])
            .await
        {
            Ok(()) => return Ok(()),
            Err(StorageError::InsufficientSpace) => {}
            Err(e) => return Err(e),
        }

        log::debug!("Reallocating metadata object");
        // Old object's accounting
        storage.blocks.blocks[self.metadata.location().block]
            .add_used_bytes(self.metadata.total_size());

        let new_file_meta_location = storage
            .find_new_object_location(
                BlockType::Metadata,
                self.metadata.payload_size() + M::object_location_bytes(),
            )
            .await?;

        let new_meta_writer = ObjectWriter::allocate(
            new_file_meta_location,
            ObjectType::FileMetadata,
            &mut storage.medium,
        )
        .await?;

        let old_metadata = core::mem::replace(&mut self.metadata, new_meta_writer);
        let old_metadata = old_metadata.finalize(&mut storage.medium).await?;

        // TODO: seek over object size when added - it should be the first for simplicity

        // Copy old object
        let mut old_object_reader =
            ObjectReader::new(old_metadata.location(), &mut storage.medium, false).await?;

        self.metadata
            .copy_from(&mut old_object_reader, &mut storage.medium)
            .await?;

        // Append location
        self.metadata
            .write(&mut storage.medium, &bytes[..byte_count])
            .await?;

        old_metadata.delete(&mut storage.medium).await?;

        Ok(())
    }

    pub async fn write(
        &mut self,
        mut data: &[u8],
        storage: &mut Storage<M>,
    ) -> Result<usize, StorageError> {
        let mut written = 0;
        while !data.is_empty() {
            let data_object = if let Some(writer) = self.data.as_mut() {
                writer
            } else {
                let data_location = match storage.find_new_object_location(BlockType::Data, 0).await
                {
                    Ok(loc) => loc,
                    Err(StorageError::InsufficientSpace) => return Ok(written),
                    Err(e) => return Err(e),
                };

                let writer = ObjectWriter::allocate(
                    data_location,
                    ObjectType::FileData,
                    &mut storage.medium,
                )
                .await?;

                self.write_location(storage, data_location).await?;

                self.data.insert(writer)
            };

            if data_object.space() > 0 {
                let chunk_len = data_object.space().min(data.len());
                let (store, remaining) = data.split_at(chunk_len);

                data_object.write(&mut storage.medium, store).await?;
                written += store.len();

                data = remaining;
            }

            if data_object.space() == 0 {
                let data_object = self.data.take().unwrap();
                let finalized = data_object.finalize(&mut storage.medium).await?;
                storage.blocks.blocks[finalized.location().block]
                    .add_used_bytes(finalized.total_size());
            }
        }

        Ok(written)
    }

    pub async fn write_all(
        &mut self,
        data: &[u8],
        storage: &mut Storage<M>,
    ) -> Result<(), StorageError> {
        if self.write(data, storage).await? != data.len() {
            return Err(StorageError::InsufficientSpace);
        }

        Ok(())
    }

    pub async fn create(
        path: &str,
        storage: &mut Storage<M>,
        mut op: impl FileDataWriter,
    ) -> Result<(), StorageError> {
        log::debug!("Writer::create(path = {:?})", path);

        if path.contains(&['/', '\\'][..]) {
            log::warn!("Path contains invalid characters");
            return Err(StorageError::InvalidOperation);
        }

        let path_hash = hash_path(path);
        let est_page_count = 1 + storage.estimate_data_chunks(op.estimate_length())?;

        // this is mutable because we can fail mid-writing. 4 bytes to store the path hash
        let file_meta_location = storage
            .find_new_object_location(
                BlockType::Metadata,
                4 + est_page_count * M::object_location_bytes(),
            )
            .await?;

        // Write file name as data object
        let filename_location = storage
            .find_new_object_location(BlockType::Data, path.len())
            .await?;

        storage
            .write_object(filename_location, path.as_bytes())
            .await?;

        // Write a non-finalized header obejct
        let mut meta_writer = ObjectWriter::allocate(
            file_meta_location,
            ObjectType::FileMetadata,
            &mut storage.medium,
        )
        .await?;
        meta_writer
            .write(&mut storage.medium, &path_hash.to_le_bytes())
            .await?;

        storage
            .write_location(&mut meta_writer, filename_location)
            .await?;

        let mut this = Self {
            metadata: meta_writer,
            data: None,
        };

        if let Err(e) = op.write(&mut this, &mut *storage).await {
            storage.delete_file_at(this.metadata.location()).await?;
            return Err(e);
        };

        if let Some(data_object) = this.data {
            let finalized = data_object.finalize(&mut storage.medium).await?;
            storage.blocks.blocks[finalized.location().block]
                .add_used_bytes(finalized.total_size());
        }

        let finalized = this.metadata.finalize(&mut storage.medium).await?;
        storage.blocks.blocks[finalized.location().block].add_used_bytes(finalized.total_size());

        Ok(())
    }

    pub fn bind<'a>(&'a mut self, storage: &'a mut Storage<M>) -> BoundWriter<'a, M>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:,
    {
        BoundWriter {
            writer: self,
            storage,
        }
    }
}

pub struct BoundWriter<'a, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    writer: &'a mut Writer<M>,
    storage: &'a mut Storage<M>,
}

impl<M> BoundWriter<'_, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, StorageError> {
        self.writer.write(buf, self.storage).await
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), StorageError> {
        self.writer.write_all(buf, self.storage).await
    }
}

#[cfg(feature = "embedded-io")]
impl<M> embedded_io::Io for BoundWriter<'_, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    type Error = StorageError;
}

#[cfg(feature = "embedded-io")]
impl<M> embedded_io::asynch::Write for BoundWriter<'_, M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        BoundWriter::write(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        BoundWriter::write_all(self, buf).await
    }
}

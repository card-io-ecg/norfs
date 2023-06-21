use crate::{
    medium::StorageMedium,
    reader::BoundReader,
    storable::{LoadError, Storable},
    writer::BoundWriter,
    StorageError,
};

impl<T, const N: usize> Storable for heapless::Vec<T, N>
where
    T: Storable,
{
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let count = usize::load(reader).await?;

        let mut vec = heapless::Vec::new();

        for _ in 0..count {
            vec.push(T::load(reader).await?)
                .map_err(|_| LoadError::InvalidValue)?;
        }

        Ok(vec)
    }

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        self.len().store(writer).await?;

        for item in self.iter() {
            item.store(writer).await?;
        }

        Ok(())
    }
}

impl<const N: usize> Storable for heapless::String<N> {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let count = usize::load(reader).await?;

        let mut string = heapless::String::new();

        {
            let vec = unsafe { string.as_mut_vec() };
            for _ in 0..count {
                vec.push(u8::load(reader).await?)
                    .map_err(|_| LoadError::InvalidValue)?;
            }

            if core::str::from_utf8(vec).is_err() {
                return Err(LoadError::InvalidValue);
            }
        }

        Ok(string)
    }

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        self.len().store(writer).await?;

        writer.write_all(self.as_bytes()).await
    }
}

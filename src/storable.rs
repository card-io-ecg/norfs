use core::convert::Infallible;

use crate::{
    medium::StorageMedium, reader::BoundReader, varint::Varint, writer::BoundWriter, StorageError,
};

pub enum LoadError {
    InvalidValue,
    Io(StorageError),
}

impl From<Infallible> for LoadError {
    fn from(x: Infallible) -> LoadError {
        match x {}
    }
}

pub trait Storable: Sized {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized;

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized;
}

macro_rules! load_le_bytes {
    ($ty:ty => $proxy:ty) => {
        impl Storable for $ty {
            async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
            where
                M: StorageMedium,
                [(); M::BLOCK_COUNT]: Sized,
            {
                let proxy = <$proxy>::load(reader).await?;
                let value = <$ty>::try_from(proxy)?;
                Ok(value)
            }

            async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
            where
                M: StorageMedium,
                [(); M::BLOCK_COUNT]: Sized,
            {
                let proxy = <$proxy>::from(*self);
                proxy.store(writer).await
            }
        }
    };
}

impl Storable for u8 {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        reader.read_one().await.map_err(LoadError::Io)
    }

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        writer.write_all(&[*self]).await
    }
}

load_le_bytes!(u16 => Varint);
load_le_bytes!(u32 => Varint);
load_le_bytes!(u64 => Varint);
load_le_bytes!(usize => Varint);

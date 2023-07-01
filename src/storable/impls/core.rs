use core::ffi::CStr;

use crate::{
    medium::StorageMedium,
    reader::BoundReader,
    storable::{LoadError, Loadable, Storable},
    varint::{Svarint, Varint},
    writer::BoundWriter,
    StorageError,
};

macro_rules! load_le_bytes {
    ($ty:ty => $proxy:ty) => {
        impl Loadable for $ty {
            async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
            where
                M: StorageMedium,
                [(); M::BLOCK_COUNT]: Sized,
            {
                let proxy = <$proxy>::load(reader).await?;
                let value = <$ty>::try_from(proxy)?;
                Ok(value)
            }
        }

        impl Storable for $ty {
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

impl Loadable for u8 {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        reader.read_one().await.map_err(LoadError::Io)
    }
}

impl Storable for u8 {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        writer.write_all(&[*self]).await
    }
}

impl Loadable for i8 {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let bytes = reader.read_array::<1>().await.map_err(LoadError::Io)?;
        Ok(i8::from_le_bytes(bytes))
    }
}

impl Storable for i8 {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let bytes = self.to_le_bytes();
        writer.write_all(&bytes).await
    }
}

impl Loadable for bool {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        Ok(u8::load(reader).await? != 0)
    }
}

impl Storable for bool {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        (*self as u8).store(writer).await
    }
}

load_le_bytes!(u16 => Varint);
load_le_bytes!(u32 => Varint);
load_le_bytes!(u64 => Varint);
load_le_bytes!(usize => Varint);

load_le_bytes!(i16 => Svarint);
load_le_bytes!(i32 => Svarint);
load_le_bytes!(i64 => Svarint);
load_le_bytes!(isize => Svarint);

impl Loadable for char {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let mut array = [0; 4];
        let len = usize::load(reader).await?;
        reader
            .read_all(&mut array[..len])
            .await
            .map_err(LoadError::Io)?;

        core::str::from_utf8(&array[..len])
            .map_err(|_| LoadError::InvalidValue)
            .and_then(|s| s.chars().next().ok_or(LoadError::InvalidValue))
    }
}

impl Storable for char {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let mut dst = [0; 4];
        let str = self.encode_utf8(&mut dst);
        writer.write_all(str.as_bytes()).await
    }
}

impl<T: Loadable> Loadable for Option<T> {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let value = if bool::load(reader).await? {
            Some(T::load(reader).await?)
        } else {
            None
        };

        Ok(value)
    }
}

impl<T: Storable> Storable for Option<T> {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        match self {
            Some(value) => {
                true.store(writer).await?;
                value.store(writer).await
            }
            None => false.store(writer).await,
        }
    }
}

impl<T: Loadable, E: Loadable> Loadable for Result<T, E> {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let value = if bool::load(reader).await? {
            Ok(T::load(reader).await?)
        } else {
            Err(E::load(reader).await?)
        };

        Ok(value)
    }
}

impl<T: Storable, E: Storable> Storable for Result<T, E> {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        match self {
            Ok(value) => {
                true.store(writer).await?;
                value.store(writer).await
            }
            Err(value) => {
                false.store(writer).await?;
                value.store(writer).await
            }
        }
    }
}

impl Storable for &str {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        writer.write_all(self.as_bytes()).await
    }
}

impl Storable for &CStr {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        writer.write_all(self.to_bytes()).await
    }
}

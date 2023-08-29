use core::ffi::CStr;

use embedded_io::asynch::{Read, Write};

use crate::{
    storable::{LoadError, Loadable, Storable},
    varint::{Svarint, Varint},
};

macro_rules! load_proxied {
    ($ty:ty => $proxy:ty) => {
        impl Loadable for $ty {
            async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
                let proxy = <$proxy>::load(reader).await?;
                let value = <$ty>::try_from(proxy)?;
                Ok(value)
            }
        }

        impl Storable for $ty {
            async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
                let proxy = <$proxy>::from(*self);
                proxy.store(writer).await
            }
        }
    };
}

macro_rules! load_bytes {
    ($ty:ty) => {
        impl Loadable for $ty {
            async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
                let mut bytes = [0; core::mem::size_of::<$ty>()];
                reader.read_exact(&mut bytes).await?;
                Ok(Self::from_le_bytes(bytes))
            }
        }

        impl Storable for $ty {
            async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
                let bytes = self.to_le_bytes();
                writer.write_all(&bytes).await
            }
        }
    };
}

load_bytes!(u8);
load_bytes!(i8);
load_bytes!(f32);
load_bytes!(f64);

impl Loadable for bool {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        Ok(u8::load(reader).await? != 0)
    }
}

impl Storable for bool {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        (*self as u8).store(writer).await
    }
}

load_proxied!(u16 => Varint);
load_proxied!(u32 => Varint);
load_proxied!(u64 => Varint);
load_proxied!(usize => Varint);

load_proxied!(i16 => Svarint);
load_proxied!(i32 => Svarint);
load_proxied!(i64 => Svarint);
load_proxied!(isize => Svarint);

impl Loadable for char {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let mut array = [0; 4];
        let len = usize::load(reader).await?;
        reader.read_exact(&mut array[..len]).await?;

        core::str::from_utf8(&array[..len])
            .map_err(|_| LoadError::InvalidValue)
            .and_then(|s| s.chars().next().ok_or(LoadError::InvalidValue))
    }
}

impl Storable for char {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        let mut dst = [0; 4];
        let str = self.encode_utf8(&mut dst);
        writer.write_all(str.as_bytes()).await
    }
}

impl<T: Loadable> Loadable for Option<T> {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let value = if bool::load(reader).await? {
            Some(T::load(reader).await?)
        } else {
            None
        };

        Ok(value)
    }
}

impl<T: Storable> Storable for Option<T> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
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
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let value = if bool::load(reader).await? {
            Ok(T::load(reader).await?)
        } else {
            Err(E::load(reader).await?)
        };

        Ok(value)
    }
}

impl<T: Storable, E: Storable> Storable for Result<T, E> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
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

impl<T: Storable> Storable for &[T] {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.len().store(writer).await?;
        for item in *self {
            item.store(writer).await?;
        }
        Ok(())
    }
}

impl Storable for &str {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.as_bytes().store(writer).await
    }
}

impl Storable for &CStr {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.to_bytes().store(writer).await
    }
}

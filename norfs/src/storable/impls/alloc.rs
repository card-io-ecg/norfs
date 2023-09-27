use crate::storable::{LoadError, Loadable, Storable};
use embedded_io::asynch::{Read, Write};

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

impl<T: Loadable> Loadable for Vec<T> {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let len = usize::load(reader).await?;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            vec.push(T::load(reader).await?);
        }

        Ok(vec)
    }
}

impl<T: Storable> Storable for Vec<T> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.as_slice().store(writer).await
    }
}

impl<T: Loadable> Loadable for Box<[T]> {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let len = usize::load(reader).await?;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            vec.push(T::load(reader).await?);
        }

        Ok(vec.into_boxed_slice())
    }
}

impl<T: Storable> Storable for Box<[T]> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.as_ref().store(writer).await
    }
}

impl Loadable for Box<str> {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let bytes = Vec::<u8>::load(reader).await?;

        Ok(String::from_utf8(bytes)
            .map_err(|_| LoadError::InvalidValue)?
            .into_boxed_str())
    }
}

impl Storable for Box<str> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.as_bytes().store(writer).await
    }
}

impl Loadable for String {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let bytes = Vec::<u8>::load(reader).await?;

        Ok(String::from_utf8(bytes).map_err(|_| LoadError::InvalidValue)?)
    }
}

impl Storable for String {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.as_str().store(writer).await
    }
}

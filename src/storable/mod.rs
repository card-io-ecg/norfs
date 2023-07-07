pub mod impls;

use core::convert::Infallible;

use embedded_io::{
    asynch::{Read, Write},
    blocking::ReadExactError,
    Error,
};

use crate::{medium::StorageMedium, writer::FileDataWriter, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionError {
    InvalidValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadError<E: Error> {
    InvalidValue,
    UnexpectedEof,
    Io(E),
}

impl<E: Error> From<Infallible> for LoadError<E> {
    fn from(x: Infallible) -> LoadError<E> {
        match x {}
    }
}

impl<E: Error> From<ConversionError> for LoadError<E> {
    fn from(x: ConversionError) -> LoadError<E> {
        match x {
            ConversionError::InvalidValue => LoadError::InvalidValue,
        }
    }
}

impl<E: Error> From<ReadExactError<E>> for LoadError<E> {
    fn from(value: ReadExactError<E>) -> Self {
        match value {
            ReadExactError::UnexpectedEof => LoadError::UnexpectedEof,
            ReadExactError::Other(err) => LoadError::Io(err),
        }
    }
}

pub trait Loadable: Sized {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>>;
}

pub trait Storable: Sized {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error>;
}

impl<T> FileDataWriter for T
where
    T: Storable,
{
    async fn write<M>(
        &self,
        writer: &mut crate::writer::Writer<M>,
        storage: &mut crate::Storage<M>,
    ) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:,
    {
        self.store(&mut writer.bind(storage)).await
    }
}

#[cfg(test)]
mod test {
    use embedded_io::asynch::{Read, Write};

    use crate::{
        storable::{Loadable, Storable},
        test_cases, OnCollision, Storage, StorageMedium,
    };

    use super::LoadError;

    #[derive(Debug, PartialEq, Eq)]
    enum TestType {
        A { foo: u8, bar: u32 },
        B,
        C(u16),
    }

    impl Loadable for TestType {
        async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
            let data = match u8::load(reader).await? {
                0 => TestType::A {
                    foo: u8::load(reader).await?,
                    bar: u32::load(reader).await?,
                },
                1 => TestType::B,
                2 => TestType::C(u16::load(reader).await?),
                _ => return Err(LoadError::InvalidValue),
            };

            Ok(data)
        }
    }

    impl Storable for TestType {
        async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
            match self {
                TestType::A { foo, bar } => {
                    0u8.store(writer).await?;
                    foo.store(writer).await?;
                    bar.store(writer).await?;
                }
                TestType::B => {
                    1u8.store(writer).await?;
                }
                TestType::C(field) => {
                    2u8.store(writer).await?;
                    field.store(writer).await?;
                }
            }

            Ok(())
        }
    }

    test_cases! {
        async fn stored_data_can_be_read_back<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage
                .store_writer(
                    "data",
                    &TestType::C(12345),
                    OnCollision::Overwrite,
                )
                .await
                .expect("Failed to store");

            let data = storage
                .read("data")
                .await
                .expect("Failed to open file")
                .read_loadable::<TestType>(&mut storage)
                .await
                .expect("Failed to read data");

            assert_eq!(data, TestType::C(12345));
        }
    }
}

pub mod impls;

use core::convert::Infallible;

use crate::{
    medium::StorageMedium,
    reader::BoundReader,
    writer::{BoundWriter, FileDataWriter},
    StorageError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadError {
    InvalidValue,
    Io(StorageError),
}

impl From<Infallible> for LoadError {
    fn from(x: Infallible) -> LoadError {
        match x {}
    }
}

pub trait Loadable: Sized {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized;
}

pub trait Storable: Sized {
    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized;
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
    use crate::{
        reader::BoundReader,
        storable::{Loadable, Storable},
        test_cases,
        writer::BoundWriter,
        OnCollision, Storage, StorageError, StorageMedium,
    };

    use super::LoadError;

    #[derive(Debug, PartialEq, Eq)]
    enum TestType {
        A { foo: u8, bar: u32 },
        B,
        C(u16),
    }

    impl Loadable for TestType {
        async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
        where
            M: StorageMedium,
            [(); M::BLOCK_COUNT]: Sized,
        {
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
        async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
        where
            M: StorageMedium,
            [(); M::BLOCK_COUNT]: Sized,
        {
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

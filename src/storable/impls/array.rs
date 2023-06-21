use core::mem::MaybeUninit;

use crate::{
    medium::StorageMedium,
    reader::BoundReader,
    storable::{LoadError, Storable},
    writer::BoundWriter,
    StorageError,
};

impl<T, const N: usize> Storable for [T; N]
where
    T: Storable,
{
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let mut array = MaybeUninit::uninit_array::<N>();

        for idx in 0..N {
            array[idx].write(T::load(reader).await?);
        }

        Ok(unsafe { MaybeUninit::array_assume_init(array) })
    }

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        for item in self.iter() {
            item.store(writer).await?;
        }

        Ok(())
    }
}

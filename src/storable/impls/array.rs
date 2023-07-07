use core::mem::MaybeUninit;

use embedded_io::asynch::{Read, Write};

use crate::storable::{LoadError, Loadable, Storable};

impl<T, const N: usize> Loadable for [T; N]
where
    T: Loadable,
{
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let mut array = MaybeUninit::uninit_array::<N>();

        for idx in 0..N {
            array[idx].write(T::load(reader).await?);
        }

        Ok(unsafe { MaybeUninit::array_assume_init(array) })
    }
}

impl<T, const N: usize> Storable for [T; N]
where
    T: Storable,
{
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        for item in self.iter() {
            item.store(writer).await?;
        }

        Ok(())
    }
}

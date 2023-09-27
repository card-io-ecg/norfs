use crate::storable::{LoadError, Loadable, Storable};
use embedded_io::asynch::{Read, Write};

impl<T, const N: usize> Loadable for heapless::Vec<T, N>
where
    T: Loadable,
{
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let count = usize::load(reader).await?;

        let mut vec = heapless::Vec::new();

        for _ in 0..count {
            vec.push(T::load(reader).await?)
                .map_err(|_| LoadError::InvalidValue)?;
        }

        Ok(vec)
    }
}

impl<T, const N: usize> Storable for heapless::Vec<T, N>
where
    T: Storable,
{
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.len().store(writer).await?;

        for item in self.iter() {
            item.store(writer).await?;
        }

        Ok(())
    }
}

impl<const N: usize> Loadable for heapless::String<N> {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
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
}

impl<const N: usize> Storable for heapless::String<N> {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        self.len().store(writer).await?;

        writer.write_all(self.as_bytes()).await
    }
}

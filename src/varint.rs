use core::convert::Infallible;

use crate::{
    medium::StorageMedium,
    reader::BoundReader,
    storable::{LoadError, Storable},
    writer::BoundWriter,
    StorageError,
};

pub struct Varint(u64);

impl From<u16> for Varint {
    fn from(value: u16) -> Self {
        Self(value as u64)
    }
}

impl TryFrom<Varint> for u16 {
    type Error = LoadError;

    fn try_from(value: Varint) -> Result<u16, Self::Error> {
        value.0.try_into().map_err(|_| LoadError::InvalidValue)
    }
}

impl From<u32> for Varint {
    fn from(value: u32) -> Self {
        Self(value as u64)
    }
}

impl TryFrom<Varint> for u32 {
    type Error = LoadError;

    fn try_from(value: Varint) -> Result<u32, Self::Error> {
        value.0.try_into().map_err(|_| LoadError::InvalidValue)
    }
}

impl From<u64> for Varint {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl TryFrom<Varint> for u64 {
    type Error = Infallible;

    fn try_from(value: Varint) -> Result<u64, Self::Error> {
        Ok(value.0)
    }
}

impl From<usize> for Varint {
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl TryFrom<Varint> for usize {
    type Error = LoadError;

    fn try_from(value: Varint) -> Result<usize, Self::Error> {
        value.0.try_into().map_err(|_| LoadError::InvalidValue)
    }
}

pub const fn varint_bytes<T: Sized>() -> usize {
    const BITS_PER_BYTE: usize = 8;
    const BITS_PER_VARINT_BYTE: usize = 7;

    // How many data bits do we need for this type?
    let bits = core::mem::size_of::<T>() * BITS_PER_BYTE;

    // We add (BITS_PER_BYTE - 1), to ensure any integer divisions
    // with a remainder will always add exactly one full byte, but
    // an evenly divided number of bits will be the same
    let roundup_bits = bits + (BITS_PER_BYTE - 1);

    // Apply division, using normal "round down" integer division
    roundup_bits / BITS_PER_VARINT_BYTE
}

/// Returns the maximum value stored in the last encoded byte.
pub const fn max_of_last_byte<T: Sized>() -> u8 {
    let max_bits = core::mem::size_of::<T>() * 8;
    let extra_bits = max_bits % 7;
    (1 << extra_bits) - 1
}

impl Storable for Varint {
    async fn load<M>(reader: &mut BoundReader<'_, M>) -> Result<Self, LoadError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let mut out = 0;
        for i in 0..varint_bytes::<u64>() {
            let val = u8::load(reader).await?;
            let carry = (val & 0x7F) as u64;
            out |= carry << (7 * i);

            if (val & 0x80) == 0 {
                return if i == varint_bytes::<u64>() - 1 && val > max_of_last_byte::<u64>() {
                    Err(LoadError::InvalidValue)
                } else {
                    Ok(Self(out))
                };
            }
        }
        Err(LoadError::InvalidValue)
    }

    async fn store<M>(&self, writer: &mut BoundWriter<'_, M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]: Sized,
    {
        let mut value = self.0;
        let mut encoded = [0; varint_bytes::<u64>()];
        for i in 0..varint_bytes::<u64>() {
            encoded[i] = value.to_le_bytes()[0];
            if value < 128 {
                return writer.write_all(&encoded[..=i]).await;
            }

            encoded[i] |= 0x80;
            value >>= 7;
        }

        writer.write_all(&encoded[..]).await
    }
}

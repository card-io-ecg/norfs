use core::convert::Infallible;

use embedded_io::asynch::{Read, Write};

use crate::storable::{ConversionError, LoadError, Loadable, Storable};

pub struct Varint(u64);

macro_rules! varint {
    ($ty:ty) => {
        impl From<$ty> for Varint {
            fn from(value: $ty) -> Self {
                Self(value as u64)
            }
        }

        impl TryFrom<Varint> for $ty {
            type Error = ConversionError;

            fn try_from(value: Varint) -> Result<$ty, Self::Error> {
                value
                    .0
                    .try_into()
                    .map_err(|_| ConversionError::InvalidValue)
            }
        }
    };
}

varint!(u16);
varint!(u32);
varint!(usize);

impl From<u64> for Varint {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl TryFrom<Varint> for u64 {
    type Error = ConversionError;

    fn try_from(value: Varint) -> Result<u64, Self::Error> {
        Ok(value.0)
    }
}

pub struct Svarint(i64);

macro_rules! svarint {
    ($ty:ty) => {
        impl From<$ty> for Svarint {
            fn from(value: $ty) -> Self {
                Self(value as i64)
            }
        }

        impl TryFrom<Svarint> for $ty {
            type Error = ConversionError;

            fn try_from(value: Svarint) -> Result<$ty, Self::Error> {
                value
                    .0
                    .try_into()
                    .map_err(|_| ConversionError::InvalidValue)
            }
        }
    };
}

svarint!(i16);
svarint!(i32);
svarint!(isize);

impl From<i64> for Svarint {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl TryFrom<Svarint> for i64 {
    type Error = Infallible;

    fn try_from(value: Svarint) -> Result<i64, Self::Error> {
        Ok(value.0)
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

impl Loadable for Varint {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        let mut out = 0;
        for i in 0..varint_bytes::<u64>() {
            let val = u8::load(reader).await?;
            let carry = (val & 0x7F) as u64;
            out |= carry << (7 * i);

            if (val & 0x80) == 0 {
                return if i == varint_bytes::<u64>() - 1 && val > max_of_last_byte::<u64>() {
                    Err(LoadError::from(ConversionError::InvalidValue))
                } else {
                    Ok(Self(out))
                };
            }
        }
        Err(LoadError::from(ConversionError::InvalidValue))
    }
}

impl Storable for Varint {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
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

impl Loadable for Svarint {
    async fn load<R: Read>(reader: &mut R) -> Result<Self, LoadError<R::Error>> {
        Varint::load(reader).await.map(|v| Self(zigzag_decode(v.0)))
    }
}

impl Storable for Svarint {
    async fn store<W: Write>(&self, writer: &mut W) -> Result<(), W::Error> {
        zigzag_encode(self.0).store(writer).await
    }
}

#[inline]
const fn zigzag_encode(val: i64) -> u64 {
    ((val << 1) ^ (val >> ((core::mem::size_of::<u64>() - 1) * 8))) as u64
}

#[inline]
const fn zigzag_decode(val: u64) -> i64 {
    (val >> 1) as i64 ^ -((val & 1) as i64)
}

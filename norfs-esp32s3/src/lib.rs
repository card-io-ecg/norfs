#![no_std]
#![feature(async_fn_in_trait)]
#![allow(stable_features, async_fn_in_trait, unknown_lints)]

use embassy_futures::yield_now;
use norfs_driver::{
    aligned::AlignedStorage,
    medium::{MediumError, WriteGranularity},
};

const WRITE_GRANULARITY: WriteGranularity = WriteGranularity::Bit;
const BLOCK_SIZE: usize = 65536;
const SMALL_BLOCK_SIZE: usize = 4096;
const PAGE_SIZE: usize = 256;

#[cfg(feature = "critical-section")]
#[inline(always)]
#[link_section = ".rwtext"]
fn maybe_with_critical_section<R>(f: impl FnOnce() -> R) -> R {
    critical_section::with(|_| f())
}

#[cfg(not(feature = "critical-section"))]
#[inline(always)]
#[allow(unused)]
fn maybe_with_critical_section<R>(f: impl FnOnce() -> R) -> R {
    f()
}

macro_rules! rom_fn {
    (fn $name:ident($($arg:tt: $ty:ty),*) -> $retval:ty = $addr:expr) => {
        #[inline(always)]
        #[allow(unused)]
        #[link_section = ".rwtext"]
        pub(crate) fn $name($($arg:$ty),*) -> i32 {
            maybe_with_critical_section(|| unsafe {
                let rom_fn: unsafe extern "C" fn($($arg: $ty),*) -> $retval =
                    core::mem::transmute($addr as usize);
                    rom_fn($($arg),*)
            })
        }
    };

    ($(fn $name:ident($($arg:tt: $ty:ty),*) -> $retval:ty = $addr:expr),+) => {
        $(
            rom_fn!(fn $name($($arg: $ty),*) -> $retval = $addr);
        )+
    };
}

rom_fn!(
    fn esp_rom_spiflash_read(src_addr: u32, data: *mut u32, len: u32) -> i32 = 0x40000a20,
    fn esp_rom_spiflash_unlock() -> i32 = 0x40000a2c,
    fn esp_rom_spiflash_erase_block(block_number: u32) -> i32 = 0x40000a08,
    fn esp_rom_spiflash_erase_sector(block_number: u32) -> i32 = 0x400009fc,
    fn esp_rom_spiflash_write(dest_addr: u32, data: *const u32, len: u32) -> i32 = 0x40000a14,
    fn esp_rom_spiflash_read_user_cmd(status: *mut u32, cmd: u8) -> i32 = 0x40000a5c
);

pub trait InternalPartition {
    const OFFSET: usize;
    const SIZE: usize;
}

struct UnlockToken<'a>(&'a mut bool);
impl Drop for UnlockToken<'_> {
    fn drop(&mut self) {
        *self.0 = false;
    }
}

#[link_section = ".rwtext"]
#[inline(never)]
fn wait_idle() -> Result<(), MediumError> {
    const SR_WIP: u32 = 1 << 0;

    let mut status = 0x01;
    while status & SR_WIP != 0 {
        if esp_rom_spiflash_read_user_cmd(&mut status, 0x05) != 0 {
            return Err(MediumError::Write);
        }
    }
    Ok(())
}

#[link_section = ".rwtext"]
#[inline(never)]
fn write_impl(offset: usize, data: &[u8]) -> Result<(), MediumError> {
    maybe_with_critical_section(|| {
        let len = data.len();
        let ptr = data.as_ptr().cast();
        if esp_rom_spiflash_write(offset as u32, ptr, len as u32) == 0 {
            wait_idle()
        } else {
            Err(MediumError::Write)
        }
    })
}

#[link_section = ".rwtext"]
#[inline(never)]
fn erase_sector_impl(sector: usize) -> Result<(), MediumError> {
    maybe_with_critical_section(|| {
        if esp_rom_spiflash_erase_sector(sector as u32) == 0 {
            wait_idle()
        } else {
            Err(MediumError::Erase)
        }
    })
}

#[link_section = ".rwtext"]
#[inline(never)]
fn erase_block_impl(block: usize) -> Result<(), MediumError> {
    maybe_with_critical_section(|| {
        if esp_rom_spiflash_erase_block(block as u32) == 0 {
            wait_idle()
        } else {
            Err(MediumError::Erase)
        }
    })
}

#[link_section = ".rwtext"]
#[inline(never)]
fn read_impl(offset: usize, data: &mut [u8]) -> Result<(), MediumError> {
    maybe_with_critical_section(|| {
        let len = data.len() as u32;
        let ptr = data.as_mut_ptr().cast();
        if esp_rom_spiflash_read(offset as u32, ptr, len) == 0 {
            Ok(())
        } else {
            Err(MediumError::Read)
        }
    })
}

pub struct InternalDriver<P: InternalPartition> {
    unlocked: bool,
    _partition: P,
}

impl<P: InternalPartition> InternalDriver<P> {
    pub const fn new(partition: P) -> Self {
        Self {
            unlocked: false,
            _partition: partition,
        }
    }

    fn unlock(&mut self) -> Result<UnlockToken<'_>, MediumError> {
        if !self.unlocked {
            if esp_rom_spiflash_unlock() != 0 {
                return Err(MediumError::Write);
            }
            self.unlocked = true;
        }

        Ok(UnlockToken(&mut self.unlocked))
    }

    fn offset(block: usize, offset: usize) -> usize {
        P::OFFSET + block * Self::BLOCK_SIZE + offset
    }
}

impl<P: InternalPartition> AlignedStorage for InternalDriver<P> {
    const BLOCK_COUNT: usize = P::SIZE / BLOCK_SIZE;
    const BLOCK_SIZE: usize = BLOCK_SIZE;
    const PAGE_SIZE: usize = PAGE_SIZE;
    const WRITE_GRANULARITY: WriteGranularity = WRITE_GRANULARITY;

    async fn erase(&mut self, block: usize) -> Result<(), MediumError> {
        let _token = self.unlock()?;

        let offset = P::OFFSET / Self::BLOCK_SIZE;
        let block = offset + block;

        let result = erase_block_impl(block);
        yield_now().await;

        result
    }

    async fn read_aligned(
        &mut self,
        block: usize,
        offset: usize,
        data: &mut [u8],
    ) -> Result<(), MediumError> {
        let offset = Self::offset(block, offset);

        let result = read_impl(offset, data);
        yield_now().await;

        result
    }

    async fn write_aligned(
        &mut self,
        block: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MediumError> {
        let _token = self.unlock()?;

        let offset = Self::offset(block, offset);
        let result = write_impl(offset, data);
        yield_now().await;

        result
    }
}

pub struct SmallInternalDriver<P: InternalPartition> {
    unlocked: bool,
    _partition: P,
}

impl<P: InternalPartition> SmallInternalDriver<P> {
    pub const fn new(partition: P) -> Self {
        Self {
            unlocked: false,
            _partition: partition,
        }
    }

    fn unlock(&mut self) -> Result<UnlockToken<'_>, MediumError> {
        if !self.unlocked {
            if esp_rom_spiflash_unlock() != 0 {
                return Err(MediumError::Write);
            }
            self.unlocked = true;
        }

        Ok(UnlockToken(&mut self.unlocked))
    }

    fn offset(block: usize, offset: usize) -> usize {
        P::OFFSET + block * Self::BLOCK_SIZE + offset
    }
}

impl<P: InternalPartition> AlignedStorage for SmallInternalDriver<P> {
    const BLOCK_COUNT: usize = P::SIZE / SMALL_BLOCK_SIZE;
    const BLOCK_SIZE: usize = SMALL_BLOCK_SIZE;
    const PAGE_SIZE: usize = PAGE_SIZE;
    const WRITE_GRANULARITY: WriteGranularity = WRITE_GRANULARITY;

    async fn erase(&mut self, block: usize) -> Result<(), MediumError> {
        let _token = self.unlock()?;

        let offset = P::OFFSET / Self::BLOCK_SIZE;
        let block = offset + block;

        let result = erase_sector_impl(block);
        yield_now().await;

        result
    }

    async fn read_aligned(
        &mut self,
        block: usize,
        offset: usize,
        data: &mut [u8],
    ) -> Result<(), MediumError> {
        let offset = Self::offset(block, offset);
        let result = read_impl(offset, data);
        yield_now().await;

        result
    }

    async fn write_aligned(
        &mut self,
        block: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), MediumError> {
        let _token = self.unlock()?;

        let offset = Self::offset(block, offset);
        let result = write_impl(offset, data);
        yield_now().await;

        result
    }
}

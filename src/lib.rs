#![cfg_attr(not(test), no_std)]
#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]
#![feature(generic_const_exprs)] // Eww
#![feature(maybe_uninit_uninit_array, maybe_uninit_array_assume_init)]
#![allow(incomplete_features)]

use core::fmt::Debug;

use crate::{
    diag::Counters,
    ll::{
        blocks::{BlockHeaderKind, BlockInfo, BlockOps, BlockType, IndexedBlockInfo},
        objects::{
            ObjectHeader, ObjectInfo, ObjectIterator, ObjectLocation, ObjectReader, ObjectState,
            ObjectType, ObjectWriter,
        },
    },
    medium::{StorageMedium, StoragePrivate},
    read_dir::ReadDir,
    reader::Reader,
    writer::{FileDataWriter, Writer},
};

pub mod diag;
pub mod drivers;
pub mod fxhash;
pub mod gc;
pub mod ll;
pub mod medium;
pub mod read_dir;
pub mod reader;
pub mod storable;
pub mod varint;
pub mod writer;

/// Error values returned by storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    /// The file does not exist.
    NotFound,

    /// The filesystem is not formatted.
    NotFormatted,

    /// The filesystem state is inconsistent.
    FsCorrupted,

    /// The underlying driver returned a driver-specific error.
    Io,

    /// The operation is not permitted.
    InvalidOperation,

    /// The storage medium is full.
    InsufficientSpace,

    /// The end of file was reached.
    EndOfFile,

    /// The input buffer is not large enough to hold the output.
    InsufficientBuffer,
}

impl embedded_io::Error for StorageError {
    fn kind(&self) -> embedded_io::ErrorKind {
        embedded_io::ErrorKind::Other
    }
}

struct BlockInfoCollection<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    blocks: [BlockInfo<M>; M::BLOCK_COUNT],
}

impl<M> BlockInfoCollection<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    async fn allocate_new_object(
        &mut self,
        ty: BlockType,
        min_free: usize,
        medium: &mut M,
    ) -> Result<ObjectLocation, StorageError> {
        self.allocate_object(ty, min_free, false, medium).await
    }

    fn all_blocks(&self) -> impl Iterator<Item = IndexedBlockInfo<M>> + '_ {
        self.blocks
            .iter()
            .copied()
            .enumerate()
            .map(|(idx, info)| IndexedBlockInfo(idx, info))
    }

    fn blocks(&self, ty: BlockType) -> impl Iterator<Item = IndexedBlockInfo<M>> + '_ {
        self.all_blocks().filter(move |info| info.is_type(ty))
    }

    fn allocate_object_impl(
        &self,
        ty: BlockType,
        min_free: usize,
        allow_gc_block: bool,
    ) -> Result<usize, StorageError> {
        log::trace!("Storage::allocate_object({ty:?}, {min_free}, {allow_gc_block:?})");

        // Predicates
        fn and<M: StorageMedium>(
            a: impl Fn(&IndexedBlockInfo<M>) -> bool,
            b: impl Fn(&IndexedBlockInfo<M>) -> bool,
        ) -> impl Fn(&IndexedBlockInfo<M>) -> bool {
            move |info: &IndexedBlockInfo<M>| a(info) && b(info)
        }
        let has_enough_free_space = move |info: &IndexedBlockInfo<M>| info.free_space() >= min_free;
        let not_empty = |info: &IndexedBlockInfo<M>| !info.is_empty();

        // Try to find a used block with enough free space
        if let Some(block) = self.blocks(ty).find(and(not_empty, has_enough_free_space)) {
            return Ok(block.0);
        }
        if let Some(block) = self.blocks(ty).find(has_enough_free_space) {
            return Ok(block.0);
        }

        // We reserve 2 blocks for GC.
        if allow_gc_block || self.blocks(BlockType::Undefined).count() > 2 {
            // Pick a free block. Prioritize lesser used blocks.
            if let Some(block) = self
                .blocks(BlockType::Undefined)
                .filter(has_enough_free_space)
                .min_by_key(|info| info.erase_count())
            {
                return Ok(block.0);
            }
        }

        // No block found
        Err(StorageError::InsufficientSpace)
    }

    async fn allocate_object(
        &mut self,
        ty: BlockType,
        min_free: usize,
        allow_gc_block: bool,
        medium: &mut M,
    ) -> Result<ObjectLocation, StorageError> {
        let location = self
            .allocate_object_impl(ty, min_free, allow_gc_block)
            .map(|block| ObjectLocation {
                block,
                offset: self.blocks[block].used_bytes(),
            })?;

        if self.blocks[location.block].is_unassigned() {
            log::debug!("Setting block {} to {ty:?}", location.block);
            BlockOps::new(medium)
                .set_block_type(location.block, ty)
                .await?;
            self.blocks[location.block].header.set_block_type(ty);
        }

        Ok(location)
    }

    pub(crate) async fn find_block_to_free(
        &mut self,
        medium: &mut M,
    ) -> Result<Option<(IndexedBlockInfo<M>, usize)>, StorageError> {
        let mut target_block = None::<(IndexedBlockInfo<M>, usize)>;

        // Select block with enough freeable space and minimum erase counter
        for info in self.all_blocks().filter(|block| !block.is_empty()) {
            let freeable = info.calculate_freeable_space(medium).await?;

            match target_block {
                Some((idx, potential)) => {
                    if freeable > potential
                        || (potential == freeable && info.erase_count() < idx.erase_count())
                    {
                        target_block = Some((info, freeable));
                    }
                }

                None => target_block = Some((info, freeable)),
            }
        }

        Ok(target_block)
    }

    async fn format(&mut self, block_to_free: usize, medium: &mut M) -> Result<(), StorageError> {
        BlockOps::new(medium).format_block(block_to_free).await?;
        self.blocks[block_to_free].update_stats_after_erase();

        Ok(())
    }

    async fn format_indexed(
        &mut self,
        block_to_free: IndexedBlockInfo<M>,
        medium: &mut M,
    ) -> Result<(), StorageError> {
        self.format(block_to_free.0, medium).await
    }
}

/// A mounted storage partition.
pub struct Storage<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    medium: M,
    blocks: BlockInfoCollection<M>,
}

/// Controls what happens when storing data to a file that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCollision {
    /// Overwrite the existing file.
    Overwrite,

    /// The operation returns an error.
    Fail,
}

impl<M> Storage<M>
where
    M: StorageMedium,
    [(); M::BLOCK_COUNT]:,
{
    /// Mounts the filesystem.
    ///
    /// Returns an error if the filesystem is not formatted.
    pub async fn mount(mut partition: M) -> Result<Self, StorageError> {
        let mut blocks = [BlockInfo::new_unknown(); M::BLOCK_COUNT];

        let mut ops = BlockOps::new(&mut partition);
        for (idx, block) in blocks.iter_mut().enumerate() {
            *block = ops.scan_block(idx).await?;
        }

        Ok(Self {
            medium: partition,
            blocks: BlockInfoCollection { blocks },
        })
    }

    /// Unconditionally formats the filesystem.
    pub async fn format(partition: &mut M) -> Result<(), StorageError> {
        BlockOps::new(partition).format_storage().await
    }

    /// Unconditionally formats then mounts the filesystem.
    pub async fn format_and_mount(mut partition: M) -> Result<Self, StorageError> {
        Self::format(&mut partition).await?;

        Self::mount(partition).await
    }

    /// Returns the total capacity of the filesystem in bytes.
    pub fn capacity(&self) -> usize {
        M::BLOCK_COUNT * M::BLOCK_SIZE
    }

    /// Returns the number of free bytes in the filesystem.
    ///
    /// Note: this function does not count deleted files as free space, so the result will
    /// not match the value `capacity() - used_bytes()`.
    pub fn free_bytes(&self) -> usize {
        self.blocks
            .blocks
            .iter()
            .map(|blk| M::BLOCK_SIZE - blk.used_bytes())
            .sum()
    }

    /// Returns the number of bytes used in the filesystem.
    ///
    /// This function takes filesystem overhead into account, but does not count deleted files.
    pub async fn used_bytes(&mut self) -> Result<usize, StorageError> {
        let mut used_bytes = 0;

        for (block_idx, info) in self.blocks.blocks.iter().enumerate() {
            match info.kind() {
                BlockHeaderKind::Empty => {}
                BlockHeaderKind::Known(BlockType::Undefined) | BlockHeaderKind::Unknown => {
                    used_bytes += info.used_bytes();
                }
                BlockHeaderKind::Known(_) => {
                    let mut iter = ObjectIterator::new::<M>(block_idx);

                    while let Some(object) = iter.next(&mut self.medium).await? {
                        if let ObjectState::Finalized = object.header.state() {
                            used_bytes += object.total_size();
                        }
                    }
                }
            }
        }

        Ok(used_bytes)
    }

    /// Deletes the file at `path`.
    pub async fn delete(&mut self, path: &str) -> Result<(), StorageError> {
        log::debug!("Storage::delete({path})");
        let location = self.lookup(path).await?;
        self.delete_file_at(location).await
    }

    /// Creates a new file at `path` with the given `data`.
    ///
    /// If a file already exists at `path`, the behaviour is determined by `if_exists`.
    ///  - `OnCollision::Overwrite` will overwrite the existing file.
    ///  - `OnCollision::Fail` will return an error.
    pub async fn store(
        &mut self,
        path: &str,
        data: &[u8],
        if_exists: OnCollision,
    ) -> Result<(), StorageError> {
        struct DataWriter<'a>(&'a [u8]);
        impl FileDataWriter for DataWriter<'_> {
            async fn write<M>(
                &self,
                writer: &mut Writer<M>,
                storage: &mut Storage<M>,
            ) -> Result<(), StorageError>
            where
                M: StorageMedium,
                [(); M::BLOCK_COUNT]:,
            {
                writer.write_all(self.0, storage).await
            }

            fn estimate_length(&self) -> usize {
                self.0.len()
            }
        }

        self.store_writer(path, &DataWriter(data), if_exists).await
    }

    /// Creates a new file at `path` with the given `data`.
    ///
    /// If a file already exists at `path`, the behaviour is determined by `if_exists`.
    ///  - `OnCollision::Overwrite` will overwrite the existing file.
    ///  - `OnCollision::Fail` will return an error.
    pub async fn store_writer(
        &mut self,
        path: &str,
        data: &impl FileDataWriter,
        if_exists: OnCollision,
    ) -> Result<(), StorageError> {
        log::debug!(
            "Storage::store_writer({path}, estimated = {})",
            data.estimate_length()
        );

        self.make_space_for(path.len(), data.estimate_length())
            .await?;

        let overwritten_location = self.lookup(path).await;

        let overwritten = match overwritten_location {
            Ok(overwritten) => Some(overwritten),
            Err(StorageError::NotFound) => None,
            Err(e) => return Err(e),
        };

        if overwritten.is_some() && if_exists == OnCollision::Fail {
            log::debug!("File already exists at path: {path}");
            return Err(StorageError::InvalidOperation);
        }

        if let Some(overwritten) = overwritten.as_ref() {
            log::debug!("Overwriting location: {overwritten:?}");
        }

        // TODO: reuse filename object
        Writer::create(path, self, data).await?;

        if let Some(location) = overwritten {
            self.delete_file_at(location).await?;
        }

        Ok(())
    }

    /// Convenience method for checking if a file exists. Ignores all errors.
    pub async fn exists(&mut self, path: &str) -> bool {
        log::debug!("Storage::exists({path})");
        self.lookup(path).await.is_ok()
    }

    /// Opens the file at `path` for reading.
    ///
    /// Returns a reader object on success and an error on failure.
    ///
    /// Modifying the filesystem while a reader is open results in undefined behaviour.
    pub async fn read(&mut self, path: &str) -> Result<Reader<M>, StorageError> {
        log::debug!("Storage::read({path})");
        let object = self.lookup(path).await?;
        let metadata = object.read_metadata(&mut self.medium).await?;
        Ok(Reader::new(metadata))
    }

    /// Returns the content size of the file at `path`.
    pub async fn file_size(&mut self, path: &str) -> Result<usize, StorageError> {
        log::debug!("Storage::file_size({path})");
        let object = self.lookup(path).await?;

        let mut meta = object.read_metadata(&mut self.medium).await?;

        let mut size = 0;
        while let Some(chunk) = meta.next_object_location(&mut self.medium).await? {
            let data_object = ObjectReader::new(chunk, &mut self.medium, false).await?;
            size += data_object.len();
        }

        Ok(size)
    }

    pub async fn read_dir(&mut self) -> Result<ReadDir<M>, StorageError> {
        log::debug!("Storage::read_dir");
        Ok(ReadDir::new(self))
    }

    fn estimate_data_chunks(&self, mut len: usize) -> Result<usize, StorageError> {
        let mut block_count = 0;

        for (ty, skip) in [(BlockType::Data, 0), (BlockType::Undefined, 2)] {
            for block in self.blocks.blocks(ty).skip(skip) {
                let space = block.free_space();
                if space > ObjectHeader::byte_count::<M>() {
                    len = len.saturating_sub(space - ObjectHeader::byte_count::<M>());
                    block_count += 1;

                    if len == 0 {
                        return Ok(block_count);
                    }
                }
            }
        }

        Err(StorageError::InsufficientSpace)
    }

    async fn make_space_for(&mut self, path_len: usize, len: usize) -> Result<(), StorageError> {
        let mut meta_allocated = false;
        loop {
            let blocks = match self.estimate_data_chunks(
                M::align(len) + M::align(path_len) + M::align(ObjectHeader::byte_count::<M>()),
            ) {
                Ok(blocks) => blocks,
                Err(StorageError::InsufficientSpace) => {
                    DataObject.try_to_make_space(self).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };

            if meta_allocated {
                // Hopefully, freeing space didn't free the metadata block. If it did, we'll
                // exit with insufficient space error later.
                break;
            }

            let meta_size =
                ObjectHeader::byte_count::<M>() + 4 + (blocks + 1) * M::object_location_bytes();

            match self
                .blocks
                .allocate_new_object(BlockType::Metadata, meta_size, &mut self.medium)
                .await
            {
                Ok(_) => meta_allocated = true,
                Err(StorageError::InsufficientSpace) => MetaObject.try_to_make_space(self).await?,
                Err(e) => return Err(e),
            }
        }

        log::trace!("Storage::make_space_for({len}) done");
        Ok(())
    }

    async fn lookup(&mut self, path: &str) -> Result<ObjectLocation, StorageError> {
        let path_hash = hash_path(path);

        for block in self.blocks.blocks(BlockType::Metadata) {
            let mut iter = block.objects();

            'objs: while let Some(object) = iter.next(&mut self.medium).await? {
                if object.state() != ObjectState::Finalized {
                    continue 'objs;
                }

                let metadata = object.read_metadata(&mut self.medium).await?;

                if metadata.path_hash == path_hash {
                    let mut reader =
                        ObjectReader::new(metadata.filename_location, &mut self.medium, false)
                            .await?;

                    if reader.len() != path.len() {
                        continue 'objs;
                    }

                    let mut path_buf = [0u8; 16];

                    let mut read = 0;
                    while read < path.len() {
                        let len = path_buf.len().min(path.len() - read);
                        let buf = &mut path_buf[..len];

                        let bytes_read = reader.read(&mut self.medium, buf).await?;
                        let path_bytes = &path.as_bytes()[read..read + bytes_read];

                        if path_bytes != buf {
                            continue 'objs;
                        }

                        read += bytes_read;
                    }

                    return Ok(metadata.location());
                }
            }
        }

        // not found
        Err(StorageError::NotFound)
    }

    async fn delete_file_at(&mut self, meta_location: ObjectLocation) -> Result<(), StorageError> {
        let mut metadata = meta_location.read_metadata(&mut self.medium).await?;

        debug_assert_ne!(metadata.object.state(), ObjectState::Free);

        if let Some(filename_object) =
            ObjectInfo::read(metadata.filename_location, &mut self.medium).await?
        {
            filename_object.delete(&mut self.medium).await?;
        }

        while let Some(location) = metadata.next_object_location(&mut self.medium).await? {
            let mut header = ObjectHeader::read(location, &mut self.medium).await?;
            if header.state() != ObjectState::Free {
                header
                    .update_state(&mut self.medium, ObjectState::Deleted)
                    .await?;
            }
        }

        metadata
            .object
            .update_state(&mut self.medium, ObjectState::Deleted)
            .await?;

        Ok(())
    }

    async fn write_file_data(
        &mut self,
        location: ObjectLocation,
        data: &[u8],
    ) -> Result<(), StorageError> {
        self.write_object(location, ObjectType::FileData, data)
            .await
    }

    async fn write_object(
        &mut self,
        location: ObjectLocation,
        obj_ty: ObjectType,
        data: &[u8],
    ) -> Result<(), StorageError> {
        self.blocks.blocks[location.block].add_used_bytes(
            ObjectWriter::write_to(location, obj_ty, &mut self.medium, data).await?,
        );
        Ok(())
    }

    async fn write_location(
        &mut self,
        meta_writer: &mut ObjectWriter<M>,
        location: ObjectLocation,
    ) -> Result<(), StorageError> {
        let bytes = location.into_bytes::<M>();
        meta_writer.write(&mut self.medium, &bytes).await
    }

    async fn find_metadata_of_object(
        &mut self,
        object: &ObjectInfo<M>,
    ) -> Result<ObjectInfo<M>, StorageError> {
        log::trace!("Storage::find_metadata_of_object({:?})", object.location());
        for block in self.blocks.blocks(BlockType::Metadata) {
            let mut objects = block.objects();
            while let Some(meta_object) = objects.next(&mut self.medium).await? {
                match meta_object.state() {
                    ObjectState::Free => break,
                    ObjectState::Allocated => break,
                    ObjectState::Finalized => {}
                    ObjectState::Deleted => continue,
                }
                let mut meta = meta_object.read_metadata(&mut self.medium).await?;
                while let Some(loc) = meta.next_object_location(&mut self.medium).await? {
                    if loc == object.location() {
                        log::trace!(
                            "Storage::find_metadata_of_object({:?}) -> {:?}",
                            object.location(),
                            meta_object.location()
                        );
                        return Ok(meta_object);
                    }
                }
            }
        }

        Err(StorageError::NotFound)
    }

    async fn find_new_object_location(
        &mut self,
        ty: BlockType,
        len: usize,
    ) -> Result<ObjectLocation, StorageError> {
        log::trace!("Storage::find_new_object_location({ty:?}, {len})");

        // find block with most free space
        let object_size = M::align(ObjectHeader::byte_count::<M>()) + len;
        let location = self
            .blocks
            .allocate_new_object(ty, object_size, &mut self.medium)
            .await?;

        log::trace!("Storage::find_new_object_location({ty:?}, {len}) -> {location:?}");

        Ok(location)
    }
}

// Async functions can't be recursive. Splitting out implementation for each block type means
// we can reuse code without recursion.
trait ObjectMover: Debug {
    const BLOCK_TYPE: BlockType;

    async fn move_object<M>(
        &mut self,
        storage: &mut Storage<M>,
        object: ObjectInfo<M>,
        destination: ObjectLocation,
    ) -> Result<ObjectInfo<M>, StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:;

    async fn try_to_make_space<M>(&mut self, storage: &mut Storage<M>) -> Result<(), StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:,
    {
        log::debug!("{self:?}::try_to_make_space()");
        let Some((block_to_free, freeable)) = storage
            .blocks
            .find_block_to_free(&mut storage.medium)
            .await?
        else {
            log::debug!("Could not find a block to free");
            return Err(StorageError::InsufficientSpace);
        };

        if freeable != block_to_free.used_bytes() {
            log::debug!("{self:?}::try_to_make_space(): Moving objects out of block to free");
            // We need to move objects out of this block
            let mut iter = block_to_free.objects();

            while let Some(object) = iter.next(&mut storage.medium).await? {
                match object.state() {
                    ObjectState::Free | ObjectState::Deleted => continue,
                    ObjectState::Allocated => {
                        log::warn!("Encountered an allocated object");
                        // TODO: retry in a different object
                        return Err(StorageError::InsufficientSpace);
                    }
                    ObjectState::Finalized => {}
                }

                let copy_location = storage
                    .blocks
                    .allocate_object(
                        Self::BLOCK_TYPE,
                        object.total_size(),
                        true,
                        &mut storage.medium,
                    )
                    .await
                    .map_err(|_| StorageError::InsufficientSpace)?;

                self.move_object(storage, object, copy_location).await?;
            }
        }

        storage
            .blocks
            .format_indexed(block_to_free, &mut storage.medium)
            .await
    }
}

#[derive(Debug)]
struct DataObject;

impl ObjectMover for DataObject {
    const BLOCK_TYPE: BlockType = BlockType::Data;

    async fn move_object<M>(
        &mut self,
        storage: &mut Storage<M>,
        object: ObjectInfo<M>,
        destination: ObjectLocation,
    ) -> Result<ObjectInfo<M>, StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:,
    {
        log::trace!("{self:?}::move_object");

        let mut meta = storage.find_metadata_of_object(&object).await?;
        let new_meta_location = match storage
            .find_new_object_location(BlockType::Metadata, meta.total_size())
            .await
        {
            Ok(loc) => loc,
            Err(StorageError::InsufficientSpace) => {
                MetaObject.try_to_make_space(storage).await?;
                let new = storage
                    .find_new_object_location(BlockType::Metadata, meta.total_size())
                    .await?;
                // Look up again in case it was moved
                meta = storage.find_metadata_of_object(&object).await?;
                new
            }
            Err(e) => return Err(e),
        };

        log::debug!(
            "Moving data object {:?} to {destination:?}",
            object.location()
        );
        log::debug!(
            "Moving meta object {:?} to {new_meta_location:?}",
            meta.location()
        );

        // copy metadata object while replacing current object location to new location
        let mut meta_writer = ObjectWriter::allocate(
            new_meta_location,
            ObjectType::FileMetadata,
            &mut storage.medium,
        )
        .await?;
        let mut old_object_reader = meta.read_metadata(&mut storage.medium).await?;

        // copy header
        meta_writer
            .write(
                &mut storage.medium,
                &old_object_reader.path_hash.to_le_bytes(),
            )
            .await?;

        let filename_location = if old_object_reader.filename_location == object.location() {
            destination
        } else {
            old_object_reader.filename_location
        };
        let bytes = filename_location.into_bytes::<M>();
        meta_writer.write(&mut storage.medium, &bytes).await?;

        // copy object locations
        while let Some(loc) = old_object_reader
            .next_object_location(&mut storage.medium)
            .await?
        {
            let location = if loc == object.location() {
                destination
            } else {
                loc
            };
            let bytes = location.into_bytes::<M>();
            meta_writer.write(&mut storage.medium, &bytes).await?;
        }

        // copy data object
        let copied = object.copy_object(&mut storage.medium, destination).await?;
        storage.blocks.blocks[copied.location().block].add_used_bytes(copied.total_size());

        // finalize metadata object
        let meta_info = meta_writer.finalize(&mut storage.medium).await?;
        storage.blocks.blocks[meta_info.location().block].add_used_bytes(meta_info.total_size());

        // delete old metadata object
        meta.delete(&mut storage.medium).await?;
        // delete old object
        object.delete(&mut storage.medium).await?;

        Ok(copied)
    }
}

#[derive(Debug)]
struct MetaObject;

impl ObjectMover for MetaObject {
    const BLOCK_TYPE: BlockType = BlockType::Metadata;

    async fn move_object<M>(
        &mut self,
        storage: &mut Storage<M>,
        object: ObjectInfo<M>,
        destination: ObjectLocation,
    ) -> Result<ObjectInfo<M>, StorageError>
    where
        M: StorageMedium,
        [(); M::BLOCK_COUNT]:,
    {
        log::trace!("{self:?}::move_object");
        let info = object.move_object(&mut storage.medium, destination).await?;
        storage.blocks.blocks[destination.block].add_used_bytes(info.total_size());

        Ok(info)
    }
}

fn hash_path(path: &str) -> u32 {
    fxhash::hash32(path.as_bytes())
}

impl<P> Storage<Counters<P>>
where
    P: StorageMedium,
    [(); P::BLOCK_COUNT]:,
    [(); Counters::<P>::BLOCK_COUNT]:,
{
    pub fn erase_count(&self) -> usize {
        self.medium.erase_count
    }

    pub fn read_count(&self) -> usize {
        self.medium.read_count
    }

    pub fn write_count(&self) -> usize {
        self.medium.write_count
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use medium::{
        cache::ReadCache, ram::RamStorage, ram_aligned::AlignedNorRamStorage,
        ram_nor_emulating::NorRamStorage,
    };

    const LIPSUM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce i";

    pub fn init_test() {
        _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Trace)
            .env()
            .init();
        println!();
    }

    pub(crate) async fn create_default_fs() -> Storage<NorRamStorage<256, 32>> {
        let medium = NorRamStorage::<256, 32>::new();
        Storage::format_and_mount(medium)
            .await
            .expect("Failed to mount storage")
    }

    pub(crate) async fn create_larger_fs() -> Storage<NorRamStorage<1024, 256>> {
        let medium = NorRamStorage::<1024, 256>::new();
        Storage::format_and_mount(medium)
            .await
            .expect("Failed to mount storage")
    }

    pub(crate) async fn create_aligned_fs() -> Storage<AlignedNorRamStorage<1024, 256>> {
        let medium = AlignedNorRamStorage::<1024, 256>::new();
        Storage::format_and_mount(medium)
            .await
            .expect("Failed to mount storage")
    }

    pub(crate) async fn create_aligned_fs_with_read_cache(
    ) -> Storage<ReadCache<AlignedNorRamStorage<1024, 256>, 256, 2>> {
        let medium = ReadCache::new(AlignedNorRamStorage::<1024, 256>::new());
        Storage::format_and_mount(medium)
            .await
            .expect("Failed to mount storage")
    }

    pub(crate) async fn create_word_granularity_fs<const GRANULARITY: usize>(
    ) -> Storage<RamStorage<512, 64, GRANULARITY>>
    where
        [(); RamStorage::<512, 64, GRANULARITY>::BLOCK_COUNT]:,
    {
        let medium = RamStorage::<512, 64, GRANULARITY>::new();
        Storage::format_and_mount(medium)
            .await
            .expect("Failed to mount storage")
    }

    pub(crate) async fn assert_file_contents<M: StorageMedium>(
        storage: &mut Storage<M>,
        path: &str,
        expected: &[u8],
    ) where
        [(); M::BLOCK_COUNT]:,
    {
        let mut reader = storage.read(path).await.expect("Failed to open file");

        let mut contents = vec![0; expected.len()];
        let read = reader
            .read(storage, &mut contents)
            .await
            .expect("Failed to read file");

        let mut should_remain_empty = [0; 1];
        assert_eq!(
            0,
            reader
                .read(storage, &mut should_remain_empty)
                .await
                .unwrap()
        );
        assert_eq!(read, expected.len());
        assert_eq!(contents, expected);
    }

    #[macro_export]
    macro_rules! test_cases {
        (
            $(async fn $test_name:ident<M: StorageMedium>(mut $storage:ident: Storage<M> $(,)?) $code:tt)+
        ) => {
            $(
                #[async_std::test]
                async fn $test_name() {
                    async fn test_case_impl<M: StorageMedium>(mut $storage: Storage<M>)
                    where
                        [(); M::BLOCK_COUNT]:,
                    {
                        $code
                    }

                    crate::test::init_test();

                    log::info!("Running test case with create_default_fs");
                    test_case_impl(crate::test::create_default_fs().await).await;
                    log::info!("Running test case with create_larger_fs");
                    test_case_impl(crate::test::create_larger_fs().await).await;
                    log::info!("Running test case with create_word_granularity_fs::<1>");
                    test_case_impl(crate::test::create_word_granularity_fs::<1>().await).await;
                    log::info!("Running test case with create_word_granularity_fs::<4>");
                    test_case_impl(crate::test::create_word_granularity_fs::<4>().await).await;
                    log::info!("Running test case with create_aligned_fs");
                    test_case_impl(crate::test::create_aligned_fs().await).await;
                    log::info!("Running test case with create_aligned_fs_with_read_cache");
                    test_case_impl(crate::test::create_aligned_fs_with_read_cache().await).await;
                }
            )+
        };
    }

    test_cases! {
        async fn lookup_returns_error_if_file_does_not_exist<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            assert!(!storage.exists("foo").await);

            assert!(
                storage.read("foo").await.is_err(),
                "Lookup returned Ok unexpectedly"
            );
        }

        async fn delete_returns_error_if_file_does_not_exist<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            assert!(!storage.exists("foo").await);
            storage
                .delete("foo")
                .await
                .expect_err("Delete returned Ok unexpectedly");
        }

        async fn written_file_can_be_read<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage
                .store("foo", b"barbaz", OnCollision::Overwrite)
                .await
                .expect("Create failed");

            let mut reader = storage.read("foo").await.expect("Failed to open file");

            let mut buf = [0u8; 6];

            reader
                .read(&mut storage, &mut buf)
                .await
                .expect("Failed to read file");

            assert_eq!(buf, *b"barbaz");
        }

        async fn reading_overwritten_file_reads_newer_data<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage
                .store("foo", b"barbaz", OnCollision::Overwrite)
                .await
                .expect("Create failed");

            assert!(storage.exists("foo").await);

            storage
                .store("foo", b"foofoobar", OnCollision::Overwrite)
                .await
                .expect("Create failed");

            assert!(storage.exists("foo").await);

            assert_file_contents(&mut storage, "foo", b"foofoobar").await;
        }

        async fn failure_to_overwrite_preserves_original_file<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage
                .store("foo", b"barbaz", OnCollision::Fail)
                .await
                .expect("Create failed");

            assert!(storage.exists("foo").await);

            assert!(
                storage.store("foo", b"foofoobar", OnCollision::Fail).await.is_err(),
                "Store succeeded unexpectedly"
            );

            assert!(storage.exists("foo").await);

            assert_file_contents(&mut storage, "foo", b"barbaz").await;
        }

        async fn content_can_be_longer_than_block_size<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage.store("foo", LIPSUM, OnCollision::Overwrite).await.expect("Create failed");

            let mut reader = storage.read("foo").await.expect("Failed to open file");

            let mut buf = [0u8; 64];

            // Read in two chunks to test that the reader resumes with the current byte
            reader
                .read(&mut storage, &mut buf[0..32])
                .await
                .expect("Failed to read file");
            reader
                .read(&mut storage, &mut buf[32..])
                .await
                .expect("Failed to read file");

            assert_eq!(buf, *LIPSUM);
        }

        async fn file_size_reports_content_size<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage.store("foo", LIPSUM, OnCollision::Overwrite).await.expect("Create failed");

            let file_size = storage.file_size("foo").await.expect("Failed to read file size");

            assert_eq!(file_size, LIPSUM.len());
        }

        async fn deleted_file_can_no_longer_be_read<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage
                .store("foo", b"barbaz", OnCollision::Overwrite)
                .await
                .expect("Create failed");

            storage.delete("foo").await.expect("Failed to delete");

            assert!(!storage.exists("foo").await);
            assert!(
                storage.read("foo").await.is_err(),
                "Lookup returned Ok unexpectedly"
            );
        }

        async fn reading_reads_from_the_correct_file<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage.store("foo", b"bar", OnCollision::Overwrite).await.expect("Create failed");
            storage.store("baz", b"asdf", OnCollision::Overwrite).await.expect("Create failed");

            assert_file_contents(&mut storage, "foo", b"bar").await;
            assert_file_contents(&mut storage, "baz", b"asdf").await;
        }

        async fn read_dir_returns_all_files_once<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage.store("foo", b"bar", OnCollision::Overwrite).await.expect("Create failed");
            storage.store("baz", b"asdf", OnCollision::Overwrite).await.expect("Create failed");

            let mut files = storage.read_dir().await.expect("Failed to list files");

            let mut fn_buffer = [0u8; 32];
            while let Some(file) = files.next(&mut storage).await.unwrap() {
                match file.name(&mut storage, &mut fn_buffer).await.unwrap() {
                    "foo" => assert_file_contents(&mut storage, "foo", b"bar").await,
                    "baz" => assert_file_contents(&mut storage, "baz", b"asdf").await,
                    _ => panic!("Unexpected file"),
                }
            }
        }

        async fn read_dir_can_delete_files<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            storage.store("foo", b"bar", OnCollision::Overwrite).await.expect("Create failed");
            storage.store("baz", b"asdf", OnCollision::Overwrite).await.expect("Create failed");

            let mut files = storage.read_dir().await.expect("Failed to list files");

            while let Some(file) = files.next(&mut storage).await.unwrap() {
                file.delete(&mut storage).await.unwrap();
            }

            assert!(!storage.exists("foo").await);
            assert!(!storage.exists("bar").await);
        }

        async fn can_reuse_space_of_deleted_files<M: StorageMedium>(
            mut storage: Storage<M>,
        ) {
            for _ in 0..50 {
                storage
                    .store("foo", LIPSUM, OnCollision::Overwrite)
                    .await
                    .expect("Failed to create");

                storage.delete("foo").await.expect("Failed to delete");
            }
        }
    }

    #[async_std::test]
    async fn fails_to_write_file_if_not_enough_space() {
        init_test();

        let mut storage = create_default_fs().await;

        storage
            .store("foo", LIPSUM, OnCollision::Overwrite)
            .await
            .expect("Create failed");

        assert!(storage.exists("foo").await);

        assert!(
            storage
                .store("bar", LIPSUM, OnCollision::Overwrite)
                .await
                .is_err(),
            "Store returned Ok unexpectedly"
        );
    }
}

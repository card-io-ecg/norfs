//! File system object management.
//!
//! Objects are the basic blocks of NorFS. Objects have a type, a state, and a payload.
//! The object state is designed to be able to be updated atomically, so that a power loss
//! during an update will not result in an irrecoverably corrupted object.
//!
//! Each object can be in one of the following states:
//!
//!  - Allocated: the object may have been partially written, but is not yet finalized.
//!  - Finalized: the object's payload is complete and the header contains a valid payload size.
//!  - Deleted: the object's header contains a valid payload size but it's contents should no longer
//!    be read.
//!
//! The file system partitions physical blocks into multiple types:
//!
//!  - Metadata blocks contain metadata objects (currently, file metadata).
//!  - Data blocks contain data objects and file name objects.
//!
//! This partitioning helps with file lookup, as the file system can skip over data blocks when
//! searching for a file. The file system allocates objects linearly in their appropriate blocks.
//! The file system does not reuse deleted objects, but instead allocates new objects in the next
//! free location.
//!
//! A consistent file system should not contain allocated, but not finalized objects. This case is
//! enforced by the garbage collector, but an unexpected power loss may still result a partially
//! written object. These objects are always at the end of a physical block though, which means
//! that they can be detected and ignored.
//!
//! Objects do not have a fixed location. The garbage collector is allowed to move any object
//! if necessary.

use core::marker::PhantomData;

use crate::{
    debug, error,
    ll::blocks::BlockHeader,
    medium::{StorageMedium, WriteGranularity},
    trace, warn, StorageError,
};

// Add new ones so that ANDing two variant together results in an invalid bit pattern.
// Do not use 0xFF as it is reserved for the free state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ObjectType {
    FileMetadata = 0x8F,
    FileData = 0x8E,
    FileName = 0x8D,
}

impl ObjectType {
    fn parse(byte: u8) -> Option<Self> {
        match byte {
            v if v == Self::FileMetadata as u8 => Some(Self::FileMetadata),
            v if v == Self::FileData as u8 => Some(Self::FileData),
            v if v == Self::FileName as u8 => Some(Self::FileName),
            _ => None,
        }
    }

    fn byte_count() -> usize {
        1
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ObjectDataState {
    Untrusted = 0xFF,
    Valid = 0xF0,
    Deleted = 0x00,
}

impl ObjectDataState {
    fn parse(byte: u8) -> Result<Self, StorageError> {
        match byte {
            v if v == Self::Untrusted as u8 => Ok(Self::Untrusted),
            v if v == Self::Valid as u8 => Ok(Self::Valid),
            v if v == Self::Deleted as u8 => Ok(Self::Deleted),
            _ => {
                warn!("Unknown object data state: {:02X}", byte);
                Err(StorageError::FsCorrupted)
            }
        }
    }

    fn parse_pair(finalized_byte: u8, deleted_byte: u8) -> Result<Self, StorageError> {
        match (finalized_byte, deleted_byte) {
            (0xFF, 0xFF) => Ok(Self::Untrusted),
            (0x00, 0xFF) => Ok(Self::Valid),
            (0x00, 0x00) => Ok(Self::Deleted),
            _ => {
                warn!(
                    "Unknown object data state: ({:02X}, {:02X})",
                    finalized_byte, deleted_byte
                );
                Err(StorageError::FsCorrupted)
            }
        }
    }

    /// Converts the state into an `(offset, value)` pair.
    fn into_raw<M: StorageMedium>(self) -> (usize, u8) {
        const STATE_FLAG_SET_BYTE: u8 = 0;

        match M::WRITE_GRANULARITY {
            WriteGranularity::Bit => (1, self as u8),
            WriteGranularity::Word(w) if self == Self::Valid => (w, STATE_FLAG_SET_BYTE),
            WriteGranularity::Word(w) if self == Self::Deleted => (2 * w, STATE_FLAG_SET_BYTE),
            WriteGranularity::Word(_) => panic!("Can't write this value"),
        }
    }

    fn byte_count<M: StorageMedium>() -> usize {
        match M::WRITE_GRANULARITY {
            WriteGranularity::Bit => 1,
            WriteGranularity::Word(w) => 2 * w,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum CompositeObjectState {
    Free,
    Allocated(ObjectType, ObjectDataState),
}

impl From<CompositeObjectState> for ObjectState {
    fn from(value: CompositeObjectState) -> Self {
        match value {
            CompositeObjectState::Free => Self::Free,
            CompositeObjectState::Allocated(_, data) => match data {
                ObjectDataState::Untrusted => Self::Allocated,
                ObjectDataState::Valid => Self::Finalized,
                ObjectDataState::Deleted => Self::Deleted,
            },
        }
    }
}

impl CompositeObjectState {
    fn transition_state(
        self,
        new_state: ObjectState,
        object_type: ObjectType,
    ) -> Result<Self, StorageError> {
        let current_state = ObjectState::from(self);

        if current_state > new_state {
            // Can't go backwards in state
            error!(
                "Can't change object state from {:?} to {:?}",
                current_state, new_state
            );
            return Err(StorageError::InvalidOperation);
        }

        if let Self::Allocated(ty, _) = self {
            // Can't change allocated object type
            if ty != object_type {
                error!(
                    "Can't change object type from {:?} to {:?}",
                    ty, object_type
                );
                return Err(StorageError::InvalidOperation);
            }
        }

        let new_data_state = match new_state {
            ObjectState::Free => unreachable!(),
            ObjectState::Allocated => ObjectDataState::Untrusted,
            ObjectState::Finalized => ObjectDataState::Valid,
            ObjectState::Deleted => ObjectDataState::Deleted,
        };
        Ok(Self::Allocated(object_type, new_data_state))
    }

    fn byte_count<M: StorageMedium>() -> usize {
        M::align(ObjectType::byte_count()) + M::align(ObjectDataState::byte_count::<M>())
    }

    pub async fn read<M: StorageMedium>(
        medium: &mut M,
        location: ObjectLocation,
    ) -> Result<Self, StorageError> {
        trace!("CompositeObjectState::read({:?})", location);
        let bytes_read = Self::byte_count::<M>();

        let mut buffer = [0; 12];
        assert!(bytes_read <= buffer.len());
        let buffer = &mut buffer[..bytes_read];

        medium.read(location.block, location.offset, buffer).await?;

        match ObjectType::parse(buffer[0]) {
            Some(ty) => {
                let data_state = match M::WRITE_GRANULARITY {
                    WriteGranularity::Bit => ObjectDataState::parse(buffer[1])?,
                    WriteGranularity::Word(w) => {
                        ObjectDataState::parse_pair(buffer[w], buffer[2 * w])?
                    }
                };

                Ok(Self::Allocated(ty, data_state))
            }
            None if buffer[0] == 0xFF => Ok(Self::Free),
            None => {
                warn!("Unknown object type: {:02X}", buffer[0]);
                Err(StorageError::FsCorrupted)
            }
        }
    }

    pub async fn allocate<M: StorageMedium>(
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
    ) -> Result<Self, StorageError> {
        trace!(
            "CompositeObjectState::allocate({:?}, {:?})",
            location,
            object_type
        );
        let this = Self::read(medium, location).await?;
        let this = this.transition_state(ObjectState::Allocated, object_type)?;

        Self::write_object_type(medium, location, object_type).await?;

        Ok(this)
    }

    async fn update_state<M: StorageMedium>(
        self,
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
        state: ObjectState,
    ) -> Result<Self, StorageError> {
        trace!(
            "CompositeObjectState::update_state({:?}, {:?}) -> {:?}",
            location,
            object_type,
            state
        );
        let this = self.transition_state(state, object_type)?;

        Self::write_object_data_state(medium, location, state).await?;

        Ok(this)
    }

    pub async fn finalize<M: StorageMedium>(
        self,
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
    ) -> Result<Self, StorageError> {
        self.update_state(medium, location, object_type, ObjectState::Finalized)
            .await
    }

    pub async fn delete<M: StorageMedium>(
        self,
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
    ) -> Result<Self, StorageError> {
        self.update_state(medium, location, object_type, ObjectState::Deleted)
            .await
    }

    fn object_type(self) -> Result<ObjectType, StorageError> {
        match self {
            Self::Allocated(ty, _) => Ok(ty),
            Self::Free => {
                error!("Can't read object of type Free");
                Err(StorageError::InvalidOperation)
            }
        }
    }

    async fn write_object_type<M: StorageMedium>(
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
    ) -> Result<(), StorageError> {
        medium
            .write(location.block, location.offset, &[object_type as u8])
            .await?;

        Ok(())
    }

    async fn write_object_data_state<M: StorageMedium>(
        medium: &mut M,
        location: ObjectLocation,
        state: ObjectState,
    ) -> Result<(), StorageError> {
        let data_state = match state {
            ObjectState::Free | ObjectState::Allocated => {
                return Err(StorageError::InvalidOperation);
            }
            ObjectState::Finalized => ObjectDataState::Valid,
            ObjectState::Deleted => ObjectDataState::Deleted,
        };

        let (offset, byte) = data_state.into_raw::<M>();
        medium
            .write(location.block, location.offset + offset, &[byte])
            .await?;

        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ObjectState {
    Free,
    Allocated,
    Finalized,
    Deleted,
}

impl ObjectState {
    fn is_free(self) -> bool {
        matches!(self, ObjectState::Free)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ObjectLocation {
    pub block: usize,
    pub offset: usize,
}

impl ObjectLocation {
    pub fn into_bytes<M: StorageMedium>(self) -> heapless08::Vec<u8, 8> {
        let mut bytes = heapless08::Vec::<u8, 8>::new();

        bytes
            .extend_from_slice(&self.block.to_le_bytes()[..M::block_count_bytes()])
            .unwrap();

        bytes
            .extend_from_slice(&self.offset.to_le_bytes()[..M::block_size_bytes()])
            .unwrap();

        bytes
    }

    fn from_bytes<M: StorageMedium>(bytes: &[u8]) -> Self {
        fn u32_from_le_byte_slice(bytes: &[u8]) -> u32 {
            let mut buf = [0u8; 4];
            buf[0..bytes.len()].copy_from_slice(bytes);
            u32::from_le_bytes(buf)
        }

        debug_assert_eq!(bytes.len(), M::object_location_bytes());

        let (block_idx_byte_slice, offset_byte_slice) = bytes.split_at(M::block_count_bytes());

        Self {
            block: u32_from_le_byte_slice(block_idx_byte_slice) as usize,
            offset: u32_from_le_byte_slice(offset_byte_slice) as usize,
        }
    }

    pub(crate) async fn read_metadata<M: StorageMedium>(
        self,
        medium: &mut M,
    ) -> Result<MetadataObjectHeader<M>, StorageError> {
        if let Some(info) = ObjectInfo::read(self, medium).await? {
            info.read_metadata(medium).await
        } else {
            Err(StorageError::FsCorrupted)
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
struct ObjectPayloadSize {
    payload_size: usize, // At most block size - header
}

impl ObjectPayloadSize {
    pub fn unset<M: StorageMedium>() -> Self {
        Self {
            // A number of 0xFF bytes that is the size of the object size field.
            payload_size: (1 << (M::object_size_bytes() * 8)) - 1,
        }
    }

    // TODO: this encodes that size follows the object state.
    pub fn payload_size_offset<M: StorageMedium>() -> usize {
        M::align(CompositeObjectState::byte_count::<M>())
    }

    // TODO: we might want to pass in the offset directly
    async fn read<M: StorageMedium>(
        location: ObjectLocation,
        medium: &mut M,
    ) -> Result<Self, StorageError> {
        let mut object_size_bytes = [0; 4];

        medium
            .read(
                location.block,
                location.offset + Self::payload_size_offset::<M>(),
                &mut object_size_bytes[0..M::object_size_bytes()],
            )
            .await?;

        Ok(Self {
            payload_size: u32::from_le_bytes(object_size_bytes) as usize,
        })
    }

    pub fn payload_size<M: StorageMedium>(&self) -> Option<usize> {
        if *self == Self::unset::<M>() {
            None
        } else {
            Some(self.payload_size)
        }
    }

    pub fn byte_count<M: StorageMedium>() -> usize {
        M::object_size_bytes()
    }
}

/// Filesystem object header.
///
/// Each object has a header which contains the object's state and type,
/// and the size of the object's payload.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ObjectHeader {
    state: CompositeObjectState,
    payload_size: ObjectPayloadSize,
    pub location: ObjectLocation,
}

impl ObjectHeader {
    pub fn byte_count<M: StorageMedium>() -> usize {
        // We need to align both because they are written separately
        M::align(CompositeObjectState::byte_count::<M>())
            + M::align(ObjectPayloadSize::byte_count::<M>())
    }

    pub fn payload_offset<M: StorageMedium>() -> usize {
        ObjectPayloadSize::payload_size_offset::<M>()
    }

    pub async fn read<M: StorageMedium>(
        location: ObjectLocation,
        medium: &mut M,
    ) -> Result<Self, StorageError> {
        trace!("ObjectHeader::read({:?})", location);
        let state = CompositeObjectState::read(medium, location).await?;
        let payload_size = ObjectPayloadSize::read(location, medium).await?;

        Ok(Self {
            state,
            payload_size,
            location,
        })
    }

    pub async fn allocate<M: StorageMedium>(
        medium: &mut M,
        location: ObjectLocation,
        object_type: ObjectType,
    ) -> Result<Self, StorageError> {
        debug!("ObjectHeader::allocate({:?}, {:?})", location, object_type);

        let state = CompositeObjectState::allocate(medium, location, object_type).await?;

        Ok(Self {
            state,
            payload_size: ObjectPayloadSize::unset::<M>(),
            location,
        })
    }

    pub fn state(&self) -> ObjectState {
        self.state.into()
    }

    pub fn object_type(&self) -> Result<ObjectType, StorageError> {
        self.state.object_type()
    }

    pub fn payload_size<M: StorageMedium>(&self) -> Option<usize> {
        self.payload_size.payload_size::<M>()
    }

    pub async fn update_state<M: StorageMedium>(
        &mut self,
        medium: &mut M,
        state: ObjectState,
    ) -> Result<(), StorageError> {
        debug_assert!(!state.is_free());

        trace!(
            "ObjectHeader::update_state({:?}, {:?})",
            self.location,
            state
        );

        if state == self.state.into() {
            return Ok(());
        }

        let object_type = self.object_type()?;

        self.state = match state {
            ObjectState::Finalized | ObjectState::Deleted => {
                self.state
                    .update_state(medium, self.location, object_type, state)
                    .await?
            }
            ObjectState::Allocated => return Err(StorageError::InvalidOperation),
            ObjectState::Free => unreachable!(),
        };

        Ok(())
    }

    pub async fn set_payload_size<M: StorageMedium>(
        &mut self,
        medium: &mut M,
        size: usize,
    ) -> Result<(), StorageError> {
        trace!(
            "ObjectHeader::set_payload_size({:?}, {})",
            self.location,
            size
        );

        if self.payload_size::<M>().is_some() {
            warn!("payload size already set");
            return Err(StorageError::InvalidOperation);
        }

        let bytes = size.to_le_bytes();
        medium
            .write(
                self.location.block,
                self.location.offset + Self::payload_offset::<M>(),
                &bytes[0..M::object_size_bytes()],
            )
            .await?;

        self.payload_size.payload_size = size;

        Ok(())
    }
}

// Object payload contains a list of object locations.
pub struct MetadataObjectHeader<M: StorageMedium> {
    pub object: ObjectHeader,
    pub path_hash: u32,
    pub filename_location: ObjectLocation,
    data_object_cursor: usize, // Used to iterate through the list of object locations.
    _parent: Option<ObjectLocation>,
    _medium: PhantomData<M>,
}

impl<M: StorageMedium> Clone for MetadataObjectHeader<M> {
    fn clone(&self) -> Self {
        Self {
            object: self.object,
            path_hash: self.path_hash,
            filename_location: self.filename_location,
            data_object_cursor: self.data_object_cursor,
            _parent: self._parent,
            _medium: PhantomData,
        }
    }
}

impl<M: StorageMedium> MetadataObjectHeader<M> {
    /// Returns the location of the next data object of the current file.
    pub async fn next_object_location(
        &mut self,
        medium: &mut M,
    ) -> Result<Option<ObjectLocation>, StorageError> {
        trace!("MetadataObjectHeader::next_object_location()");

        let location = if !self.at_last_data_object() {
            let location = self.read_location(medium).await?;
            self.data_object_cursor += M::object_location_bytes();
            Some(location)
        } else {
            None
        };

        Ok(location)
    }

    fn metadata_header_size() -> usize {
        4 + M::object_location_bytes() // path hash + filename location
    }

    async fn read_location(&self, medium: &mut M) -> Result<ObjectLocation, StorageError> {
        let mut location_bytes = [0; 8];
        let location_bytes = &mut location_bytes[0..M::object_location_bytes()];

        medium
            .read(
                self.location().block,
                self.location().offset
                    + M::align(ObjectHeader::byte_count::<M>())
                    + Self::metadata_header_size()
                    + self.data_object_cursor,
                location_bytes,
            )
            .await?;

        Ok(ObjectLocation::from_bytes::<M>(location_bytes))
    }

    fn at_last_data_object(&self) -> bool {
        self.data_object_cursor
            >= self
                .object
                .payload_size::<M>()
                .map(|size| size - Self::metadata_header_size())
                .unwrap_or(0)
    }

    pub fn location(&self) -> ObjectLocation {
        self.object.location
    }

    pub fn reset(&mut self) {
        self.data_object_cursor = 0;
    }
}

pub struct ObjectWriter<M: StorageMedium> {
    object: ObjectHeader,
    cursor: usize,
    buffer: heapless08::Vec<u8, 4>, // TODO: support larger word sizes?
    _medium: PhantomData<M>,
}

impl<M: StorageMedium> ObjectWriter<M> {
    pub async fn allocate(
        location: ObjectLocation,
        object_type: ObjectType,
        medium: &mut M,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            object: ObjectHeader::allocate(medium, location, object_type).await?,
            cursor: 0,
            buffer: heapless08::Vec::new(),
            _medium: PhantomData,
        })
    }

    fn fill_buffer<'d>(&mut self, data: &'d [u8]) -> &'d [u8] {
        // Buffering is not necessary if we can write arbitrary bits.
        match M::WRITE_GRANULARITY {
            WriteGranularity::Bit | WriteGranularity::Word(1) => data,
            WriteGranularity::Word(len) => {
                let copied = data.len().min(len - self.buffer.len());
                self.buffer.extend_from_slice(&data[0..copied]).unwrap();

                &data[copied..]
            }
        }
    }

    fn can_flush(&self) -> bool {
        match M::WRITE_GRANULARITY {
            WriteGranularity::Bit | WriteGranularity::Word(1) => false,
            WriteGranularity::Word(len) => self.buffer.len() == len,
        }
    }

    async fn append_payload(&mut self, medium: &mut M, buffer: &[u8]) -> Result<(), StorageError> {
        medium
            .write(self.object.location.block, self.data_write_offset(), buffer)
            .await?;
        self.cursor += buffer.len();

        Ok(())
    }

    async fn flush(&mut self, medium: &mut M) -> Result<(), StorageError> {
        // Buffering is not necessary if we can write arbitrary bits.
        if let WriteGranularity::Bit | WriteGranularity::Word(1) = M::WRITE_GRANULARITY {
            return Ok(());
        }

        if !self.buffer.is_empty() {
            // FIXME: This copy is a bit unfortunate, but it's only 4 bytes. We should still
            // probably refactor this to avoid the copy.
            let buffer = core::mem::take(&mut self.buffer);
            self.append_payload(medium, &buffer).await?;
        }

        Ok(())
    }

    pub async fn write_to(
        location: ObjectLocation,
        object_type: ObjectType,
        medium: &mut M,
        data: &[u8],
    ) -> Result<usize, StorageError> {
        let mut this = Self::allocate(location, object_type, medium).await?;

        this.write(medium, data).await?;
        let info = this.finalize(medium).await?;
        Ok(info.total_size())
    }

    fn data_write_offset(&self) -> usize {
        let header_size = ObjectHeader::byte_count::<M>();
        self.object.location.offset + header_size + self.cursor
    }

    pub fn space(&self) -> usize {
        M::BLOCK_SIZE - self.data_write_offset() - self.buffer.len()
    }

    pub async fn write(&mut self, medium: &mut M, mut data: &[u8]) -> Result<(), StorageError> {
        if self.object.state() != ObjectState::Allocated {
            return Err(StorageError::InvalidOperation);
        }

        if self.space() < data.len() {
            debug!(
                "Insufficient space ({}) to write data ({})",
                self.space(),
                data.len()
            );
            return Err(StorageError::InsufficientSpace);
        }

        if !self.buffer.is_empty() {
            data = self.fill_buffer(data);
            if self.can_flush() {
                self.flush(medium).await?;
            }
        }

        let len = data.len();
        let aligned_count = len - len % M::WRITE_GRANULARITY.width();

        let (aligned, remaining) = data.split_at(aligned_count);
        self.append_payload(medium, aligned).await?;

        data = self.fill_buffer(remaining);
        debug_assert!(data.is_empty());

        Ok(())
    }

    async fn write_payload_size(&mut self, medium: &mut M) -> Result<(), StorageError> {
        self.flush(medium).await?;
        self.object.set_payload_size(medium, self.cursor).await
    }

    async fn set_state(&mut self, medium: &mut M, state: ObjectState) -> Result<(), StorageError> {
        self.object.update_state(medium, state).await
    }

    pub fn location(&self) -> ObjectLocation {
        self.object.location
    }

    pub fn payload_size(&self) -> usize {
        self.cursor + self.buffer.len()
    }

    pub fn total_size(&self) -> usize {
        ObjectHeader::byte_count::<M>() + self.payload_size()
    }

    pub async fn finalize(mut self, medium: &mut M) -> Result<ObjectInfo<M>, StorageError> {
        if self.object.state() != ObjectState::Allocated {
            error!("Can not finalize object in state {:?}", self.object.state());
            return Err(StorageError::InvalidOperation);
        }

        // must be two different writes for powerloss safety
        self.write_payload_size(medium).await?;
        self.set_state(medium, ObjectState::Finalized).await?;

        Ok(ObjectInfo::with_header(self.object))
    }

    pub async fn delete(mut self, medium: &mut M) -> Result<(), StorageError> {
        if let ObjectState::Free | ObjectState::Deleted = self.object.state() {
            error!("Can not delete object in state {:?}", self.object.state());
            return Ok(());
        }

        if self.object.state() == ObjectState::Allocated {
            self.write_payload_size(medium).await?;
        }

        self.set_state(medium, ObjectState::Deleted).await
    }

    pub(crate) async fn copy_from(
        &mut self,
        source: &mut ObjectReader<M>,
        medium: &mut M,
    ) -> Result<(), StorageError> {
        let mut buffer = [0; 16];
        while source.remaining() > 0 {
            let read_size = source.read(medium, &mut buffer).await?;
            self.write(medium, &buffer[0..read_size]).await?;
        }

        Ok(())
    }
}

pub struct ObjectReader<M: StorageMedium> {
    location: ObjectLocation,
    object: ObjectHeader,
    cursor: usize,
    _medium: PhantomData<M>,
}

impl<M: StorageMedium> ObjectReader<M> {
    pub async fn new(
        location: ObjectLocation,
        medium: &mut M,
        allow_non_finalized: bool,
    ) -> Result<Self, StorageError> {
        trace!("ObjectReader::new({:?})", location);

        // We read back the header to ensure that the object is in a valid state.
        let object = ObjectHeader::read(location, medium).await?;

        if object.state() != ObjectState::Finalized {
            if allow_non_finalized && object.state() != ObjectState::Free {
                // We can read data from unfinalized/deleted objects if the caller allows it.
            } else {
                // We can only read data from finalized objects.
                error!("Trying to read {:?} object", object.state());
                return Err(StorageError::FsCorrupted);
            }
        }

        Ok(Self {
            location,
            object,
            cursor: 0,
            _medium: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.object.payload_size::<M>().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> usize {
        self.len() - self.cursor
    }

    pub fn rewind(&mut self) {
        self.cursor = 0;
    }

    pub async fn read(&mut self, medium: &mut M, buf: &mut [u8]) -> Result<usize, StorageError> {
        let read_offset = self.location.offset + ObjectHeader::byte_count::<M>() + self.cursor;
        let read_size = buf.len().min(self.remaining());

        medium
            .read(self.location.block, read_offset, &mut buf[0..read_size])
            .await?;

        self.cursor += read_size;

        Ok(read_size)
    }
}

pub struct ObjectInfo<M: StorageMedium> {
    pub header: ObjectHeader,
    _medium: PhantomData<M>,
}

impl<M: StorageMedium> ObjectInfo<M> {
    fn with_header(header: ObjectHeader) -> Self {
        Self {
            header,
            _medium: PhantomData,
        }
    }

    pub fn state(&self) -> ObjectState {
        self.header.state()
    }

    pub fn total_size(&self) -> usize {
        ObjectHeader::byte_count::<M>() + self.header.payload_size::<M>().unwrap_or(0)
    }

    pub fn location(&self) -> ObjectLocation {
        self.header.location
    }

    pub async fn read_metadata(
        &self,
        medium: &mut M,
    ) -> Result<MetadataObjectHeader<M>, StorageError> {
        let mut path_hash_bytes = [0; 4];
        let path_hash_offset = self.location().offset + M::align(ObjectHeader::byte_count::<M>());
        medium
            .read(
                self.location().block,
                path_hash_offset,
                &mut path_hash_bytes,
            )
            .await?;

        let mut filename_location_bytes = [0; 8];
        let filename_location_bytes = &mut filename_location_bytes[0..M::object_location_bytes()];

        medium
            .read(
                self.location().block,
                path_hash_offset + 4,
                filename_location_bytes,
            )
            .await?;

        Ok(MetadataObjectHeader {
            object: self.header,
            path_hash: u32::from_le_bytes(path_hash_bytes),
            filename_location: ObjectLocation::from_bytes::<M>(filename_location_bytes),
            data_object_cursor: 0,
            _parent: None,
            _medium: PhantomData,
        })
    }

    pub async fn read(
        location: ObjectLocation,
        medium: &mut M,
    ) -> Result<Option<Self>, StorageError> {
        trace!("ObjectInfo::read({:?})", location);
        let header = ObjectHeader::read(location, medium).await?;
        trace!("ObjectInfo::read({:?}) -> {:?}", location, header);

        if header.state().is_free() {
            return Ok(None);
        }

        Ok(Some(Self::with_header(header)))
    }

    pub async fn copy_object(
        &self,
        medium: &mut M,
        dst: ObjectLocation,
    ) -> Result<Self, StorageError> {
        let mut source = ObjectReader::new(self.location(), medium, false).await?;
        let mut target = ObjectWriter::allocate(dst, self.header.object_type()?, medium).await?;

        target.copy_from(&mut source, medium).await?;

        target.finalize(medium).await
    }

    pub async fn move_object(
        self,
        medium: &mut M,
        dst: ObjectLocation,
    ) -> Result<Self, StorageError> {
        let new = self.copy_object(medium, dst).await?;
        self.delete(medium).await?;

        Ok(new)
    }

    pub async fn finalize(mut self, medium: &mut M) -> Result<Self, StorageError> {
        self.header
            .update_state(medium, ObjectState::Finalized)
            .await?;

        Ok(self)
    }

    pub async fn delete(mut self, medium: &mut M) -> Result<(), StorageError> {
        self.header.update_state(medium, ObjectState::Deleted).await
    }
}

pub struct ObjectIterator {
    location: ObjectLocation,
}

impl ObjectIterator {
    pub fn new<M: StorageMedium>(block: usize) -> Self {
        Self {
            location: ObjectLocation {
                block,
                offset: BlockHeader::<M>::byte_count(),
            },
        }
    }

    pub async fn next<M: StorageMedium>(
        &mut self,
        medium: &mut M,
    ) -> Result<Option<ObjectInfo<M>>, StorageError> {
        if self.location.offset + ObjectHeader::byte_count::<M>() >= M::BLOCK_SIZE {
            return Ok(None);
        }

        let info = ObjectInfo::read(self.location, medium).await?;
        if let Some(info) = info.as_ref() {
            self.location.offset += M::align(info.total_size());
        }

        Ok(info)
    }

    pub(crate) fn current_offset(&self) -> usize {
        self.location.offset
    }

    pub(crate) fn block_idx(&self) -> usize {
        self.location.block
    }
}

use bytes::{Bytes, BytesMut};
use std::alloc::{alloc, dealloc, Layout};
use std::ops::{Deref, DerefMut};

/// Calculate CRC32C using hardware-accelerated instructions where available.
#[inline]
pub fn compute_crc32c(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// An aligned memory buffer suitable for Linux `O_DIRECT` raw device I/O.
/// Direct I/O requires buffer addresses and lengths to be multiples of the block size (typically 4096 bytes).
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    capacity: usize,
    len: usize,
}

// Safety: The buffer owns its allocated memory and is safe to transfer between threads.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    pub const DEFAULT_ALIGNMENT: usize = 4096;

    /// Allocate a new buffer aligned to `alignment` bytes (default 4096).
    pub fn new(capacity: usize, alignment: usize) -> Self {
        assert!(alignment > 0 && alignment.is_power_of_two());
        let rounded_capacity = (capacity + alignment - 1) & !(alignment - 1);
        let layout = Layout::from_size_align(rounded_capacity, alignment)
            .expect("Valid memory layout for aligned buffer");

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        Self {
            ptr,
            layout,
            capacity: rounded_capacity,
            len: 0,
        }
    }

    pub fn with_default_alignment(capacity: usize) -> Self {
        Self::new(capacity, Self::DEFAULT_ALIGNMENT)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Appends a byte slice to the aligned buffer.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> bool {
        if self.len + data.len() > self.capacity {
            return false;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len += data.len();
        true
    }

    /// Sets the length manually (e.g. after a direct read).
    ///
    /// # Safety
    /// Caller must ensure `new_len <= self.capacity` and bytes up to `new_len` are initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity);
        self.len = new_len;
    }

    /// Returns a raw pointer to the aligned memory.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Returns a mutable raw pointer to the aligned memory.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Converts the initialized prefix into an immutable `Bytes` object without copying if possible,
    /// or by slicing into a new Bytes.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self[..self.len])
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

/// Helper for splitting a byte stream efficiently.
pub fn slice_range(bytes: &Bytes, start: usize, len: usize) -> Bytes {
    bytes.slice(start..start + len)
}

/// Zero-copy byte ring buffer utilities.
#[derive(Debug, Default)]
pub struct ByteAccumulator {
    inner: BytesMut,
}

impl ByteAccumulator {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(capacity),
        }
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.inner.extend_from_slice(slice);
    }

    pub fn freeze(&mut self) -> Bytes {
        self.inner.split().freeze()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c() {
        let data = b"oxideMq-streaming-storage";
        let crc1 = compute_crc32c(data);
        let crc2 = compute_crc32c(data);
        assert_eq!(crc1, crc2);
        assert_ne!(crc1, 0);
    }

    #[test]
    fn test_aligned_buffer() {
        let mut buf = AlignedBuffer::with_default_alignment(8192);
        assert_eq!(buf.capacity() % 4096, 0);
        assert_eq!((buf.as_ptr() as usize) % 4096, 0);

        let test_data = b"hello direct io world";
        assert!(buf.extend_from_slice(test_data));
        assert_eq!(&buf[..], test_data);
        assert_eq!(buf.to_bytes().as_ref(), test_data);
    }

    #[test]
    fn test_byte_accumulator() {
        let mut acc = ByteAccumulator::with_capacity(64);
        acc.extend_from_slice(b"record-batch-1");
        let frozen = acc.freeze();
        assert_eq!(frozen.as_ref(), b"record-batch-1");
        assert_eq!(acc.len(), 0);
    }
}

//! Bounded buffered readers and simple read-ahead with a fixed cap.
//!
//! For now we rely on `BufReader` with an explicit capacity to bound the in-flight
//! buffer. Exec/planner can layer scheduling/backpressure around this as needed.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

/// A thin wrapper over `BufReader` with a fixed capacity to bound in-flight bytes.
pub struct BoundedBufReader<R: Read> {
    inner: BufReader<R>,
}

impl<R: Read> BoundedBufReader<R> {
    /// Create a new bounded reader with a maximum internal buffer size.
    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            inner: BufReader::with_capacity(capacity, reader),
        }
    }

    /// Access the underlying buffer length (bytes currently buffered).
    pub fn buffer_len(&self) -> usize {
        self.inner.buffer().len()
    }

    /// Consume `amt` bytes from the internal buffer (like `BufRead::consume`).
    pub fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

impl<R: Read> Read for BoundedBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<R: Read> BufRead for BoundedBufReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

/// Convenience helper to create a bounded reader from a file path.
pub fn bounded_from_path<P: AsRef<Path>>(
    path: P,
    cap: usize,
) -> io::Result<BoundedBufReader<File>> {
    let file = File::open(path)?;
    Ok(BoundedBufReader::with_capacity(cap, file))
}

//! Small binary reader and writer used by the file formats (little-endian, varints, length-prefixed strings).

use crate::error::{Error, Result};
use crate::varint;

/// Appends binary values to a buffer.
#[derive(Default)]
pub(crate) struct Writer {
    pub(crate) buf: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub(crate) fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn varint(&mut self, v: u64) {
        varint::put(&mut self.buf, v);
    }

    pub(crate) fn bytes(&mut self, v: &[u8]) {
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }

    pub(crate) fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
}

/// Reads binary values from a slice; every read can fail with [`Error::Corrupt`].
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(len).filter(|&end| end <= self.buf.len()).ok_or(Error::Corrupt("unexpected end of data"))?;
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    pub(crate) fn varint(&mut self) -> Result<u64> {
        varint::get(self.buf, &mut self.pos)
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.varint()?).map_err(|_| Error::Corrupt("length"))?;
        self.take(len)
    }

    pub(crate) fn str(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| Error::Corrupt("text is not UTF-8"))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_truncation() {
        let mut w = Writer::new();
        w.u8(7);
        w.u32(0xdead_beef);
        w.u64(u64::MAX - 1);
        w.varint(300);
        w.str("héllo");
        w.bytes(&[1, 2, 3]);
        let mut r = Reader::new(&w.buf);
        assert_eq!((r.u8().unwrap(), r.u32().unwrap(), r.u64().unwrap(), r.varint().unwrap()), (7, 0xdead_beef, u64::MAX - 1, 300));
        assert_eq!((r.str().unwrap(), r.bytes().unwrap()), ("héllo".to_string(), &[1u8, 2, 3][..]));
        assert!(r.is_empty());
        for cut in 0..w.buf.len() {
            let mut r = Reader::new(&w.buf[..cut]);
            let _ =
                r.u8().and_then(|_| r.u32()).and_then(|_| r.u64()).and_then(|_| r.varint()).and_then(|_| r.str()).and_then(|_| r.bytes());
        }
        assert!(Reader::new(&[5, b'a']).str().is_err());
    }
}

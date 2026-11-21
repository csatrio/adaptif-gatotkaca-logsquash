//! LEB128 variable-length integers.

use crate::error::{Error, Result};
use bytes::BufMut;

/// Appends `v` to `out` as a LEB128 varint (1 to 10 bytes).
pub fn put(out: &mut impl BufMut, mut v: u64) {
    while v >= 0x80 {
        out.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    out.put_u8(v as u8);
}

/// Reads a varint from `buf` at `*pos` and advances `*pos` past it.
pub fn get(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *buf.get(*pos).ok_or(Error::Corrupt("varint: unexpected end"))?;
        *pos += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(Error::Corrupt("varint: overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_boundaries() {
        for v in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put(&mut buf, v);
            let mut pos = 0;
            assert_eq!(get(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn sizes() {
        let size = |v| {
            let mut b = Vec::new();
            put(&mut b, v);
            b.len()
        };
        assert_eq!((size(127), size(128), size(u64::MAX)), (1, 2, 10));
    }

    #[test]
    fn rejects_truncated_and_overlong() {
        assert!(get(&[0x80], &mut 0).is_err());
        assert!(get(&[0xff; 11], &mut 0).is_err());
    }
}

use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{RespDecode, RespEncode, RespError};

use super::{parse_length, CRLF_LEN};

#[derive(Debug, Clone, PartialEq)]
pub struct BulkString(pub(crate) Vec<u8>);

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16);
        let len = if self.len() == 0 {
            -1
        } else {
            self.len() as isize
        };
        buf.extend_from_slice(&format!("${}\r\n", len).into_bytes());
        if len > 0 {
            buf.extend_from_slice(&self);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        if len == 0 {
            // 处理 Null bulk string 的情况
            // 吃掉 empty string: $0\r\n\r\n 字符，n表示数字，如bulk string字符长度为 $0\r\n\r\n
            buf.advance(end + CRLF_LEN + CRLF_LEN);
            return Ok(BulkString::new(""));
        }
        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespError::NotComplete);
        }
        // 吃掉 $n\r\n 字符，n表示数字，如bulk string字符长度为 $3\r\n
        buf.advance(end + CRLF_LEN);
        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString::new(data[..len].to_vec()))
    }
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        BulkString(s.as_bytes().to_vec())
    }
}

impl From<String> for BulkString {
    fn from(s: String) -> Self {
        BulkString(s.into_bytes())
    }
}

impl From<&[u8]> for BulkString {
    fn from(s: &[u8]) -> Self {
        BulkString(s.to_vec())
    }
}

impl<const N: usize> From<[u8; N]> for BulkString {
    fn from(s: [u8; N]) -> Self {
        BulkString(s.to_vec())
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::BufMut;

    use super::*;
    use crate::RespFrame;

    #[test]
    fn test_bulk_string_encode() -> Result<()> {
        let frame: RespFrame = BulkString::new(b"hello").into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
        Ok(())
    }

    #[test]
    fn test_null_bulk_string_encode() -> Result<()> {
        let frame: RespFrame = BulkString::new(b"").into();
        assert_eq!(frame.encode(), b"$-1\r\n");
        Ok(())
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"$5\r\nhello\r\n");
        let frame = BulkString::decode(&mut buf)?;

        let expected_frame = BulkString::new(b"hello");
        assert_eq!(frame, expected_frame);
        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"$0\r\n\r\n");
        let frame = BulkString::decode(&mut buf)?;

        assert_eq!(frame, "".into());
        Ok(())
    }
}

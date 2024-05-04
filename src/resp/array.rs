use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{RespDecode, RespEncode, RespError, RespFrame};

use super::{calc_total_length, parse_length, BUFF_CAP, CRLF_LEN};

#[derive(Debug, Clone, PartialEq)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUFF_CAP);
        let len = if self.len() == 0 {
            -1
        } else {
            self.len() as isize
        };
        buf.extend_from_slice(&format!("*{}\r\n", len).into_bytes());
        if len > 0 {
            for frame in self.0 {
                buf.extend_from_slice(&frame.encode());
            }
        }
        buf
    }
}

impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        if len == 0 {
            // 吃掉 empty array *0\r\n\r\n 字符，n表示数字，如数组字符长度为 *0\r\n\r\n
            buf.advance(end + CRLF_LEN + CRLF_LEN);
            return Ok(RespArray::new([]));
        }
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        // 吃掉 *n\r\n 字符，n表示数字，如数组字符长度为 *3\r\n
        buf.advance(end + CRLF_LEN);
        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }
        Ok(RespArray::new(frames))
    }
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Self(s.into())
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::BufMut;

    use super::*;
    use crate::{BulkString, RespFrame};

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = RespArray::new(vec![
            BulkString::new("set".to_string()).into(),
            BulkString::new("hello".to_string()).into(),
            BulkString::new("world".to_string()).into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        )
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespArray::new([]).into();
        assert_eq!(frame.encode(), b"*-1\r\n")
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespArray::new([b"set".into(), b"hello".into(), b"world".into(),])
        );

        buf.put_slice(b"*2\r\n$3\r\nset\r\n");
        let err = RespArray::decode(&mut buf).unwrap_err();
        assert_eq!(err, RespError::NotComplete);

        buf.put_slice(b"$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*0\r\n\r\n");

        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([]));
        Ok(())
    }
}

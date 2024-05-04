use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{RespDecode, RespEncode, RespError};

use super::{calc_total_length, frame::RespFrame, parse_length, BUFF_CAP, CRLF_LEN};

#[derive(Debug, Clone, PartialEq)]
pub struct RespSet(pub(crate) Vec<RespFrame>);

// -set: "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUFF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        // 吃掉 ~n\r\n 字符，n表示数字，如set字符长度为 ~3\r\n
        buf.advance(end + CRLF_LEN);
        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }
        Ok(RespSet::new(frames))
    }
    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::BufMut;

    use super::*;
    use crate::{BulkString, RespArray, RespFrame};

    #[test]
    fn test_set_encode() -> Result<()> {
        let frame: RespFrame = RespSet::new([
            RespArray::new([1234.into(), true.into()]).into(),
            BulkString::new("world".to_string()).into(),
        ])
        .into();
        let data = frame.encode();
        assert_eq!(data, b"~2\r\n*2\r\n:+1234\r\n#t\r\n$5\r\nworld\r\n");
        Ok(())
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"~2\r\n*2\r\n:+1234\r\n#t\r\n$5\r\nworld\r\n");
        let frame = RespSet::decode(&mut buf)?;

        let expected_frame = RespSet::new([
            RespArray::new([1234.into(), true.into()]).into(),
            BulkString::new("world".to_string()).into(),
        ]);
        assert_eq!(frame, expected_frame);
        Ok(())
    }
}

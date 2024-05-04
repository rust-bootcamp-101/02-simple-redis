use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_simple_frame_data, CRLF_LEN};

// - integer: ":[<+|->]<value>\n"
impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        // 负号不需要，会在format的时候添加
        let sign = if self < 0 { "" } else { "+" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

impl RespDecode for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;

        // split the buffer
        let data = buf.split_to(end + CRLF_LEN);
        // data[1..end]，从1开始是为了去掉符号位
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(s.parse()?)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::BufMut;

    use super::*;
    use crate::RespFrame;

    #[test]
    fn test_integer_encode() {
        let frame: RespFrame = 123.into();
        assert_eq!(frame.encode(), b":+123\r\n");

        let frame: RespFrame = (-123).into();
        assert_eq!(frame.encode(), b":-123\r\n");
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b":+123\r\n");
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, 123);

        buf.put_slice(b":-123\r\n");
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, -123);
        Ok(())
    }
}

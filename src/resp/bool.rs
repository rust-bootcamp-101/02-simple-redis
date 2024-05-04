use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::extract_fixed_data;

// - boolean: "#<t|f>\r\n"
impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

impl RespDecode for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        match extract_fixed_data(buf, "#t\r\n", "Bool") {
            Ok(_) => Ok(true),
            Err(_) => match extract_fixed_data(buf, "#f\r\n", "Bool") {
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            },
        }
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bytes::BufMut;

    use super::*;
    use crate::RespFrame;

    #[test]
    fn test_boolean_encode() -> Result<()> {
        let frame: RespFrame = false.into();
        assert_eq!(frame.encode(), b"#f\r\n");

        let frame: RespFrame = true.into();
        assert_eq!(frame.encode(), b"#t\r\n");
        Ok(())
    }

    #[test]
    fn test_boolean_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"#t\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(frame);
        buf.put_slice(b"#f\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(!frame);
        Ok(())
    }
}

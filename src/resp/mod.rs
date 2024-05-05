mod array;
mod bool;
mod bulk_string;
mod double;
mod frame;
mod integer;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;

use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    array::{RespArray, RespNullArray},
    bulk_string::{BulkString, RespNullBulkString},
    frame::RespFrame,
    map::RespMap,
    null::RespNull,
    set::RespSet,
    simple_error::SimpleError,
    simple_string::SimpleString,
};

const BUFF_CAP: usize = 4096;

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),
    #[error("Frame is not complete")]
    NotComplete,

    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Parse error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("Internal server error")]
    InternalServerError,
}

fn extract_fixed_data(
    buf: &mut BytesMut,
    expect: &str,
    expect_type: &str,
) -> Result<(), RespError> {
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }
    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: {}, got {:?}",
            expect_type, buf
        )));
    }

    buf.advance(expect.len());
    Ok(())
}

fn extract_simple_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        // 符号 + \r\n 最少得是3个字符
        return Err(RespError::NotComplete);
    }
    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: {}, got: {:?}",
            prefix, buf
        )));
    }

    // search for "\r\n"
    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;
    Ok(end)
}

/// find_crlf: nth找第几个\r\n所在的下标位置
/// 参数:
///      buf: 数据
///      nth: 要找的 \r\n 的数量
fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count: usize = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }
    None
}

// 返回 数组 [end, len]
fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}

fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            // find nth CRLF in the buffer. for array and set, we need to find 1 CRLF for each element
            for _ in 0..len {
                let length = RespFrame::expect_length(data)?;
                data = &data[length..];
                total += length;
            }
            Ok(total)
        }
        "%" => {
            // find nth CRLF in the buffer. for map, we need to find 2 CRLF for each key-value pair
            for _ in 0..len {
                // 获取一个 string key 的长度
                let length = SimpleString::expect_length(data)?;
                data = &data[length..];
                total += length;

                // 获取一个 Resp Frame value 的长度
                let length = RespFrame::expect_length(data)?;
                data = &data[length..];
                total += length;
            }
            Ok(total)
        }
        _ => Ok(len + CRLF_LEN),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_calc_array_length() -> Result<()> {
        let buf = b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let prefix = "*";
        let (end, len) = parse_length(buf, prefix)?;
        let total_len = calc_total_length(buf, end, len, prefix)?;
        assert_eq!(total_len, buf.len());

        let buf = b"*3\r\n$3\r\nset\r\n";
        let prefix = "*";
        let (end, len) = parse_length(buf, prefix)?;
        let err = calc_total_length(buf, end, len, prefix).unwrap_err();
        assert_eq!(err, RespError::NotComplete);
        Ok(())
    }
}

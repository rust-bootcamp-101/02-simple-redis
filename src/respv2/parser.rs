use std::collections::BTreeMap;

use winnow::{
    ascii::{digit1, float},
    combinator::{alt, dispatch, fail, opt, preceded, terminated},
    error::{ContextError, ErrMode},
    token::{any, take, take_until},
    PResult, Parser,
};

use crate::{
    BulkString, RespArray, RespError, RespFrame, RespMap, RespNull, RespSet, SimpleError,
    SimpleString,
};

const CRLF: &[u8] = b"\r\n";

pub fn parse_frame_length(input: &[u8]) -> Result<usize, RespError> {
    let target = &mut (&*input);
    let ret = parse_frame_len(target);
    match ret {
        Ok(_) => {
            let start = input.as_ptr() as usize;
            let end = (*target).as_ptr() as usize;
            let len = end - start;
            Ok(len)
        }
        Err(_) => Err(RespError::NotComplete),
    }
}

fn parse_frame_len(input: &mut &[u8]) -> PResult<()> {
    let mut simple_parser = terminated(take_until(0.., CRLF), CRLF).value(());
    dispatch! {any;
        b'+' => simple_parser,
        b'-' => simple_parser,
        b':' => simple_parser,
        b'$' => bulk_string_len,
        b'*' => array_len,
        b'_' => simple_parser,
        b'#' => simple_parser,
        b',' => simple_parser,
        b'%' => map_len,
        b'~' => set_len,
        _ => fail,
    }
    .parse_next(input)
}

pub fn parse_frame(input: &mut &[u8]) -> PResult<RespFrame> {
    dispatch! {any;
        b'+' => simple_string.map(RespFrame::SimpleString),
        b'-' => error.map(RespFrame::Error),
        b':' => integer.map(RespFrame::Integer),
        b'$' => bulk_string.map(RespFrame::BulkString),
        b'*' => array.map(RespFrame::Array),
        b'_' => null.map(RespFrame::Null),
        b'#' => boolean.map(RespFrame::Boolean),
        b',' => decimal.map(RespFrame::Double),
        b'%' => map.map(RespFrame::Map),
        b'~' => set.map(RespFrame::Set),
        _ => fail,
    }
    .parse_next(input)
}

// - simple string: "OK\r\n"
fn simple_string(input: &mut &[u8]) -> PResult<SimpleString> {
    parse_string.map(SimpleString).parse_next(input)
}

fn error(input: &mut &[u8]) -> PResult<SimpleError> {
    parse_string.map(SimpleError).parse_next(input)
}

fn integer(input: &mut &[u8]) -> PResult<i64> {
    let sign = opt(alt(('+', '-'))).parse_next(input)?.unwrap_or('+');
    let sign: i64 = if sign == '+' { 1 } else { -1 };
    let v: i64 = terminated(digit1.parse_to(), CRLF).parse_next(input)?;
    Ok(sign * v)
}

fn bulk_string(input: &mut &[u8]) -> PResult<BulkString> {
    let len: i64 = integer.parse_next(input)?;
    if len == 0 || len == -1 {
        return Ok(BulkString(vec![]));
    } else if len < -1 {
        return Err(err_cut("bulk string length error"));
    }

    let data = terminated(take(len as usize), CRLF)
        .map(|s: &[u8]| s.to_vec())
        .parse_next(input)?;
    Ok(BulkString(data))
}

fn array(input: &mut &[u8]) -> PResult<RespArray> {
    let len: i64 = integer.parse_next(input)?;
    if len == 0 || len == -1 {
        return Ok(RespArray(vec![]));
    } else if len < -1 {
        return Err(err_cut("array length must be non-negative"));
    }
    let mut arr = Vec::with_capacity(len as usize);
    for _ in 0..len {
        arr.push(parse_frame(input)?)
    }
    Ok(RespArray(arr))
}

fn null(input: &mut &[u8]) -> PResult<RespNull> {
    "_\r\n".value(RespNull).parse_next(input)
}

fn boolean(input: &mut &[u8]) -> PResult<bool> {
    let b = alt(('t', 'f')).parse_next(input)?;
    Ok(b == 't')
}

fn decimal(input: &mut &[u8]) -> PResult<f64> {
    terminated(float, CRLF).parse_next(input)
}

fn map(input: &mut &[u8]) -> PResult<RespMap> {
    let len: i64 = integer.parse_next(input)?;
    if len <= 0 {
        return Err(err_cut("map length must be non-negative"));
    }

    let mut map = BTreeMap::new();
    for _ in 0..len {
        let key = preceded('+', parse_string).parse_next(input)?;
        let value = parse_frame(input)?;
        map.insert(key, value);
    }
    Ok(RespMap(map))
}

fn set(input: &mut &[u8]) -> PResult<RespSet> {
    let len: i64 = integer.parse_next(input)?;
    if len <= 0 {
        return Err(err_cut("set length must be non-negative"));
    }

    let mut vec = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let value = parse_frame(input)?;
        vec.push(value);
    }
    Ok(RespSet(vec))
}

fn parse_string(input: &mut &[u8]) -> PResult<String> {
    terminated(take_until(0.., CRLF), CRLF)
        .map(|s: &[u8]| String::from_utf8_lossy(s).into_owned())
        .parse_next(input)
}

fn err_cut(_s: impl Into<String>) -> ErrMode<ContextError> {
    let context = ContextError::default();
    ErrMode::Cut(context)
}

fn array_len(input: &mut &[u8]) -> PResult<()> {
    let len: i64 = integer.parse_next(input)?;
    if len == 0 || len == -1 {
        return Ok(());
    } else if len < -1 {
        return Err(err_cut("array length must be non-negative"));
    }

    for _ in 0..len {
        parse_frame_len(input)?;
    }
    Ok(())
}

fn map_len(input: &mut &[u8]) -> PResult<()> {
    let len: i64 = integer.parse_next(input)?;
    if len <= 0 {
        return Err(err_cut("map length must be non-negative"));
    }

    for _ in 0..len {
        terminated(take_until(0.., CRLF), CRLF)
            .value(())
            .parse_next(input)?;
        parse_frame_len(input)?;
    }
    Ok(())
}

fn set_len(input: &mut &[u8]) -> PResult<()> {
    let len: i64 = integer.parse_next(input)?;
    if len <= 0 {
        return Err(err_cut("set length must be non-negative"));
    }

    for _ in 0..len {
        parse_frame_len(input)?;
    }
    Ok(())
}

fn bulk_string_len(input: &mut &[u8]) -> PResult<()> {
    let len: i64 = integer.parse_next(input)?;
    if len == 0 || len == -1 {
        return Ok(());
    } else if len < -1 {
        return Err(err_cut("bulk string length error"));
    }

    terminated(take(len as usize), CRLF)
        .value(())
        .parse_next(input)
}

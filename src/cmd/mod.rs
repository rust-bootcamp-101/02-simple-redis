mod hmap;
mod map;

use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;

use crate::{Backend, RespArray, RespError, RespFrame, SimpleString};
use thiserror::Error;

// you could also use once_cell instead of lazy_static
lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

const ONE_ARG: usize = 1;
const TWO_ARGS: usize = 2;
const THREE_ARGS: usize = 3;

const ONE_CMD: usize = 1;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("{0}")]
    RespError(#[from] RespError),

    #[error("FromUtf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(&self, backend: &Backend) -> RespFrame;
}

#[derive(Debug)]
#[enum_dispatch(CommandExecutor)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),

    // unrecognized command
    Unrecognized(Unrecognized),
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,

    // 该字段为了测试 保证key排序是一致的，实际返回并不需要排序
    sort: bool,
}

#[derive(Debug)]
pub struct Unrecognized;

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(v: RespFrame) -> Result<Self, Self::Error> {
        match v {
            RespFrame::Array(array) => array.try_into(),
            _ => Err(CommandError::InvalidCommand(
                "Command must be an array".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        match v.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                _ => Ok(Unrecognized.into()),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first argument".to_string(),
            )),
        }
    }
}

impl CommandExecutor for Unrecognized {
    fn execute(&self, _: &Backend) -> RespFrame {
        RESP_OK.clone()
    }
}

fn validate_command(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != n_args + 1 {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} argument",
            names.join(" "),
            n_args
        )));
    }
    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidArgument(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidArgument(
                    "GET command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }

    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    // let mut args = Vec::with_capacity(value.len() - start);
    // for i in start..value.len() {
    //     args.push(value[i]);
    // }
    // Ok(args)

    // 下面的更简洁

    Ok(value.0.into_iter().skip(start).collect::<Vec<_>>())
}

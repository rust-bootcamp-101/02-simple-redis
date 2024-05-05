mod echo;
mod hmap;
mod map;
mod smap;

use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;

use crate::{Backend, RespArray, RespError, RespFrame, SimpleString};
use thiserror::Error;

use self::echo::Echo;

// you could also use once_cell instead of lazy_static
lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

const ZERO_ARG: usize = 0;
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

pub trait CommandArgs {
    fn expect_args() -> NArgs; // 处理不定参数的问题
}

#[derive(Debug)]
#[enum_dispatch(CommandExecutor)]
pub enum Command {
    Echo(Echo),
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
    HMGet(HMGet),

    SAdd(SAdd),
    Sismember(Sismember),
    SMembers(SMembers),
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
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct SAdd {
    key: String,
    values: Vec<RespFrame>,
}

#[derive(Debug)]
pub struct Sismember {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct SMembers {
    key: String,
}

#[derive(Debug)]
pub struct Unrecognized;

#[derive(Debug)]
pub enum NArgs {
    Equal(usize),           // 等于多少个参数
    GreaterAndEqual(usize), // 大于多少个参数
}

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
                b"echo" => Ok(Echo::try_from(v)?.into()),
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                b"hmget" => Ok(HMGet::try_from(v)?.into()),
                b"sadd" => Ok(SAdd::try_from(v)?.into()),
                b"sismember" => Ok(Sismember::try_from(v)?.into()),
                b"smembers" => Ok(SMembers::try_from(v)?.into()),
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

impl CommandArgs for Unrecognized {
    fn expect_args() -> NArgs {
        NArgs::Equal(ZERO_ARG)
    }
}

fn validate_command<T: CommandArgs>(
    value: &RespArray,
    names: &[&'static str],
) -> Result<(), CommandError> {
    let args = T::expect_args();
    match args {
        NArgs::Equal(n_args) => {
            if value.len() != n_args + 1 {
                return Err(CommandError::InvalidArgument(format!(
                    "{} command must have exactly {} argument",
                    names.join(" "),
                    n_args
                )));
            }
        }
        NArgs::GreaterAndEqual(n_args) => {
            if value.len() < n_args + 1 {
                return Err(CommandError::InvalidArgument(format!(
                    "{} command must have exactly {} argument",
                    names.join(" "),
                    n_args
                )));
            }
        }
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

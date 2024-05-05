use crate::{Backend, RespArray, RespFrame};

use super::{
    extract_args, validate_command, CommandArgs, CommandError, CommandExecutor, NArgs, ONE_ARG,
    ONE_CMD,
};

// echo command:  https://redis.io/commands/echo/

#[derive(Debug)]
pub struct Echo {
    message: String,
}

impl CommandExecutor for Echo {
    fn execute(&self, _backend: &Backend) -> RespFrame {
        RespFrame::BulkString(self.message.clone().into())
    }
}

impl CommandArgs for Echo {
    fn expect_args() -> NArgs {
        NArgs::Equal(ONE_ARG)
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["echo"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Echo {
                message: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{Backend, BulkString, RespDecode};

    use anyhow::Result;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_echo_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*2\r\n$4\r\necho\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Echo = frame.try_into()?;
        assert_eq!(result.message, "hello");

        // 测试大写命令 ECHO
        buf.put_slice(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Echo = frame.try_into()?;
        assert_eq!(result.message, "hello");
        Ok(())
    }

    #[test]
    fn test_echo_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Echo {
            message: "hello".to_string(),
        };
        let result = cmd.execute(&backend);

        let expected_result = BulkString::new("hello").into();
        assert_eq!(result, expected_result);
        Ok(())
    }
}

use crate::{Backend, RespArray, RespFrame, RespNull};

use super::{
    extract_args, validate_command, CommandError, CommandExecutor, Get, Set, ONE_ARG, ONE_CMD,
    RESP_OK, TWO_ARGS,
};

impl CommandExecutor for Get {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let Some(value) = backend.get(&self.key) else {
            return RespFrame::Null(RespNull);
        };
        value
    }
}

impl CommandExecutor for Set {
    fn execute(&self, backend: &Backend) -> RespFrame {
        backend.set(self.key.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], ONE_ARG)?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"], TWO_ARGS)?;

        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};

    use crate::RespDecode;

    use super::*;

    #[test]
    fn test_get_from_resp_array() -> Result<()> {
        let input = RespArray::new([
            RespFrame::BulkString("get".into()),
            RespFrame::BulkString("key".into()),
        ]);
        let result: Get = input.try_into()?;
        assert_eq!(result.key, "key");

        let mut buf = BytesMut::new();
        buf.put_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Get = frame.try_into()?;
        assert_eq!(result.key, "hello");
        Ok(())
    }

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Set = frame.try_into()?;
        assert_eq!(result.key, "hello");
        assert_eq!(result.value, RespFrame::BulkString("world".into()));
        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let value = RespFrame::BulkString("world".into());
        let backend = Backend::new();
        let cmd = Set {
            key: "hello".to_string(),
            value: value.clone(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = Get {
            key: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, value);
        Ok(())
    }
}

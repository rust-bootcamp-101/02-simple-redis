use crate::{RespArray, RespFrame, RespNull};

use super::{
    extract_args, validate_command, CommandError, CommandExecutor, HGet, HGetAll, HSet, ONE_ARG,
    ONE_CMD, RESP_OK, THREE_ARGS, TWO_ARGS,
};

impl CommandExecutor for HGet {
    fn execute(&self, backend: &crate::Backend) -> RespFrame {
        let Some(value) = backend.hget(&self.key, self.field.clone()) else {
            return RespFrame::Null(RespNull);
        };
        value
    }
}

impl CommandExecutor for HSet {
    fn execute(&self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key.clone(), self.field.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(&self, backend: &crate::Backend) -> RespFrame {
        let Some(all) = backend.hgetall(&self.key) else {
            return RespArray::new([]).into();
        };
        let mut data = Vec::with_capacity(all.len());
        let mut ret = Vec::with_capacity(all.len() * 2);
        for v in all.iter() {
            let key = v.key().to_owned();
            if self.sort {
                data.push((key, v.value().clone()));
            } else {
                ret.push(RespFrame::BulkString(key.into()));
                ret.push(v.value().clone());
            }
        }
        if self.sort {
            data.sort_by(|a, b| a.0.cmp(&b.0));
            ret = data
                .into_iter()
                .flat_map(|(k, v)| [RespFrame::BulkString(k.into()), v])
                .collect::<Vec<_>>();
        }
        RespArray::new(ret).into()
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], TWO_ARGS)?;

        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], ONE_ARG)?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
                sort: false,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], THREE_ARGS)?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Backend, BulkString, RespDecode};

    use super::*;
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: HGet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");
        Ok(())
    }

    #[test]
    fn test_hgetall_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: HGetAll = frame.try_into()?;
        assert_eq!(result.key, "map");
        Ok(())
    }

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n+world\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: HSet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");
        assert_eq!(result.value, RespFrame::SimpleString("world".into()));
        Ok(())
    }

    #[test]
    fn test_hset_and_hgetall_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString("world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "foo".to_string(),
            value: RespFrame::BulkString("bar".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HGetAll {
            key: "map".to_string(),
            sort: true, // 为了保证测试通过，使用排序后的结果
        };
        let result = cmd.execute(&backend);
        let expected_result = RespArray::new([
            BulkString::new("foo").into(),
            BulkString::new("bar").into(),
            BulkString::new("hello").into(),
            BulkString::new("world").into(),
        ])
        .into();
        assert_eq!(result, expected_result);
        Ok(())
    }
}

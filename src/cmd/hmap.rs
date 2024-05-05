use crate::{Backend, RespArray, RespFrame, RespNull};

use super::{
    extract_args, validate_command, CommandArgs, CommandError, CommandExecutor, HGet, HGetAll,
    HMGet, HSet, NArgs, ONE_ARG, ONE_CMD, RESP_OK, THREE_ARGS, TWO_ARGS,
};

impl CommandExecutor for HGet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let Some(value) = backend.hget(&self.key, self.field.clone()) else {
            return RespFrame::Null(RespNull);
        };
        value
    }
}

impl CommandExecutor for HSet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        backend.hset(self.key.clone(), self.field.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(&self, backend: &Backend) -> RespFrame {
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

impl CommandExecutor for HMGet {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let mut fields = Vec::with_capacity(self.fields.len());
        // key 不存在
        let Some(all) = backend.hgetall(&self.key) else {
            for _ in 0..self.fields.len() {
                fields.push(RespFrame::Null(RespNull));
            }
            return RespArray::new(fields).into();
        };

        // key 存在
        for field in &self.fields {
            if let Some(value) = all.get(field) {
                fields.push(value.value().clone());
            } else {
                fields.push(RespFrame::Null(RespNull));
            }
        }

        RespArray::new(fields).into()
    }
}

impl CommandArgs for HGet {
    fn expect_args() -> NArgs {
        NArgs::Equal(TWO_ARGS)
    }
}

impl CommandArgs for HSet {
    fn expect_args() -> NArgs {
        NArgs::Equal(THREE_ARGS)
    }
}

impl CommandArgs for HGetAll {
    fn expect_args() -> NArgs {
        NArgs::Equal(ONE_ARG)
    }
}

impl CommandArgs for HMGet {
    fn expect_args() -> NArgs {
        NArgs::GreaterAndEqual(TWO_ARGS)
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["hget"])?;

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
        validate_command::<Self>(&value, &["hgetall"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
                sort: true, // 保证边解码的时候，排序是固定的
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["hset"])?;
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

impl TryFrom<RespArray> for HMGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["hmget"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let mut fields = Vec::new();
                while let Some(RespFrame::BulkString(field)) = args.next() {
                    fields.push(String::from_utf8(field.0)?);
                }
                Ok(HMGet {
                    key: String::from_utf8(key.0)?,
                    fields,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or fields".to_string(),
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
    fn test_hmget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*5\r\n$5\r\nHMGET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$7\r\nnofield\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: HMGet = frame.try_into()?;
        let fields = ["field1", "field2", "nofield"];
        assert_eq!(result.key, "myhash");
        assert_eq!(result.fields, fields);
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

    #[test]
    fn test_hmget_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = HSet {
            key: "myhash".to_string(),
            field: "field1".to_string(),
            value: RespFrame::BulkString("value1".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "myhash".to_string(),
            field: "field2".to_string(),
            value: RespFrame::BulkString("value2".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HMGet {
            key: "myhash".to_string(),
            fields: vec![
                "field1".to_string(),
                "field2".to_string(),
                "nofield".to_string(),
            ],
        };
        let result = cmd.execute(&backend);
        let expected_result = RespArray::new([
            BulkString::new("value1").into(),
            BulkString::new("value2").into(),
            RespNull.into(),
        ])
        .into();
        assert_eq!(result, expected_result);
        Ok(())
    }
}

use crate::{RespArray, RespEncode, RespError, RespFrame};

use super::{
    extract_args, validate_command, Backend, CommandArgs, CommandError, CommandExecutor, NArgs,
    SAdd, SMembers, Sismember, ONE_ARG, ONE_CMD, TWO_ARGS,
};

impl CommandExecutor for SAdd {
    fn execute(&self, backend: &Backend) -> RespFrame {
        match backend.sadd(self.key.clone(), self.values.clone()) {
            Ok(ret) => RespFrame::Integer(ret as i64),
            Err(e) => RespFrame::Error(e.to_string().into()),
        }
    }
}

impl CommandExecutor for Sismember {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let Some(entry) = backend.smap.get(&self.key) else {
            return RespFrame::Integer(0);
        };
        let Ok(entry) = entry.value().lock() else {
            return RespFrame::Error(RespError::InternalServerError.to_string().into());
        };
        // 直接比较编码
        let encoded = self.value.clone().encode();
        if entry.iter().any(|v| v.clone().encode() == encoded) {
            return RespFrame::Integer(1);
        }

        RespFrame::Integer(0)
    }
}

impl CommandExecutor for SMembers {
    fn execute(&self, backend: &Backend) -> RespFrame {
        let Some(entry) = backend.smap.get(&self.key) else {
            return RespArray::new([]).into();
        };
        let Ok(entry) = entry.value().lock() else {
            return RespFrame::Error(RespError::InternalServerError.to_string().into());
        };
        let members = entry.iter().cloned().collect::<Vec<_>>();
        RespArray::new(members).into()
    }
}

impl CommandArgs for SAdd {
    fn expect_args() -> NArgs {
        NArgs::GreaterAndEqual(TWO_ARGS)
    }
}

impl CommandArgs for Sismember {
    fn expect_args() -> NArgs {
        NArgs::Equal(TWO_ARGS)
    }
}

impl CommandArgs for SMembers {
    fn expect_args() -> NArgs {
        NArgs::Equal(ONE_ARG)
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["sadd"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let mut values = Vec::new();
                for v in args {
                    values.push(v);
                }
                Ok(SAdd {
                    key: String::from_utf8(key.0)?,
                    values,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or values".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Sismember {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["sismember"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Sismember {
                key: String::from_utf8(key.0)?,
                value,
            }),

            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for SMembers {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command::<Self>(&value, &["smembers"])?;
        let mut args = extract_args(value, ONE_CMD)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(SMembers {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
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
    fn test_sadd_from_resp_array() -> Result<()> {
        // 测试 sadd
        let input = RespArray::new([
            RespFrame::BulkString("sadd".into()),
            RespFrame::BulkString("myset".into()),
            RespFrame::BulkString("one".into()),
        ]);
        let result: SAdd = input.try_into()?;
        assert_eq!(result.key, "myset");
        assert_eq!(result.values, [RespFrame::BulkString("one".into())]);

        let mut buf = BytesMut::new();
        buf.put_slice(b"*4\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$3\r\none\r\n$3\r\ntwo\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: SAdd = frame.try_into()?;
        assert_eq!(result.key, "myset");
        assert_eq!(
            result.values,
            [
                RespFrame::BulkString("one".into()),
                RespFrame::BulkString("two".into())
            ]
        );

        Ok(())
    }

    #[test]
    fn test_sismember_from_resp_array() -> Result<()> {
        // 测试 sismember
        let input = RespArray::new([
            RespFrame::BulkString("sismember".into()),
            RespFrame::BulkString("myset".into()),
            RespFrame::BulkString("one".into()),
        ]);
        let result: Sismember = input.try_into()?;
        assert_eq!(result.key, "myset");
        assert_eq!(result.value, RespFrame::BulkString("one".into()));

        let mut buf = BytesMut::new();
        buf.put_slice(b"*3\r\n$9\r\nsismember\r\n$5\r\nmyset\r\n$3\r\none\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Sismember = frame.try_into()?;
        assert_eq!(result.key, "myset");
        assert_eq!(result.value, RespFrame::BulkString("one".into()));
        Ok(())
    }

    #[test]
    fn test_smembers_from_array() -> Result<()> {
        let input = RespArray::new([
            RespFrame::BulkString("smembers".into()),
            RespFrame::BulkString("myset".into()),
        ]);
        let result: SMembers = input.try_into()?;
        assert_eq!(result.key, "myset");

        let mut buf = BytesMut::new();
        buf.put_slice(b"*2\r\n$8\r\nsmembers\r\n$5\r\nmyset\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: SMembers = frame.try_into()?;
        assert_eq!(result.key, "myset");
        Ok(())
    }

    #[test]
    fn test_sadd_and_sismember_and_smembers_command() -> Result<()> {
        let key = "myset".to_string();
        // sadd 添加一个没有的元素, 返回1
        let backend = Backend::new();
        let cmd = SAdd {
            key: key.clone(),
            values: vec![RespFrame::BulkString("one".into())],
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        // sadd 添加一个已存在的元素, 返回0
        let cmd = SAdd {
            key: key.clone(),
            values: vec![RespFrame::BulkString("one".into())],
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(0));

        // sadd 添加两个已存在的元素, 返回2
        let cmd = SAdd {
            key: key.clone(),
            values: vec![
                RespFrame::BulkString("two".into()),
                RespFrame::BulkString("three".into()),
            ],
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(2));

        // sadd 添加一个已存在的元素, 一个不存在的元素, 返回1
        let cmd = SAdd {
            key: key.clone(),
            values: vec![
                RespFrame::BulkString("two".into()),
                RespFrame::SimpleString("two".into()),
            ],
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        // sismember 查询元素是否在集合中
        let cmd = Sismember {
            key: key.clone(),
            value: RespFrame::BulkString("one".into()),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let cmd = Sismember {
            key: key.clone(),
            value: RespFrame::BulkString("three".into()),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let cmd = Sismember {
            key: key.clone(),
            value: RespFrame::BulkString("two".into()),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(1));

        let cmd = Sismember {
            key: key.clone(),
            value: RespFrame::BulkString("not_in_set".into()),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Integer(0));

        // smembers 查询所有集合元素
        let cmd = SMembers { key };
        let ret = cmd.execute(&backend);
        assert_eq!(
            ret,
            RespArray::new([
                RespFrame::BulkString("one".into()),
                RespFrame::BulkString("two".into()),
                RespFrame::BulkString("three".into()),
                RespFrame::SimpleString("two".into()),
            ])
            .into()
        );
        Ok(())
    }
}

use super::{extract_args, validate_dyn_command};
use crate::{CommandError, CommandExecutor, RespArray, RespFrame};
use anyhow::Result;

#[derive(Debug)]
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

impl CommandExecutor for HMGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let values = backend.hmget(&self.key, &self.fields);

        let mut ret = Vec::with_capacity(values.len());
        for v in values {
            match v {
                Some(v) => {
                    ret.push(v);
                }
                None => {
                    ret.push(RespFrame::Null(crate::RespNull));
                }
            }
        }

        RespArray::new(ret).into()
    }
}

impl TryFrom<Vec<RespFrame>> for HMGet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_dyn_command(&value, &["hmget"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(String::from_utf8(key.0.expect("not null"))?),
            _ => Err(CommandError::InvalidArgument(
                "HMGET command must have a BulkString key argument".to_string(),
            )),
        }?;

        let mut fields = Vec::new();

        for arg in args {
            match arg {
                RespFrame::BulkString(field) => {
                    fields.push(String::from_utf8(field.0.expect("not null"))?)
                }
                _ => {
                    return Err(CommandError::InvalidArgument(
                        "HMGET command arguments must be BulkString".to_string(),
                    ))
                }
            }
        }

        Ok(Self { key, fields })
    }
}

#[cfg(test)]
mod tests {
    use crate::{cmd::hset::HSet, RespDecode};

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hmget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$5\r\nhmget\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HMGet = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.fields, vec!["hello", "world"]);

        Ok(())
    }

    #[test]
    fn test_hmget_command() -> Result<()> {
        let backend = crate::Backend::new();
        HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        }
        .execute(&backend);

        let cmd = HMGet {
            key: "map".to_string(),
            fields: vec!["hello".to_string(), "rust".to_string()],
        };
        let result = cmd.execute(&backend);
        assert_eq!(
            result,
            RespArray::new(vec![
                RespFrame::BulkString(b"world".into()),
                RespFrame::Null(crate::RespNull)
            ])
            .into()
        );

        Ok(())
    }
}

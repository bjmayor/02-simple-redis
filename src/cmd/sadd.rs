use super::{extract_args, validate_dyn_command};
use crate::{CommandError, CommandExecutor, RespFrame};

#[derive(Debug)]
pub struct SAdd {
    pub(crate) key: String,
    pub(crate) members: Vec<String>,
}

impl CommandExecutor for SAdd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let count = backend.sadd(self.key, self.members);
        RespFrame::Integer(count as i64)
    }
}

impl TryFrom<Vec<RespFrame>> for SAdd {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_dyn_command(&value, &["sadd"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(String::from_utf8(key.0.expect("not null"))?),
            _ => Err(CommandError::InvalidArgument(
                "SADD command must have a BulkString key argument".to_string(),
            )),
        }?;

        let mut members = Vec::new();

        for arg in args {
            match arg {
                RespFrame::BulkString(member) => {
                    members.push(String::from_utf8(member.0.expect("not null"))?)
                }
                _ => {
                    return Err(CommandError::InvalidArgument(
                        "SADD command arguments must be BulkString".to_string(),
                    ))
                }
            }
        }

        Ok(Self { key, members })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, RespArray, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_sadd_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: SAdd = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "hello");
        assert_eq!(result.members, vec!["world"]);

        Ok(())
    }

    #[test]
    fn test_sadd_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = SAdd {
            key: "hello".to_string(),
            members: vec![
                "world".to_string(),
                "world2".to_string(),
                "world".to_string(),
            ],
        };

        let resp = cmd.execute(&backend);
        assert_eq!(resp, RespFrame::Integer(2));

        Ok(())
    }
}

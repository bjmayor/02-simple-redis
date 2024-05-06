use super::{extract_args, validate_command};
use crate::{CommandError, CommandExecutor, RespFrame};

#[derive(Debug)]
pub struct SIsMember {
    pub(crate) key: String,
    pub(crate) member: String,
}

impl CommandExecutor for SIsMember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let exists = backend.sismember(&self.key, &self.member);
        RespFrame::Integer(exists as i64)
    }
}

impl TryFrom<Vec<RespFrame>> for SIsMember {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_command(&value, &["sismember"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(String::from_utf8(key.0.expect("not null"))?),
            _ => Err(CommandError::InvalidArgument(
                "SISMEMBER command must have a BulkString key argument".to_string(),
            )),
        }?;

        let member = match args.next() {
            Some(RespFrame::BulkString(member)) => {
                Ok(String::from_utf8(member.0.expect("not null"))?)
            }
            _ => Err(CommandError::InvalidArgument(
                "SISMEMBER command must have a BulkString member argument".to_string(),
            )),
        }?;

        Ok(Self { key, member })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cmd::sadd::SAdd, Backend, RespArray, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_sismember_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$9\r\nsismember\r\n$5\r\nmyset\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: SIsMember = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "myset");
        assert_eq!(result.member, "world");

        Ok(())
    }

    #[test]
    fn test_sismember_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = SIsMember {
            key: "myset".to_string(),
            member: "world".to_string(),
        };
        let result = cmd.execute(&backend);

        assert_eq!(result, RespFrame::Integer(0));

        SAdd {
            key: "myset".to_string(),
            members: vec!["world".to_string()],
        }
        .execute(&backend);
        let cmd = SIsMember {
            key: "myset".to_string(),
            member: "world".to_string(),
        };
        let result = cmd.execute(&backend);

        assert_eq!(result, RespFrame::Integer(1));

        Ok(())
    }
}

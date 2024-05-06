use crate::{CommandError, CommandExecutor, RespFrame};

use super::{extract_args, validate_command, RESP_OK};

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Set {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.set(self.key.clone(), self.value.clone());
        RESP_OK.clone()
    }
}

impl TryFrom<Vec<RespFrame>> for Set {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Self {
                key: String::from_utf8(key.0.expect("not null"))?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "SET command must have two BulkString arguments".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cmd::get::Get, Backend, RespArray, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Set = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "hello");
        assert_eq!(result.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = Get {
            key: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        Ok(())
    }
}

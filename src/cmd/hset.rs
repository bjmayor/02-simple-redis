use crate::{CommandError, CommandExecutor, RespFrame};

use super::{extract_args, validate_command, RESP_OK};

#[derive(Debug)]
pub struct HSet {
    pub(crate) key: String,
    pub(crate) field: String,
    pub(crate) value: RespFrame,
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<Vec<RespFrame>> for HSet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;
        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(Self {
                    key: String::from_utf8(key.0.expect("not null"))?,
                    field: String::from_utf8(field.0.expect("not null"))?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "HSET command must have three BulkString arguments".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{RespArray, RespDecode, RespFrame};

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HSet = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");
        assert_eq!(result.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }
}

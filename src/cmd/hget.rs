use crate::{CommandError, CommandExecutor, RespFrame};

use super::{extract_args, validate_command};

#[derive(Debug)]
pub struct HGet {
    pub(crate) key: String,
    pub(crate) field: String,
}

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

impl TryFrom<Vec<RespFrame>> for HGet {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();

        // test if the first element is a bulk string
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(Self {
                key: String::from_utf8(key.0.expect("not null"))?,
                field: String::from_utf8(field.0.expect("not null"))?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "HGET command must have two BulkString arguments".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{RespArray, RespDecode};

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HGet = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");

        Ok(())
    }
}

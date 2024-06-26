use crate::{CommandError, CommandExecutor, RespFrame};

use super::{extract_args, validate_command};

#[derive(Debug)]
pub struct Get {
    pub(crate) key: String,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

impl TryFrom<Vec<RespFrame>> for Get {
    type Error = CommandError;

    fn try_from(value: Vec<RespFrame>) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();

        // test if the first element is a bulk string
        match args.next() {
            Some(RespFrame::BulkString(key)) => match key.0 {
                Some(k) => Ok(Self {
                    key: String::from_utf8(k)?,
                }),
                None => Err(CommandError::InvalidArgument(
                    "GET command must have a BulkString argument".to_string(),
                )),
            },
            _ => Err(CommandError::InvalidArgument(
                "GET command must have a BulkString argument".to_string(),
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
    fn test_get_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Get = frame.0.unwrap().try_into()?;
        assert_eq!(result.key, "hello");

        Ok(())
    }
}

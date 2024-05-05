use crate::{BulkString, CommandError, CommandExecutor, RespArray, RespFrame};

use super::{extract_args, validate_command};

#[derive(Debug)]
pub struct Echo {
    value: BulkString,
}

impl CommandExecutor for Echo {
    fn execute(self, _backend: &crate::Backend) -> RespFrame {
        RespFrame::BulkString(self.value.clone())
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["echo"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(value)) => Ok(Self {
                value: value.clone(),
            }),
            _ => Err(CommandError::InvalidArgument(
                "ECHO command must have a BulkString argument".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RespFrame;

    #[test]
    fn test_echo_execute() {
        let echo = Echo {
            value: "hello".into(),
        };
        let frame = echo.execute(&crate::Backend::default());
        assert_eq!(frame, RespFrame::BulkString(b"hello".into()));
    }
}

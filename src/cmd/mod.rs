use crate::{Backend, RespArray, RespFrame};
mod echo;
mod get;
mod hget;
mod hget_all;
mod hmget;
mod hset;
mod sadd;
mod sismember;

mod set;
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;

lazy_static! {
    static ref RESP_OK: RespFrame = "OK".into();
}
use thiserror::Error;

use self::{
    echo::Echo, get::Get, hget::HGet, hget_all::HGetAll, hmget::HMGet, hset::HSet, sadd::SAdd,
    set::Set, sismember::SIsMember,
};
#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command {0}")]
    InvalidCommand(String),

    #[error("Invalid argument {0}")]
    InvalidArgument(String),
    #[error("RespError: {0}")]
    RespError(#[from] crate::RespError),
    #[error("Utf8Error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
    Echo(Echo),
    HMGet(HMGet),
    SAdd(SAdd),
    SIsMember(SIsMember),
    // unrecognized command
    Unrecognized(Unrecognized),
}

#[derive(Debug)]
pub struct Unrecognized;

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(value: RespFrame) -> Result<Self, Self::Error> {
        match value {
            RespFrame::Array(array) => array.try_into(),
            _ => Err(CommandError::InvalidCommand(
                "Command must be an array".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        match v.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                b"echo" => Ok(Echo::try_from(v)?.into()),
                b"hmget" => Ok(HMGet::try_from(v)?.into()),
                b"sadd" => Ok(SAdd::try_from(v)?.into()),
                b"sismember" => Ok(SIsMember::try_from(v)?.into()),
                _ => Ok(Unrecognized.into()),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first argument".to_string(),
            )),
        }
    }
}
impl CommandExecutor for Unrecognized {
    fn execute(self, _: &Backend) -> RespFrame {
        RESP_OK.clone()
    }
}

fn validate_command(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != n_args + names.len() {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} argument",
            names.join(" "),
            n_args
        )));
    }

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn validate_dyn_command(
    value: &RespArray,
    names: &[&'static str],
    at_least_n_args: usize,
) -> Result<(), CommandError> {
    if value.len() < at_least_n_args + names.len() {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have at least {} argument",
            names.join(" "),
            at_least_n_args
        )));
    }

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}

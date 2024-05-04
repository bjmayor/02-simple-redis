use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;
use std::{
    collections::BTreeMap,
    fmt::Display,
    ops::{Deref, DerefMut},
};
use thiserror::Error;

mod decode;
mod encode;

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length:{0}")]
    InvalidFrameLength(isize),
    #[error("Frame is not complete ")]
    NotComplete,

    #[error("Invalid integer: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Invalid double: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("Invalid string: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}
// SimpleString +OK\r\n
// SimpleError -Error\r\n
// Integers :[<+|->]<value>\r\n :+1\r\n :-1\r\n
// BulkString $<length>\r\n<bytes>\r\n $6\r\nfoobar\r\n
// Null bulk strings $-1\r\n
// Array *<number-of-elements>\r\n<elements>\r\n *2\r\n:1\r\n:2\r\n
// Nulls _\r\n
// Booleans #t\r\n #f\r\n
// Doubles ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n ,+1.0\r\n ,-1.0\r\n
// Big numbers ([+|-]<number>\r\n  (3492890328409238509324850943850943825024385\r\n
// Bulk errors !<length>\r\n<error>\r\n !6\r\nfoobar\r\n
// Verbatim strings =<length>\r\n<encoding>:<data>\r\n =15\r\ntxt:Some string\r\n
// Maps %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n> %2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
// Sets ~<number-of-elements>\r\n<element-1>...<element-n>
#[enum_dispatch(RespEncode)]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    NullBulkString(RespNullBulkString),
    Null(RespNull),
    NullArray(RespNullArray),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleString(pub(crate) String);
impl Deref for SimpleString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleError(pub(crate) String);
impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}
impl Deref for SimpleError {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNull;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNullArray;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Vec<RespFrame>);
impl Deref for RespArray {
    type Target = Vec<RespFrame>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNullBulkString;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);
impl Deref for BulkString {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Display for BulkString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespMap(pub(crate) BTreeMap<String, RespFrame>);
impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespSet(pub(crate) Vec<RespFrame>);
impl Deref for RespSet {
    type Target = Vec<RespFrame>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl RespMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}

impl RespSet {
    pub fn new() -> Self {
        Self(Vec::new())
    }
}

impl Default for RespSet {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&str> for RespFrame {
    fn from(s: &str) -> Self {
        Self::SimpleString(SimpleString(s.to_string()))
    }
}

impl From<String> for SimpleString {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl From<String> for BulkString {
    fn from(s: String) -> Self {
        BulkString(s.into_bytes())
    }
}

impl From<&str> for SimpleError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for SimpleError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl From<&[u8]> for RespFrame {
    fn from(s: &[u8]) -> Self {
        BulkString(s.to_vec()).into()
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec()).into()
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(s.into())
    }
}

// utility functions
// extract_fixed_data extracts the fixed data from the buffer
// ex: null, nullbulkstring, bool
fn extract_fixed_data(
    buf: &mut BytesMut,
    expect: &str,
    expect_type: &str,
) -> Result<(), RespError> {
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: {}, got: {:?}",
            expect_type, buf
        )));
    }

    buf.advance(expect.len());
    Ok(())
}

// used to extract the length of the frame
// ex: integer, double,  simplestring, simpleerror
fn extract_simple_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: SimpleString({}), got: {:?}",
            prefix, buf
        )));
    }

    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;

    Ok(end)
}

// find nth CRLF in the buffer
fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }

    None
}

// extract the length of the frame
// ex:  array, map, set
fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}

fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            // find nth CRLF in the buffer, for array and set, we need to find 1 CRLF for each element
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        "%" => {
            // find nth CRLF in the buffer. For map, we need to find 2 CRLF for each key-value pair
            for _ in 0..len {
                let len = SimpleString::expect_length(data)?;

                data = &data[len..];
                total += len;

                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        _ => Ok(len + CRLF_LEN),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_calc_array_length() -> Result<()> {
        let buf = b"*2\r\n$3\r\nset\r\n$5\r\nhello\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let total_len = calc_total_length(buf, end, len, "*")?;
        assert_eq!(total_len, buf.len());

        let buf = b"*2\r\n$3\r\nset\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let ret = calc_total_length(buf, end, len, "*");
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        Ok(())
    }
}

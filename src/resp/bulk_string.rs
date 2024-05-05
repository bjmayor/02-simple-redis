use std::{fmt::Display, ops::Deref};

use bytes::{Buf, BytesMut};

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_fixed_data, parse_length, CRLF_LEN};

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNullBulkString;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16);
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self.0);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

impl RespEncode for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        buf.advance(end + CRLF_LEN); // skip the *...
                                     // consume the bulk string data
        let bs = BulkString::new(&buf[..len]);
        buf.advance(len + CRLF_LEN); // skip the length and \r\n
        Ok(bs)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let exptect_length = end + CRLF_LEN + len + CRLF_LEN;
        if buf.len() < exptect_length {
            return Err(RespError::NotComplete);
        }
        Ok(exptect_length)
    }
}

impl RespDecode for RespNullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "$-1\r\n", "NullBulkString")?;
        Ok(RespNullBulkString)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(5)
    }
}

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

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        Self(s.into())
    }
}

impl From<String> for BulkString {
    fn from(s: String) -> Self {
        BulkString(s.into_bytes())
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec())
    }
}
impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_bulk_string_encode() {
        let s = BulkString::new("Hello".to_string());
        assert_eq!(s.encode(), b"$5\r\nHello\r\n");
    }

    #[test]
    fn test_resp_null_bulk_string_encode() {
        let s = RespNullBulkString;
        assert_eq!(s.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_null_bulk_string_decode() {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = RespNullBulkString;
        assert_eq!(RespNullBulkString::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_bulk_string_decode() {
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let frame = BulkString::new("foobar".to_string());
        assert_eq!(BulkString::decode(&mut buf).unwrap(), frame);
        let mut buf = BytesMut::from("$2\r\nOk\r\n");
        let frame = BulkString::new("Ok".to_string());
        assert_eq!(BulkString::decode(&mut buf).unwrap(), frame);
    }
}

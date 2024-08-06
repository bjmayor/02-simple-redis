use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_simple_frame_data, frame::RespFrame, CRLF_LEN};

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleString(pub(crate) String);

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        Ok(SimpleString::new(String::from_utf8_lossy(&data[1..end])))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

impl Deref for SimpleString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl From<&str> for RespFrame {
    fn from(s: &str) -> Self {
        Self::SimpleString(SimpleString(s.to_string()))
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for SimpleString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for SimpleString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {

    use bytes::BufMut;

    use super::*;
    #[test]
    fn test_simple_string_encode() {
        let s = SimpleString::new("Ok".to_string());
        assert_eq!(s.encode(), b"+Ok\r\n");
    }

    #[test]
    fn test_simple_string_decode() {
        let mut buf = BytesMut::from("+Ok\r\n");
        let frame = SimpleString::new("Ok".to_string());
        assert_eq!(SimpleString::decode(&mut buf).unwrap(), frame);

        buf.extend_from_slice(b"+hello\r");
        let ret = SimpleString::decode(&mut buf);
        assert_eq!(ret, Err(RespError::NotComplete));

        buf.put_u8(b'\n');
        let ret = SimpleString::decode(&mut buf).unwrap();
        assert_eq!(ret, SimpleString::new("hello".to_string()))
    }
}

use std::ops::Deref;

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_simple_frame_data, CRLF_LEN};

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleError(pub(crate) String);

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}
impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        Ok(SimpleError::new(String::from_utf8_lossy(&data[1..end])))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

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

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_simple_error_encode() {
        let s = SimpleError::new("Error".to_string());
        assert_eq!(s.encode(), b"-Error\r\n");
    }
    #[test]
    fn test_simple_error_decode() {
        let mut buf = BytesMut::from("-Error\r\n");
        let frame = SimpleError::new("Error".to_string());
        assert_eq!(SimpleError::decode(&mut buf).unwrap(), frame);
    }
}

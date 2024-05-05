use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{calc_total_length, frame::RespFrame, parse_length, BUF_CAP};

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespSet(pub(crate) Vec<RespFrame>);

// set: ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

// Sets ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[1..end]);
        let len = s.parse()?;
        let mut set = RespSet::new();
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            set.push(frame);
        }
        Ok(set)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

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

#[cfg(test)]
mod tests {

    use crate::SimpleString;

    use super::*;

    #[test]
    fn test_set_encode() {
        let mut s = RespSet::new();
        s.push(RespFrame::Integer(1));
        assert_eq!(s.encode(), b"~1\r\n:+1\r\n");
    }

    #[test]
    fn test_set_decode() {
        let mut buf = BytesMut::from("~4\r\n+first\r\n:+1\r\n+second\r\n:+2\r\n");
        let mut set = RespSet::new();
        set.push(SimpleString::new("first".to_string()).into());
        set.push(1.into());
        set.push(SimpleString::new("second".to_string()).into());
        set.push(2.into());
        let frame = set;
        assert_eq!(RespSet::decode(&mut buf).unwrap(), frame);
    }
}

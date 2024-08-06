use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{RespDecode, RespEncode, RespError};

use super::{calc_total_length, frame::RespFrame, parse_length, BUF_CAP, CRLF_LEN, NULL_ARRAY};
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Option<Vec<RespFrame>>);

// array: *<number-of-elements>\r\n<element-1>...<element-n>

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        match self.0 {
            Some(v) => {
                let mut buf = Vec::with_capacity(BUF_CAP);
                buf.extend_from_slice(&format!("*{}\r\n", v.len()).into_bytes());
                for frame in v {
                    buf.extend_from_slice(&frame.encode());
                }
                buf
            }
            None => NULL_ARRAY.clone(),
        }
    }
}

impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if buf.starts_with(&NULL_ARRAY[..3]) {
            if !buf.starts_with(&NULL_ARRAY) {
                return Err(RespError::NotComplete);
            }
            buf.advance(NULL_ARRAY.len());
            return Ok(RespArray(None));
        }
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN); // skip the *...
        let mut array = Vec::with_capacity(len);
        for _ in 0..len {
            let frame = RespFrame::decode(buf)?;
            array.push(frame);
        }
        Ok(RespArray::new(array))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        if buf.starts_with(b"*-1") {
            if buf.len() < 5 {
                return Err(RespError::NotComplete);
            }
            return Ok(5);
        }
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

impl Deref for RespArray {
    type Target = Option<Vec<RespFrame>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(Some(s.into()))
    }
}

impl From<Vec<RespFrame>> for RespArray {
    fn from(s: Vec<RespFrame>) -> Self {
        RespArray(Some(s))
    }
}

#[cfg(test)]
mod tests {

    use crate::{resp::bulk_string::BulkString, SimpleString};

    use super::*;

    #[test]
    fn test_resp_array_encode() {
        let s = RespArray::new(vec![
            BulkString::new("Ok".to_string()).into(),
            SimpleString::new("Ok".to_string()).into(),
            RespFrame::Integer(1),
        ]);
        assert_eq!(s.encode(), b"*3\r\n$2\r\nOk\r\n+Ok\r\n:1\r\n");
    }

    #[test]
    fn test_resp_null_array_encode() {
        let s = RespArray(None);
        assert_eq!(s.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_array_decode() {
        let mut buf = BytesMut::from("*3\r\n$2\r\nOk\r\n+Ok\r\n:+1\r\n");
        let frame = RespArray::new(vec![b"Ok".into(), "Ok".into(), RespFrame::Integer(1)]);
        assert_eq!(RespArray::decode(&mut buf).unwrap(), frame);

        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r");
        let ret = RespArray::decode(&mut buf);
        assert_eq!(ret, Err(RespError::NotComplete));
        buf.extend_from_slice(b"\n");
        let frame = RespArray::new(vec![b"get".into(), b"hello".into()]);
        assert_eq!(RespArray::decode(&mut buf).unwrap(), frame);
    }
}

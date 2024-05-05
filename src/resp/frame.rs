use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;

use crate::{RespDecode, RespEncode, RespError, SimpleString};

use super::{
    array::{RespArray, RespNullArray},
    bulk_string::{BulkString, RespNullBulkString},
    extract_fixed_data,
    map::RespMap,
    set::RespSet,
    simple_error::SimpleError,
};

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

// SimpleString +OK\r\n
// SimpleError -Error\r\n
// Integers :[<+|->]<value>\r\n :+1\r\n :-1\r\n
// BulkString $<length>\r\n<bytes>\r\n $6\r\nfoobar\r\n
// Array *<number-of-elements>\r\n<elements>\r\n *2\r\n:1\r\n:2\r\n
// Nulls _\r\n
// Booleans #t\r\n #f\r\n
// Doubles ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n ,+1.0\r\n ,-1.0\r\n
// Big numbers ([+|-]<number>\r\n  (3492890328409238509324850943850943825024385\r\n
// Bulk errors !<length>\r\n<error>\r\n !6\r\nfoobar\r\n
// Verbatim strings =<length>\r\n<encoding>:<data>\r\n =15\r\ntxt:Some string\r\n
// Maps %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n> %2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
// Sets ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'-') => {
                let frame = SimpleError::decode(buf)?;
                Ok(frame.into())
            }
            Some(b':') => {
                let frame = i64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'$') => {
                // try null bulk string first
                if buf.len() >= 5 && buf.starts_with(b"$-1\r\n") {
                    buf.advance(5);
                    return Ok(RespNullBulkString.into());
                }
                let frame = BulkString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'*') => {
                // try null array first
                if buf.len() >= 4 && buf.starts_with(b"*-1\r\n") {
                    buf.advance(5);
                    return Ok(RespNull.into());
                }
                let frame = RespArray::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'_') => {
                let frame = RespNullBulkString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'#') => {
                let frame = bool::decode(buf)?;
                Ok(frame.into())
            }
            Some(b',') => {
                let frame = f64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'%') => {
                // try null first
                if buf.len() >= 4 && buf.starts_with(b"%-1\r\n") {
                    buf.advance(5);
                    return Ok(RespNull.into());
                }
                let frame = RespMap::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'~') => {
                let frame = RespSet::decode(buf)?;
                Ok(frame.into())
            }
            None => Err(RespError::NotComplete),
            _ => Err(RespError::InvalidFrameType(format!(
                "expect: frame, got: {:?}",
                buf
            ))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'_') => RespNullBulkString::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b',') => f64::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            _ => Err(RespError::NotComplete),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNull;
impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespDecode for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "_\r\n", "Null")?;
        Ok(RespNull)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(3)
    }
}

impl From<&[u8]> for RespFrame {
    fn from(s: &[u8]) -> Self {
        BulkString(s.to_vec()).into()
    }
}

impl<const N: usize> From<&[u8; N]> for RespFrame {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec()).into()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_resp_null_encode() {
        let s = RespNull;
        assert_eq!(s.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_decode() {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull;
        assert_eq!(RespNull::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_resp_frame_decode() {
        let mut buf = BytesMut::from("+Ok\r\n");
        let frame = SimpleString::new("Ok".to_string()).into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("-Error\r\n");
        let frame = SimpleError::new("Error".to_string()).into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from(":1\r\n");
        let frame = 1.into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("$2\r\nOk\r\n");
        let frame = BulkString::new("Ok".to_string()).into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("*3\r\n$2\r\nOk\r\n+Ok\r\n:+1\r\n");
        let frame = RespArray::new(vec![
            BulkString::new("Ok".to_string()).into(),
            SimpleString::new("Ok".to_string()).into(),
            RespFrame::Integer(1),
        ])
        .into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("$-1\r\n");
        let frame = RespNullBulkString.into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("#t\r\n");
        let frame = true.into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from(",1.0\r\n");
        let frame = 1.0.into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);

        let mut buf = BytesMut::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        let mut map = RespMap::new();
        map.insert("first".to_string(), 1.into());
        map.insert("second".to_string(), 2.into());
        let frame = map.into();
        assert_eq!(RespFrame::decode(&mut buf).unwrap(), frame);
    }
}

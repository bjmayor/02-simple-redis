use crate::{
    BulkString, RespArray, RespDecode, RespError, RespFrame, RespMap, RespNull, RespNullBulkString,
    RespSet, SimpleError, SimpleString,
};
use bytes::{Buf, BytesMut};

use super::{
    calc_total_length, extract_fixed_data, extract_simple_frame_data, parse_length, CRLF_LEN,
};
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

impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        println!("buf: {:?}", buf);
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        Ok(SimpleString::new(String::from_utf8_lossy(&data[1..end])))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
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

impl RespDecode for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let num = String::from_utf8_lossy(&data[1..end]);
        let num = num.parse()?;
        Ok(num)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
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
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

impl RespDecode for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let b = data[1];
        let b = match b {
            b'f' => false,
            b't' => true,
            _ => {
                return Err(RespError::InvalidFrameType(format!(
                    "expected Boolean(_), got {:?}",
                    data
                )))
            }
        };
        Ok(b)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
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
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
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

impl RespDecode for f64 {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + 2);
        let num = String::from_utf8_lossy(&data[1..end]);
        let num = num.parse()?;
        Ok(num)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// Maps %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n> %2\r\n+first\r\n:1\r\n+second\r\n:2\r\n
impl RespDecode for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        let data = buf.split_to(end + 2);
        let s = String::from_utf8_lossy(&data[1..end]);
        let len = s.parse()?;
        let mut map = RespMap::new();
        for _ in 0..len {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            map.insert(key.0, value);
        }
        Ok(map)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
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
#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;
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

    #[test]
    fn test_simple_error_decode() {
        let mut buf = BytesMut::from("-Error\r\n");
        let frame = SimpleError::new("Error".to_string());
        assert_eq!(SimpleError::decode(&mut buf).unwrap(), frame);
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

    #[test]
    fn test_integer_decode() {
        let mut buf = BytesMut::from(":+1\r\n");
        let frame = 1.into();
        assert_eq!(i64::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_bool_decode() {
        let mut buf = BytesMut::from("#t\r\n");
        let frame = true;
        assert_eq!(bool::decode(&mut buf).unwrap(), frame);
        buf.extend_from_slice(b"#f\r\n");
        let frame: bool = false;
        assert_eq!(bool::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_array_decode() {
        let mut buf = BytesMut::from("*3\r\n$2\r\nOk\r\n+Ok\r\n:+1\r\n");
        let frame = RespArray::new(vec![b"Ok".into(), "Ok".into(), RespFrame::Integer(1)]);
        assert_eq!(RespArray::decode(&mut buf).unwrap(), frame);

        // buf.extend_from_slice(b"*2\r\n$2\r\nOk\r\n+Ok\r");
        // let ret = RespArray::decode(&mut buf);
        // assert_eq!(ret, Err(RespError::NotComplete));
        // buf.extend_from_slice(b"\n");
        // let frame = RespArray::new(vec![b"Ok".into(), "Ok".into()]).into();
        // assert_eq!(RespArray::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_double_decode() {
        let mut buf = BytesMut::from(",1.0\r\n");
        let frame = 1.0;
        assert_eq!(f64::decode(&mut buf).unwrap(), frame);

        buf.extend_from_slice(b",+1.23456e-9\r\n");
        let frame = 1.23456e-9;
        assert_eq!(f64::decode(&mut buf).unwrap(), frame);
    }

    #[test]
    fn test_map_decode() {
        let mut buf = BytesMut::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        let mut map = RespMap::new();
        map.insert("first".to_string(), 1.into());
        map.insert("second".to_string(), 2.into());
        let frame = map;
        assert_eq!(RespMap::decode(&mut buf).unwrap(), frame);

        buf.extend_from_slice(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r");
        let ret = RespMap::decode(&mut buf);
        assert_eq!(ret, Err(RespError::NotComplete));
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

    #[test]
    fn test_null_bulk_string_decode() {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = RespNullBulkString;
        assert_eq!(RespNullBulkString::decode(&mut buf).unwrap(), frame);
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

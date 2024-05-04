use crate::{
    BulkString, RespArray, RespEncode, RespMap, RespNull, RespNullArray, RespNullBulkString,
    RespSet, SimpleError, SimpleString,
};

const BUF_CAP: usize = 4096;

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        let sign = if self < 0 { "" } else { "+" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

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

// array: *<number-of-elements>\r\n<element-1>...<element-n>

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("*{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}
impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// #<t|f>\r\n
impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        if self {
            b"#t\r\n".to_vec()
        } else {
            b"#f\r\n".to_vec()
        }
    }
}

// double: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        let ret = if self.abs() > 1e+8 || self.abs() < 1e-8 {
            format!(",{:+e}\r\n", self)
        } else {
            format!(",{:+}\r\n", self)
        };

        buf.extend_from_slice(&ret.into_bytes());
        buf
    }
}

// map: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("%{}\r\n", self.len()).into_bytes());
        for (k, v) in self.0 {
            buf.extend_from_slice(&SimpleString::new(k).encode());
            buf.extend_from_slice(&v.encode());
        }
        buf
    }
}

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

#[cfg(test)]
mod tests {

    use crate::RespFrame;

    use super::*;
    #[test]
    fn test_simple_string_encode() {
        let s = SimpleString::new("Ok".to_string());
        assert_eq!(s.encode(), b"+Ok\r\n");
    }

    #[test]
    fn test_simple_error_encode() {
        let s = SimpleError::new("Error".to_string());
        assert_eq!(s.encode(), b"-Error\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let frame: RespFrame = 1.into();
        assert_eq!(frame.encode(), b":+1\r\n");
        let frame: RespFrame = (-1).into();
        assert_eq!(frame.encode(), b":-1\r\n");
    }

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
    fn test_resp_array_encode() {
        let s = RespArray::new(vec![
            BulkString::new("Ok".to_string()).into(),
            SimpleString::new("Ok".to_string()).into(),
            RespFrame::Integer(1),
        ]);
        assert_eq!(s.encode(), b"*3\r\n$2\r\nOk\r\n+Ok\r\n:+1\r\n");
    }

    #[test]
    fn test_resp_null_encode() {
        let s = RespNull;
        assert_eq!(s.encode(), b"_\r\n");
    }

    #[test]
    fn test_resp_null_array_encode() {
        let s = RespNullArray;
        assert_eq!(s.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_bool_encode() {
        let s = true;
        assert_eq!(s.encode(), b"#t\r\n");
        let s = false;
        assert_eq!(s.encode(), b"#f\r\n");
    }

    #[test]
    fn test_double_encode() {
        let frame: RespFrame = 123.456.into();
        assert_eq!(frame.encode(), b",+123.456\r\n");
        let frame: RespFrame = (-123.456).into();
        assert_eq!(frame.encode(), b",-123.456\r\n");

        let frame: RespFrame = 1.23456e+10.into();
        assert_eq!(frame.encode(), b",+1.23456e10\r\n");

        let frame: RespFrame = 1.23456e-10.into();
        assert_eq!(frame.encode(), b",+1.23456e-10\r\n");

        let frame: RespFrame = (-1.23456e-10).into();
        assert_eq!(frame.encode(), b",-1.23456e-10\r\n");

        let frame: RespFrame = (-1.23456e+10).into();
        assert_eq!(frame.encode(), b",-1.23456e10\r\n");
    }

    #[test]
    fn test_map_encode() {
        let mut m = RespMap::new();
        m.insert("key".to_string(), RespFrame::Integer(1));

        assert_eq!(m.encode(), b"%1\r\n+key\r\n:+1\r\n");
    }

    #[test]
    fn test_set_encode() {
        let mut s = RespSet::new();
        s.push(RespFrame::Integer(1));
        assert_eq!(s.encode(), b"~1\r\n:+1\r\n");
    }
}

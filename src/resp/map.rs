use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError, SimpleString};

use super::{calc_total_length, frame::RespFrame, parse_length, BUF_CAP};

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespMap(pub(crate) BTreeMap<String, RespFrame>);

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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_map_encode() {
        let mut m = RespMap::new();
        m.insert("key".to_string(), RespFrame::Integer(1));

        assert_eq!(m.encode(), b"%1\r\n+key\r\n:1\r\n");
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
}

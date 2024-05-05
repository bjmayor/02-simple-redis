use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_simple_frame_data, CRLF_LEN};

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{}\r\n", self).into_bytes()
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

#[cfg(test)]
mod tests {

    use crate::resp::frame::RespFrame;

    use super::*;

    #[test]
    fn test_integer_encode() {
        let frame: RespFrame = 1.into();
        assert_eq!(frame.encode(), b":1\r\n");
        let frame: RespFrame = (-1).into();
        assert_eq!(frame.encode(), b":-1\r\n");
    }

    #[test]
    fn test_integer_decode() {
        let mut buf = BytesMut::from(":1\r\n");
        let frame = 1.into();
        assert_eq!(i64::decode(&mut buf).unwrap(), frame);
    }
}

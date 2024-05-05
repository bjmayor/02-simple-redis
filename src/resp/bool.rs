use bytes::BytesMut;

use crate::{RespDecode, RespEncode, RespError};

use super::extract_simple_frame_data;

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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_bool_encode() {
        let s = true;
        assert_eq!(s.encode(), b"#t\r\n");
        let s = false;
        assert_eq!(s.encode(), b"#f\r\n");
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
}

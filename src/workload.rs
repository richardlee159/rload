use byteorder::{BigEndian, WriteBytesExt};
use std::io::Cursor;

pub fn matmul(mat_size: u64) -> Vec<u8> {
    let mut body = Vec::new();
    let mut writer = Cursor::new(&mut body);
    writer.write_u64::<BigEndian>(mat_size).unwrap();
    writer.write_u64::<BigEndian>(mat_size).unwrap();
    body
}

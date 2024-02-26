use byteorder::{BigEndian, WriteBytesExt};
use std::io::Cursor;

pub fn matmul(input_size: u64) -> Vec<u8> {
    let mut body = Vec::new();
    let mut writer = Cursor::new(&mut body);
    writer.write_u64::<BigEndian>(input_size).unwrap();
    writer.write_u64::<BigEndian>(input_size).unwrap();
    body
}

pub fn matmul_checksum(input_size: u64) -> u64 {
    let in_mat: Vec<_> = (1..input_size * input_size + 1).collect();
    let input_size = input_size as usize;
    let mut out_mat = vec![0u64; input_size * input_size];
    for i in 0..input_size {
        for j in 0..input_size {
            for k in 0..input_size {
                out_mat[i * input_size + j] +=
                    in_mat[i * input_size + k] * in_mat[j * input_size + k]
            }
        }
    }
    out_mat[out_mat.len() - 1]
}

use crate::RequestType;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Cursor;

// Consider implementing this using dynamic dispatch
impl RequestType {
    pub fn url(&self, ip: &str, is_hot: bool) -> String {
        format!(
            "http://{}:{}/{}/{}",
            ip,
            8080,
            if is_hot { "hot" } else { "cold" },
            match self {
                Self::Matmul => "matmul",
                Self::Compute => "compute",
                Self::Io => "io",
            }
        )
    }

    pub fn body(&self, input_size: u64, storage_ip: &str) -> Vec<u8> {
        match self {
            Self::Matmul => {
                let mut body = Vec::new();
                let mut writer = Cursor::new(&mut body);
                writer.write_u64::<BigEndian>(input_size).unwrap();
                writer.write_u64::<BigEndian>(input_size).unwrap();
                body
            }
            Self::Compute => {
                let iterations = input_size;
                let get_uri = format!("http://{}:{}/iterations/{}", storage_ip, 8000, iterations);
                let post_uri = format!("http://{}:{}/post", storage_ip, 8000);
                format!("{}::{}", get_uri, post_uri).into_bytes()
            }
            Self::Io => {
                let bytes = input_size;
                let get_uri = format!("http://{}:{}/bytes/{}", storage_ip, 8000, bytes);
                let post_uri = format!("http://{}:{}/post", storage_ip, 8000);
                format!("{}::{}", get_uri, post_uri).into_bytes()
            }
        }
    }

    pub fn checksum(&self, input_size: u64) -> u64 {
        match self {
            Self::Matmul => {
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
            Self::Compute => input_size,
            Self::Io => 2000000,
        }
    }
}

//! Utility to convert line protocol used for benchmarking line protocol parsers into binflux for
//! comparing performance to binflux parsing.
//!
//! # Usage
//!
//! ```
//! cargo run --bin line-protocol-to-binflux < input-line-protocol.txt > output.binflux
//! ```

use tremor_influx::decode;
use tremor_runtime::{
    codec::binflux::BInflux,
    postprocessor::{LengthPrefix, Postprocessor},
};

use std::io::{self, Read, Write};

fn main() {
    let mut buffer = String::new();
    let stdin = io::stdin();
    let mut stdin_handle = stdin.lock();
    stdin_handle.read_to_string(&mut buffer).unwrap();

    let values: Vec<simd_json::BorrowedValue> = buffer
        .lines()
        .map(|line| {
            let result = decode(line, 0);
            match result {
                Ok(Some(v)) => v,
                other => panic!(
                    "failed to decode line protocol. Error: {:?} for line \"{}\"",
                    other, line
                ),
            }
        })
        .collect();

    let stdout = io::stdout();
    let mut stdout_handle = stdout.lock();

    let mut post_p = LengthPrefix::default();

    for val in values {
        let binflux = BInflux::encode(&val).unwrap();
        let bytes: Vec<_> = post_p
            .process(0, 0, &binflux)
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        stdout_handle.write_all(&bytes).unwrap();
    }
}

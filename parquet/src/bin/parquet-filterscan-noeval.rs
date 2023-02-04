// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Binary file to read data from a Parquet file.
//!
//! # Install
//!
//! `parquet-read` can be installed using `cargo`:
//! ```
//! cargo install parquet --features=cli
//! ```
//! After this `parquet-read` should be available:
//! ```
//! parquet-read XYZ.parquet
//! ```
//!
//! The binary can also be built from the source code and run as follows:
//! ```
//! cargo run --features=cli --bin parquet-read XYZ.parquet
//! ```
//!
//! Note that `parquet-read` reads full file schema, no projection or filtering is
//! applied.

use clap::Parser;
use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::io::{self, Read};
use std::{fs::File, path::Path};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to test zonemap skipping without evaluation on decoded values"), long_about = None)]
struct Args {
    #[clap(short, long, help("Path to a parquet file, or - for stdin"))]
    file_name: String,
    #[clap(short('i'), long, help("Column idx to filter"))]
    filter_col_idx: usize,
    // #[clap(short('v1'), long, help("v1 lower bound"))]
    // filter_col_idx: usize,
}

fn main() {
    let args = Args::parse();

    let filename = args.file_name;
    let filter_col_idx = args.filter_col_idx;
    let batch_size: usize = 1024;

    let file = File::open(filename).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    let mut res = Ok((0, 0));
    let mut values: Vec<i64> = vec![0; batch_size];
    let mut def_levels = vec![0; batch_size];
    let mut rep_levels = vec![0; batch_size];
    let mut total_decoded = 0;

    for i in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(i).unwrap();
        let row_group_metadata = metadata.row_group(i);

        // for j in 0..row_group_metadata.num_columns() {
        let mut column_reader =
            row_group_reader.get_column_reader(filter_col_idx).unwrap();
        match column_reader {
            // You can also use `get_typed_column_reader` method to extract typed reader.
            ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                // typed_reader.skip_records(124);
                loop {
                    res = typed_reader.read_batch(
                        batch_size, // batch size
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut values,
                    );
                    // println!("{:#?}", values);
                    if res.as_ref().unwrap().1 == 0 {
                        break;
                    }
                    total_decoded += res.as_ref().unwrap().1;
                }
            }
            _ => {}
        }
        // }
    }
    println!("total_decoded: {}", total_decoded);
}

fn print_row(row: &Row, json: bool) {
    if json {
        println!("{}", row.to_json_value())
    } else {
        println!("{}", row);
    }
}

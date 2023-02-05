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
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::column::reader::ColumnReader;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::errors::{ParquetError, Result};
use parquet::file::page_index::index::{Index, PageIndex};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::format::PageLocation;
use parquet::record::Row;
use perf_monitor;
use std::collections::VecDeque;
use std::io::{self, Read};
use std::time::SystemTime;
use std::{fs::File, path::Path};

#[derive(Debug, Parser)]
#[clap(author, version, about("Binary file to test zonemap skipping without evaluation on decoded values"), long_about = None)]
struct Args {
    #[clap(short, long, help("Path to a parquet file, or - for stdin"))]
    file_name: String,
    #[clap(short('i'), long, help("Column idx to filter"))]
    filter_col_idx: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let filename = args.file_name;
    let filter_col_idx = args.filter_col_idx;
    let batch_size: usize = 1024 * 1024;
    let mut values_str: Vec<ByteArray> = vec![ByteArray::new(); batch_size];

    let start = SystemTime::now();
    let file = File::open(filename).unwrap();
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(file, options).unwrap();
    let metadata = reader.metadata();
    // let mut values: ValuesBufferSlice = match metadata
    //     .file_metadata()
    //     .schema_descr()
    //     .column(filter_col_idx)
    //     .physical_type()
    // {
    //     parquet::basic::Type::INT64 => values_i64.clone(),
    //     parquet::basic::Type::BYTE_ARRAY => values_str.clone(),
    //     _ => panic!("Unsupported type"),
    // };
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
            ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                loop {
                    let mut read_records = 0;
                    // NOTE(xinyu): logic from arrow_reader/mod.rs:480
                    match typed_reader.read_batch(
                        batch_size,
                        Some(&mut def_levels),
                        Some(&mut rep_levels),
                        &mut values_str,
                    ) {
                        Ok((_, 0)) => break,
                        Ok((_, rec)) => {
                            read_records += rec;
                            total_decoded += rec;
                        }
                        Err(error) => return Err(error.into()),
                    }
                    if read_records == 0 {
                        break;
                    }
                }
            }
            _ => {
                println!("not supported")
            }
        }
        // }
    }
    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    println!("total_decoded: {}", total_decoded);
    // println!("{:#?}", values_str);
    // io
    let io_stat = perf_monitor::io::get_process_io_stats().unwrap();
    println!(
        "[IO] io-in: {} bytes, io-out: {} bytes",
        io_stat.read_bytes, io_stat.write_bytes
    );
    Ok(())
}

/// Computes the number of rows in each page within a column chunk
fn compute_row_counts(offset_index: &[PageLocation], rows: i64) -> Vec<i64> {
    if offset_index.is_empty() {
        return vec![];
    }

    let mut last = offset_index[0].first_row_index;
    let mut out = Vec::with_capacity(offset_index.len());
    for o in offset_index.iter().skip(1) {
        out.push(o.first_row_index - last);
        last = o.first_row_index;
    }
    out.push(rows - last);
    out
}

/// Prints index information for a single column chunk
fn generate_row_selection<T: std::fmt::Display + std::cmp::PartialOrd>(
    column_index: &[PageIndex<T>],
    offset_index: &[PageLocation],
    row_counts: &[i64],
    v1: &T,
    v2: &T,
) -> Result<VecDeque<RowSelector>, ParquetError> {
    if column_index.len() != offset_index.len() {
        return Err(ParquetError::General(format!(
            "Index length mismatch, got {} and {}",
            column_index.len(),
            offset_index.len()
        )));
    }
    let mut res: VecDeque<RowSelector> = vec![].into();

    for (idx, ((c, o), row_count)) in column_index
        .iter()
        .zip(offset_index)
        .zip(row_counts)
        .enumerate()
    {
        if v2 < c.min.as_ref().unwrap() || v1 > c.max.as_ref().unwrap() {
            res.push_back(RowSelector::skip(*row_count as usize));
        } else {
            res.push_back(RowSelector::select(*row_count as usize));
        }
    }

    Ok(res)
}

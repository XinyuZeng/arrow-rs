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
    #[clap(short('l'), long, help("v1 lower bound"))]
    v1: String,
    #[clap(short('r'), long, help("v2 upper bound"))]
    v2: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let filename = args.file_name;
    let filter_col_idx = args.filter_col_idx;
    let v1 = args.v1;
    let v2 = args.v2;
    let batch_size: usize = 1024;

    let mut values_i64: Vec<i64> = vec![0; batch_size];
    let mut values_str: Vec<ByteArray> = vec![ByteArray::new(); batch_size];
    let start = SystemTime::now();
    let mut values_double: Vec<f64> = vec![0.0; batch_size];
    let file = File::open(filename).unwrap();
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(file, options).unwrap();
    let metadata = reader.metadata();
    // Column index data for all row groups and columns
    let column_index = metadata
        .page_indexes()
        .ok_or_else(|| ParquetError::General("Column index not found".to_string()))?;

    // Offset index data for all row groups and columns
    let offset_index = metadata
        .offset_indexes()
        .ok_or_else(|| ParquetError::General("Offset index not found".to_string()))?;

    let mut selections_per_group: Vec<VecDeque<RowSelector>> = vec![];
    // Iterate through each row group
    for (row_group_idx, ((column_indices, offset_indices), row_group)) in column_index
        .iter()
        .zip(offset_index)
        .zip(reader.metadata().row_groups())
        .enumerate()
    {
        // println!("Row Group: {row_group_idx}");
        let offset_index = offset_indices.get(filter_col_idx).ok_or_else(|| {
            ParquetError::General(format!(
                "No offset index for row group {row_group_idx} column chunk {filter_col_idx}"
            ))
        })?;

        let row_counts = compute_row_counts(offset_index, row_group.num_rows());
        let selections: VecDeque<RowSelector> = match &column_indices[filter_col_idx] {
            Index::NONE => {
                println!("NO INDEX");
                VecDeque::<RowSelector>::from(vec![RowSelector::skip(0)])
            }
            Index::INT64(v) => generate_row_selection(
                &v.indexes,
                offset_index,
                &row_counts,
                &v1.parse::<i64>().unwrap(),
                &v2.parse::<i64>().unwrap(),
            )
            .unwrap(),
            Index::DOUBLE(v) => generate_row_selection(
                &v.indexes,
                offset_index,
                &row_counts,
                &v1.parse::<f64>().unwrap(),
                &v2.parse::<f64>().unwrap(),
            )
            .unwrap(),
            Index::BYTE_ARRAY(v) => generate_row_selection(
                &v.indexes,
                offset_index,
                &row_counts,
                &ByteArray::from(v1.as_bytes().to_vec()),
                &ByteArray::from(v2.as_bytes().to_vec()),
            )
            .unwrap(),
            _ => {
                println!("NOT supported type");
                VecDeque::<RowSelector>::from(vec![RowSelector::skip(0)])
            }
        };
        selections_per_group.push(selections);
    }
    // println!("Finish row selection take: {:?}", start.elapsed().unwrap());
    // println!("{:?}", selections_per_group);
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
        let selection = &mut selections_per_group[i];

        // for j in 0..row_group_metadata.num_columns() {
        let mut column_reader =
            row_group_reader.get_column_reader(filter_col_idx).unwrap();
        match column_reader {
            // You can also use `get_typed_column_reader` method to extract typed reader.
            ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                loop {
                    // NOTE(xinyu): logic from arrow_reader/mod.rs:480
                    let mut read_records = 0;
                    while read_records < batch_size && !selection.is_empty() {
                        let front = selection.pop_front().unwrap();
                        if front.skip {
                            let skipped = match typed_reader.skip_records(front.row_count)
                            {
                                Ok(skipped) => skipped,
                                Err(e) => return Err(e.into()),
                            };

                            if skipped != front.row_count {
                                return Err(ParquetError::General(format!(
                                    "failed to skip rows, expected {}, got {}",
                                    front.row_count, skipped
                                )));
                            }
                            continue;
                        }
                        if front.row_count == 0 {
                            continue;
                        }

                        // try to read record
                        let need_read = batch_size - read_records;
                        let to_read = match front.row_count.checked_sub(need_read) {
                            Some(remaining) if remaining != 0 => {
                                // if page row count less than batch_size we must set batch size to page row count.
                                // add check avoid dead loop
                                selection.push_front(RowSelector::select(remaining));
                                need_read
                            }
                            _ => front.row_count,
                        };
                        match typed_reader.read_batch(
                            to_read,
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values_i64,
                        ) {
                            Ok((_, 0)) => break,
                            Ok((_, rec)) => {
                                read_records += rec;
                                total_decoded += rec;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    if read_records == 0 {
                        break;
                    }
                }
            }
            ColumnReader::DoubleColumnReader(ref mut typed_reader) => {
                loop {
                    // NOTE(xinyu): logic from arrow_reader/mod.rs:480
                    let mut read_records = 0;
                    while read_records < batch_size && !selection.is_empty() {
                        let front = selection.pop_front().unwrap();
                        if front.skip {
                            let skipped = match typed_reader.skip_records(front.row_count)
                            {
                                Ok(skipped) => skipped,
                                Err(e) => return Err(e.into()),
                            };

                            if skipped != front.row_count {
                                return Err(ParquetError::General(format!(
                                    "failed to skip rows, expected {}, got {}",
                                    front.row_count, skipped
                                )));
                            }
                            continue;
                        }
                        if front.row_count == 0 {
                            continue;
                        }

                        // try to read record
                        let need_read = batch_size - read_records;
                        let to_read = match front.row_count.checked_sub(need_read) {
                            Some(remaining) if remaining != 0 => {
                                // if page row count less than batch_size we must set batch size to page row count.
                                // add check avoid dead loop
                                selection.push_front(RowSelector::select(remaining));
                                need_read
                            }
                            _ => front.row_count,
                        };
                        match typed_reader.read_batch(
                            to_read,
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut values_double,
                        ) {
                            Ok((_, 0)) => break,
                            Ok((_, rec)) => {
                                read_records += rec;
                                total_decoded += rec;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    if read_records == 0 {
                        break;
                    }
                }
            }
            ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                loop {
                    // NOTE(xinyu): logic from arrow_reader/mod.rs:480
                    let mut read_records = 0;
                    while read_records < batch_size && !selection.is_empty() {
                        let front = selection.pop_front().unwrap();
                        if front.skip {
                            let skipped = match typed_reader.skip_records(front.row_count)
                            {
                                Ok(skipped) => skipped,
                                Err(e) => return Err(e.into()),
                            };

                            if skipped != front.row_count {
                                return Err(ParquetError::General(format!(
                                    "failed to skip rows, expected {}, got {}",
                                    front.row_count, skipped
                                )));
                            }
                            continue;
                        }
                        if front.row_count == 0 {
                            continue;
                        }

                        // try to read record
                        let need_read = batch_size - read_records;
                        let to_read = match front.row_count.checked_sub(need_read) {
                            Some(remaining) if remaining != 0 => {
                                // if page row count less than batch_size we must set batch size to page row count.
                                // add check avoid dead loop
                                selection.push_front(RowSelector::select(remaining));
                                need_read
                            }
                            _ => front.row_count,
                        };
                        match typed_reader.read_batch(
                            to_read,
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

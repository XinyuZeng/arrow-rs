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

use arrow::util::pretty::print_batches;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions, RowFilter,
};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::Result;
use perf_monitor;
use std::env;
use std::process::Command;
use std::string::String;
use std::time::SystemTime;
use tokio::fs::File;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 8 {
        println!("Usage: cargo run --example filterscan <path> <filter_col_idx> <data_type> <filter_type> <v1> <v2> <proj_type>");
        println!("Example: cargo run --example filterscan /tmp/tpch/lineitem.parquet 0 int point 1 none all");
        println!("Example: cargo run --example filterscan /tmp/tpch/lineitem.parquet 0 int range 1 2 all");
        println!("Example: cargo run --example filterscan /tmp/tpch/lineitem.parquet 0 int none 1 none all");
        println!("Example: cargo run --example filterscan /tmp/tpch/lineitem.parquet 0 int none 1 none one");
        return Ok(());
    }
    let path = &args[1];
    let filter_col_idx = args[2].parse::<usize>().unwrap();
    let data_type = String::from(&args[3]); // int, float, string
    let filter_type = String::from(&args[4]); // point, range, none
    let v1 = String::from(&args[5]);
    let v2 = String::from(&args[6]);
    let proj_type = String::from(&args[7]); // all, one
    let start = SystemTime::now();
    let file = File::open(path).await.unwrap();

    // Create an async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let options = ArrowReaderOptions::new().with_page_index(true);
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
        .await
        .unwrap()
        .with_batch_size(1024);

    let file_metadata = builder.metadata().file_metadata().clone();
    // let num_cols = file_metadata.schema_descr().num_columns();
    let mask = if let "all" = proj_type.as_str() {
        // Read all columns.
        ProjectionMask::all()
    } else {
        // Read only filter column
        ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx])
    };

    // Set projection mask to read only root columns 1 and 2.
    builder = builder.with_projection(mask);

    // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
    // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.
    let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];
    if filter_type == "none" {
        // do nothing
    } else if filter_type == "point" {
        if data_type == "int" {
            let filter_col_val = v1.parse::<i32>().unwrap();
            let filter = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::eq_dyn_scalar(record_batch.column(0), filter_col_val)
                },
            );
            filters.push(Box::new(filter));
        } else if data_type == "float" {
            let filter_col_val = v1.parse::<f64>().unwrap();
            let filter = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::eq_dyn_scalar(record_batch.column(0), filter_col_val)
                },
            );
            filters.push(Box::new(filter));
        } else if data_type == "string" {
            let filter_col_val = v1.parse::<String>().unwrap();
            let filter = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::eq_dyn_utf8_scalar(
                        record_batch.column(0),
                        &filter_col_val,
                    )
                },
            );
            filters.push(Box::new(filter));
        } else {
            panic!("Unsupported data type");
        }
    } else if filter_type == "range" {
        if data_type == "int" {
            let filter_col_val1 = v1.parse::<i32>().unwrap();
            let filter_col_val2 = v2.parse::<i32>().unwrap();
            let filter1 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::gt_eq_dyn_scalar(
                        record_batch.column(0),
                        filter_col_val1,
                    )
                },
            );
            let filter2 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::lt_eq_dyn_scalar(
                        record_batch.column(0),
                        filter_col_val2,
                    )
                },
            );
            filters.push(Box::new(filter1));
            filters.push(Box::new(filter2));
        } else if data_type == "float" {
            let filter_col_val1 = v1.parse::<f64>().unwrap();
            let filter_col_val2 = v2.parse::<f64>().unwrap();
            let filter1 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::gt_eq_dyn_scalar(
                        record_batch.column(0),
                        filter_col_val1,
                    )
                },
            );
            let filter2 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::lt_eq_dyn_scalar(
                        record_batch.column(0),
                        filter_col_val2,
                    )
                },
            );
            filters.push(Box::new(filter1));
            filters.push(Box::new(filter2));
        } else if data_type == "string" {
            let filter_col_val1 = v1.parse::<String>().unwrap();
            let filter_col_val2 = v2.parse::<String>().unwrap();
            let filter1 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::gt_eq_dyn_utf8_scalar(
                        record_batch.column(0),
                        &filter_col_val1,
                    )
                },
            );
            let filter2 = ArrowPredicateFn::new(
                ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
                move |record_batch| {
                    arrow::compute::lt_eq_dyn_utf8_scalar(
                        record_batch.column(0),
                        &filter_col_val2,
                    )
                },
            );
            filters.push(Box::new(filter1));
            filters.push(Box::new(filter2));
        } else {
            panic!("Unsupported data type");
        }
    } else {
        panic!("Unsupported filter type");
    }

    let row_filter = RowFilter::new(filters);
    builder = builder.with_row_filter(row_filter);

    // Build a async parquet reader.
    let stream = builder.build().unwrap();
    // println!(
    //     "before collect: {} ms",
    //     start.elapsed().unwrap().as_millis()
    // );

    let _result = stream.try_collect::<Vec<_>>().await?;

    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    // print_batches(&_result).unwrap();

    // let output = Command::new("/bin/cat")
    //     .arg("/proc/$PPID/io")
    //     .output()
    //     .expect("failed to execute process");
    // println!("status: {}", output.status);
    // println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    // println!("stderr: {}", String::from_utf8_lossy(&output.stderr));

    // io
    let io_stat = perf_monitor::io::get_process_io_stats().unwrap();
    println!(
        "[IO] io-in: {} bytes, io-out: {} bytes",
        io_stat.read_bytes, io_stat.write_bytes
    );
    Ok(())
}

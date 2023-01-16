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
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
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
    // Create parquet file that will be read.
    // let testdata = arrow::util::test_util::parquet_test_data();
    let path = &args[1];
    let filter_col_idx = args[2].parse::<usize>().unwrap();
    // let filter_col_val = String::from(&args[3]);
    let filter_col_val = args[3].parse::<i32>().unwrap();
    let start = SystemTime::now();
    let file = File::open(path).await.unwrap();

    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(1024);

    let file_metadata = builder.metadata().file_metadata().clone();
    let mask = ProjectionMask::roots(
        file_metadata.schema_descr(),
        (0..15).collect::<Vec<usize>>(),
    );
    // Set projection mask to read only root columns 1 and 2.
    builder = builder.with_projection(mask);

    // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
    // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.
    let filter = ArrowPredicateFn::new(
        ProjectionMask::roots(file_metadata.schema_descr(), [filter_col_idx]),
        move |record_batch| {
            // arrow::compute::eq_dyn_utf8_scalar(record_batch.column(0), &filter_col_val)
            arrow::compute::eq_dyn_scalar(record_batch.column(0), filter_col_val)
            // arrow::compute::lt_eq_dyn_utf8_scalar(record_batch.column(0), &filter_col_val)
        },
    );
    let row_filter = RowFilter::new(vec![Box::new(filter)]);
    builder = builder.with_row_filter(row_filter);

    // Build a async parquet reader.
    let stream = builder.build().unwrap();

    let result = stream.try_collect::<Vec<_>>().await?;

    println!("took: {} ms", start.elapsed().unwrap().as_millis());

    print_batches(&result).unwrap();

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

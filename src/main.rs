use connectorx::{destinations, prelude::*};
use futures::future::try_join_all;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, RequestExt, Response};
use polars::prelude::ParquetCompression;
use polars::prelude::{df, ParquetWriter};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::{convert::TryFrom, rc::Rc};
use tokio::task::JoinSet;
/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(_event: Request) -> Result<Response<Body>, Error> {
    // Extract some useful information from the request

    let message = format!("Hello , this is an AWS Lambda HTTP request");

    tracing::info!("Inside the lambda function. ");

    let source_conn = SourceConn::try_from(
        "postgresql://dbo:dbo@partaccountpoc-cluster.cluster-ctcsve9jprsn.us-east-1.rds.amazonaws.com:5432/partaccountpoc?cxprotocol=binary",
    )
    .expect("parse conn str failed");

    let src_conn = Arc::new(source_conn);

    tracing::info!("After getting back connection.");

    let mut handles = Vec::new();

    {
        let src_conn = src_conn.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let queries = &[CXQuery::from(
                "SELECT * FROM part_account where part_account.bekid=1",
            )];
            let destination: Arrow2Destination =
                get_arrow2(&src_conn, None, queries).expect("run failed");
            let mut df = destination.polars().unwrap();
            ParquetWriter::new(std::fs::File::create("/tmp/result1.parquet").unwrap())
                .with_statistics(true)
                .with_compression(ParquetCompression::Uncompressed)
                .finish(&mut df)
                .unwrap();
            // tracing::info!("dataframe size {:?}", arrow);
        });

        handles.push(handle);
    }
    {
        let src_conn = src_conn.clone();
        let handle2 = tokio::task::spawn_blocking(move || {
            let queries = &[CXQuery::from(
                "SELECT * FROM part_account where part_account.bekid=2",
            )];
            let destination: Arrow2Destination =
                get_arrow2(&src_conn, None, queries).expect("run failed");
            let mut df = destination.polars().unwrap();
            ParquetWriter::new(std::fs::File::create("/tmp/result2.parquet").unwrap())
                .with_statistics(true)
                .with_compression(ParquetCompression::Uncompressed)
                .finish(&mut df)
                .unwrap();
            // tracing::info!("dataframe size {:?}", arrow);
        });

        handles.push(handle2);
    }
    let results = try_join_all(handles).await;
    match results {
        Ok(_) => tracing::info!("All tasks completed successfully"),
        Err(e) => tracing::error!("Error: {}", e),
    }

    tracing::info!("After get Arrow");

    let file_path = "/tmp/parquet_writer_test.parquet"; // Replace with your file path

    // Open the file
    let file = File::open(file_path)?;

    // Get the metadata of the file, which includes information like size
    let metadata = file.metadata()?;

    // Extract the size from the metadata
    let file_size = metadata.len();

    // Print the file size
    tracing::info!("Parquet File size: {} bytes", file_size);

    // Return something that implements IntoResponse.
    // It will be serialized to the right response event automatically by the runtime
    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(message.into())
        .map_err(Box::new)?;
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use connectorx::prelude::*;
use futures::future::try_join_all;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, Response};
use polars::prelude::ParquetCompression;
use polars::prelude::{df, ParquetWriter};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{convert::TryFrom, rc::Rc};
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

    for i in 1..50 {
        let src_conn = src_conn.clone();

        //     let src_conn = SourceConn::try_from(
        //     "postgresql://dbo:dbo@partaccountpoc-cluster.cluster-ctcsve9jprsn.us-east-1.rds.amazonaws.com:5432/partaccountpoc?cxprotocol=binary",
        // )
        // .expect("parse conn str failed");
        let handle = tokio::task::spawn_blocking(move || {
            let query = format!("SELECT * FROM part_account where part_account.bekid={}", i);
            let queries = &[CXQuery::from(query.as_str())];
            let destination: Arrow2Destination =
                get_arrow2(&src_conn, None, queries).expect("run failed");
            let mut df = destination.polars().unwrap();
            ParquetWriter::new(std::fs::File::create(format!("/tmp/result{}.parquet", i)).unwrap())
                .with_statistics(true)
                .with_compression(ParquetCompression::Uncompressed)
                .finish(&mut df)
                .unwrap();
            // tracing::info!("dataframe size {:?}", arrow);
        });

        handles.push(handle);
    }

    let results = try_join_all(handles).await;

    match results {
        Ok(_) => {
            let mut transfer_tasks = Vec::new();
            for i in 1..50 {
                transfer_tasks.push(tokio::spawn(transfer_to_s3(i)));
            }
            for task in transfer_tasks {
                let _ = task.await.expect("failed to transfer");
            }

            tracing::info!("All tasks completed successfully")
        }
        Err(e) => tracing::error!("Error: {}", e),
    }

    tracing::info!("After get Arrow");

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(message.into())
        .map_err(Box::new)?;
    Ok(resp)
}

async fn transfer_to_s3(id: u16) {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let bucket_name = "pensioncalcseast1";

    let filename = format!("/tmp/result{}.parquet", id);
    let body = ByteStream::from_path(Path::new(&filename)).await;

    let s3_key = format!("results/result{}.parquet", id);

    let response = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await;

    match response {
        Ok(_) => {
            tracing::info!(
                filename = %filename,
                "data successfully stored in S3",
            );
            // Return `Response` (it will be serialized to JSON automatically by the runtime)
        }
        Err(err) => {
            // In case of failure, log a detailed error to CloudWatch.
            tracing::error!(
                err = %err,
                filename = %filename,
                "failed to upload data to S3"
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}

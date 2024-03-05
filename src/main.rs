use connectorx::prelude::*;
use lambda_http::{run, service_fn, tracing, Body, Error, Request, RequestExt, Response};
use std::convert::TryFrom;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    // Extract some useful information from the request

    let message = format!("Hello , this is an AWS Lambda HTTP request");

    let source_conn = SourceConn::try_from(
        "postgresql://dbo:dbo@partaccountpoc-cluster.cluster-ctcsve9jprsn.us-east-1.rds.amazonaws.com:5432/partaccountpoc?cxprotocol=binary",
    )
    .expect("parse conn str failed");
    let queries = &[CXQuery::from(
        "SELECT * FROM part_account where part_account.bekid=1",
    )];

    let destination: Arrow2Destination =
        get_arrow2(&source_conn, None, queries).expect("run failed");

    let arrow = destination.polars().unwrap();
    println!("dataframe size {:?}", arrow);
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

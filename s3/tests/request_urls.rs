// Model-output: Claude Fable 5.1

//! The request line bucket methods put on the wire, against the fake S3: subresources and
//! identifiers travel in the query, percent-encoded, and a key never becomes one.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, reply, serve, within};

#[tokio::test]
async fn subresources_and_ids_travel_in_the_query_and_keys_in_the_path() {
    let (endpoint, server) = serve(vec![
        reply(http(
            "200 OK",
            &[],
            b"<LocationConstraint>eu-west-1</LocationConstraint>",
        )),
        reply(http("204 No Content", &[], b"")),
        reply(http("204 No Content", &[], b"")),
    ])
    .await;
    let bucket = bucket(&endpoint);

    let (region, status) = within(bucket.location()).await.unwrap();
    assert_eq!((region.to_string().as_str(), status), ("eu-west-1", 200));
    within(bucket.abort_upload("/dir/k", "a+b/c=d&e"))
        .await
        .unwrap();
    within(bucket.delete_object("a?location#x")).await.unwrap();
    let heads = within(server).await.unwrap();

    let lines: Vec<&str> = heads
        .iter()
        .map(|head| head.lines().next().unwrap())
        .collect();
    assert_eq!(
        lines,
        [
            format!("GET /{BUCKET}/?location HTTP/1.1"),
            format!("DELETE /{BUCKET}/dir/k?uploadId=a%2Bb%2Fc%3Dd%26e HTTP/1.1"),
            format!("DELETE /{BUCKET}/a%3Flocation%23x HTTP/1.1"),
        ]
    );
}

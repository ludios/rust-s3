// Model-output: Claude Fable 5.1

//! `get_object_stream` and `get_object_range_stream` against the fake S3: the request each
//! sends, and the status, headers and body each returns.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{bucket, header_of, http, reply, serve, within};
use futures_util::StreamExt;
use s3::request::ResponseDataStream;

/// Reads `stream` to its end.
async fn read_all(stream: &mut ResponseDataStream) -> Vec<u8> {
    let mut read = Vec::new();
    while let Some(chunk) = within(stream.bytes().next()).await {
        read.extend_from_slice(&chunk.unwrap());
    }
    read
}

#[tokio::test]
async fn get_object_stream_carries_the_response_headers() {
    let whole = http("200 OK", &[("etag", "\"abc\"")], b"hello, world");
    let (endpoint, server) = serve(vec![reply(whole)]).await;
    let bucket = bucket(&endpoint);

    let mut stream = within(bucket.get_object_stream("/hello.txt"))
        .await
        .unwrap();
    let read = read_all(&mut stream).await;
    let heads = within(server).await.unwrap();

    assert_eq!(header_of(&heads[0], "range"), None);
    assert_eq!(stream.status_code, 200);
    assert_eq!(stream.headers["content-length"], "12");
    assert_eq!(stream.headers["etag"], "\"abc\"");
    assert_eq!(read, b"hello, world");
}

#[tokio::test]
async fn get_object_range_stream_sends_an_inclusive_range_and_streams_the_206() {
    let partial = http(
        "206 Partial Content",
        &[("content-range", "bytes 7-11/12")],
        b"world",
    );
    let (endpoint, server) = serve(vec![reply(partial)]).await;
    let bucket = bucket(&endpoint);

    let mut stream = within(bucket.get_object_range_stream("/hello.txt", 7, Some(11)))
        .await
        .unwrap();
    let read = read_all(&mut stream).await;
    let heads = within(server).await.unwrap();

    assert_eq!(header_of(&heads[0], "range"), Some("bytes=7-11"));
    assert_eq!(stream.status_code, 206);
    assert_eq!(stream.headers["content-length"], "5");
    assert_eq!(stream.headers["content-range"], "bytes 7-11/12");
    assert_eq!(read, b"world");
}

#[tokio::test]
async fn get_object_range_stream_without_an_end_asks_for_the_rest() {
    let partial = http(
        "206 Partial Content",
        &[("content-range", "bytes 7-11/12")],
        b"world",
    );
    let (endpoint, server) = serve(vec![reply(partial)]).await;
    let bucket = bucket(&endpoint);

    let mut stream = within(bucket.get_object_range_stream("/hello.txt", 7, None))
        .await
        .unwrap();
    let read = read_all(&mut stream).await;
    let heads = within(server).await.unwrap();

    assert_eq!(header_of(&heads[0], "range"), Some("bytes=7-"));
    assert_eq!(stream.status_code, 206);
    assert_eq!(read, b"world");
}

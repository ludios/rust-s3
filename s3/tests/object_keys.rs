// Model-output: Claude Opus 5.5

//! Object keys against the fake S3: which keys are refused before anything is sent, and that
//! the rest reach the server as the key, percent-encoded where needed.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, reply, serve, within};
use s3::Bucket;
use s3::bucket::CHUNK_SIZE;
use s3::error::S3Error;
use tokio::io::AsyncReadExt;

/// The message of the error `result` failed with, if it failed.
fn error_message<T>(result: Result<T, S3Error>) -> Option<String> {
    result.err().map(|error| error.to_string())
}

/// Asserts that every kind of object request for `key`, sent or presigned, fails with
/// `refusal`.
async fn assert_refused(bucket: &Bucket, key: &str, refusal: S3Error) {
    let tags = [("k", "v")];
    let attempts = [
        (
            "head_object",
            error_message(within(bucket.head_object(key)).await),
        ),
        (
            "get_object",
            error_message(within(bucket.get_object(key)).await),
        ),
        (
            "put_object",
            error_message(within(bucket.put_object(key, b"body")).await),
        ),
        (
            "delete_object",
            error_message(within(bucket.delete_object(key)).await),
        ),
        (
            "copy_object_internal",
            error_message(within(bucket.copy_object_internal("source", key)).await),
        ),
        (
            "put_object_tagging",
            error_message(within(bucket.put_object_tagging(key, &tags)).await),
        ),
        (
            "delete_object_tagging",
            error_message(within(bucket.delete_object_tagging(key)).await),
        ),
        (
            "abort_upload",
            error_message(within(bucket.abort_upload(key, "upload-id")).await),
        ),
        (
            "presign_get",
            error_message(within(bucket.presign_get(key, 60, None)).await),
        ),
        (
            "presign_put",
            error_message(within(bucket.presign_put(key, 60, None, None)).await),
        ),
        (
            "presign_delete",
            error_message(within(bucket.presign_delete(key, 60)).await),
        ),
    ];
    let refusal = Some(refusal.to_string());
    for (method, message) in attempts {
        assert_eq!(message, refusal, "{method}({key:?})");
    }
}

#[tokio::test]
async fn empty_keys_are_refused_and_only_delete_deletes_the_bucket() {
    let (endpoint, server) = serve(vec![reply(http("204 No Content", &[], b""))]).await;
    let bucket = bucket(&endpoint);

    // Both would address `/test-bucket/`, where a DELETE is a DeleteBucket.
    for key in ["", "/"] {
        assert_refused(&bucket, key, S3Error::EmptyKey(key.to_owned())).await;
    }
    within(bucket.delete()).await.unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(heads.len(), 1);
    let request_line = heads[0].lines().next().unwrap();
    assert_eq!(request_line, format!("DELETE /{BUCKET}/ HTTP/1.1"));
}

#[tokio::test]
async fn keys_with_a_dot_segment_are_refused_and_other_dots_are_sent_unchanged() {
    let (endpoint, server) = serve(vec![reply(http("204 No Content", &[], b""))]).await;
    let bucket = bucket(&endpoint);

    // URL parsing would resolve these to `/test-bucket/b`, `/other-bucket/secret`,
    // `/test-bucket/a/b`, the bucket itself (DeleteBucket), and `/` (ListBuckets).
    for key in [
        "a/../b",
        "dir/../../other-bucket/secret",
        "a/./b",
        ".",
        "a/..",
        "..",
        "/..",
    ] {
        assert_refused(&bucket, key, S3Error::DotSegmentKey(key.to_owned())).await;
    }
    within(bucket.delete_object("/.a/.../b./..c"))
        .await
        .unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(heads.len(), 1);
    let request_line = heads[0].lines().next().unwrap();
    assert_eq!(
        request_line,
        format!("DELETE /{BUCKET}/.a/.../b./..c HTTP/1.1")
    );
}

/// URL parsing also resolves `%2e` segments and splits paths at `\`; keys that spell dot
/// segments with them are not refused, because both characters are percent-encoded.
#[tokio::test]
async fn percent_and_backslash_in_keys_are_encoded_so_no_dot_segment_appears() {
    let bucket = bucket("http://127.0.0.1:9000");

    for (key, sent) in [("a/%2e%2e/b", "a/%252e%252e/b"), ("a\\..\\b", "a%5C..%5Cb")] {
        let url = within(bucket.presign_get(key, 60, None)).await.unwrap();
        let expected = format!("http://127.0.0.1:9000/{BUCKET}/{sent}?");
        assert!(url.starts_with(&expected), "{key:?}: {url}");
    }
}

/// The key `//` names the object `/`, which the server echoes back when the multipart upload
/// is initiated. The parts and the completion must still address `/test-bucket//`.
#[tokio::test]
async fn a_streamed_upload_to_the_object_named_slash_addresses_it_throughout() {
    let initiate = http(
        "200 OK",
        &[],
        b"<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>/</Key>\
          <UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
    );
    let part = http("200 OK", &[("etag", "\"part\"")], b"");
    let complete = http("200 OK", &[], b"<CompleteMultipartUploadResult/>");
    let (endpoint, server) = serve(vec![
        reply(initiate),
        reply(part.clone()),
        reply(part),
        reply(complete),
    ])
    .await;
    let bucket = bucket(&endpoint);
    let mut reader = tokio::io::repeat(b'x').take(CHUNK_SIZE as u64 + 1);

    let response = within(bucket.put_object_stream(&mut reader, "//"))
        .await
        .unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(response.uploaded_bytes(), CHUNK_SIZE + 1);
    let targets: Vec<&str> = heads
        .iter()
        .map(|head| head.split_whitespace().nth(1).unwrap())
        .collect();
    let object = format!("/{BUCKET}//?");
    assert!(
        targets.iter().all(|target| target.starts_with(&object)),
        "{targets:?}"
    );
}

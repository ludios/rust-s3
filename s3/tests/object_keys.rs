// Model-output: Claude Opus 5.5

//! Object keys against the fake S3: which keys are refused before anything is sent, and that
//! the rest reach the server as the key, percent-encoded where needed.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, reply, serve, within};
use s3::Bucket;
use s3::error::S3Error;

/// The message of the error `result` failed with, if it failed.
fn error_message<T>(result: Result<T, S3Error>) -> Option<String> {
    result.err().map(|error| error.to_string())
}

/// Asserts that deleting `key`, putting it, and presigning a GET or a DELETE of it all fail
/// with `refusal`.
async fn assert_refused(bucket: &Bucket, key: &str, refusal: S3Error) {
    let refusal = Some(refusal.to_string());
    let deleted = within(bucket.delete_object(key)).await;
    assert_eq!(error_message(deleted), refusal, "delete_object({key:?})");
    let put = within(bucket.put_object(key, b"body")).await;
    assert_eq!(error_message(put), refusal, "put_object({key:?})");
    let presigned_get = within(bucket.presign_get(key, 60, None)).await;
    assert_eq!(
        error_message(presigned_get),
        refusal,
        "presign_get({key:?})"
    );
    let presigned_delete = within(bucket.presign_delete(key, 60)).await;
    assert_eq!(
        error_message(presigned_delete),
        refusal,
        "presign_delete({key:?})"
    );
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

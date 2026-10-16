// Model-output: Claude Opus 5.5

//! Object keys against the fake S3: which keys are refused before anything is sent, and that
//! the rest reach the server as the key, percent-encoded where needed.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, reply, serve, within};
use s3::error::S3Error;

/// Whether `result` is the refusal of `key` for its dot segment.
fn refused<T>(result: &Result<T, S3Error>, key: &str) -> bool {
    matches!(result, Err(S3Error::DotSegmentKey(refused)) if refused == key)
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
        let deleted = within(bucket.delete_object(key)).await;
        assert!(refused(&deleted, key), "{key:?}: {deleted:?}");
        let put = within(bucket.put_object(key, b"body")).await;
        assert!(refused(&put, key), "{key:?}: {put:?}");
        let presigned = within(bucket.presign_get(key, 60, None)).await;
        assert!(refused(&presigned, key), "{key:?}: {presigned:?}");
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

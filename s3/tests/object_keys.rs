// Model-output: Claude Opus 5.5

//! Object keys against the fake S3: which keys are refused before anything is sent, and that
//! the rest reach the server unchanged.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, reply, serve, within};
use s3::error::S3Error;

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
        assert!(
            matches!(deleted, Err(S3Error::DotSegmentKey(_))),
            "{key:?}: {deleted:?}"
        );
        let presigned = within(bucket.presign_get(key, 60, None)).await;
        assert!(
            matches!(presigned, Err(S3Error::DotSegmentKey(_))),
            "{key:?}: {presigned:?}"
        );
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

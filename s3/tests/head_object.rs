// Model-output: Claude Opus 5.5

//! `head_object` against the fake S3, on every runtime: a numeric or boolean header that does
//! not parse comes back as absent, never as 0 or false.

mod fake_s3;

use fake_s3::{bucket, http, reply, serve, within};
use s3::error::S3Error;
use s3::serde_types::HeadObjectResult;

/// HEADs `/object` on the fake S3 while it serves one 200 reply with `headers` and no body.
/// Returns what `head_object` returned.
fn head_with(headers: &[(&str, &str)]) -> Result<(HeadObjectResult, u16), S3Error> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (endpoint, server) = runtime.block_on(serve(vec![reply(http("200 OK", headers, b""))]));
    let bucket = bucket(&endpoint);
    #[cfg(feature = "with-tokio")]
    let result = runtime.block_on(within(bucket.head_object("/object")));
    #[cfg(feature = "with-async-std")]
    let result = async_std::task::block_on(bucket.head_object("/object"));
    #[cfg(feature = "sync")]
    let result = bucket.head_object("/object");
    runtime.block_on(within(server)).unwrap();
    result
}

/// A malformed `Content-Length` is no length. hyper, under reqwest or surf, hands it over on a
/// HEAD, which has no body to frame; attohttpc and async-h1 refuse the response.
#[test]
fn malformed_content_length_is_absent() {
    let result = head_with(&[("content-length", "12x")]);

    #[cfg(any(feature = "with-tokio", feature = "with-async-std-hyper"))]
    assert_eq!(result.unwrap().0.content_length, None);
    #[cfg(not(any(feature = "with-tokio", feature = "with-async-std-hyper")))]
    assert!(result.is_err(), "{result:?}");
}

/// The parts count, missing metadata count and delete marker of a HEAD reply carrying the
/// given values in their headers.
fn numbers_and_booleans(
    parts_count: &str,
    missing_meta: &str,
    delete_marker: &str,
) -> (Option<i64>, Option<i64>, Option<bool>) {
    let (head, _) = head_with(&[
        ("x-amz-mp-parts-count", parts_count),
        ("x-amz-missing-meta", missing_meta),
        ("x-amz-delete-marker", delete_marker),
    ])
    .unwrap();
    (head.parts_count, head.missing_meta, head.delete_marker)
}

#[test]
fn malformed_numbers_and_booleans_are_absent() {
    assert_eq!(
        numbers_and_booleans("3", "2", "true"),
        (Some(3), Some(2), Some(true))
    );
    assert_eq!(numbers_and_booleans("many", "", "yes"), (None, None, None));
}

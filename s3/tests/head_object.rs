// Model-output: Claude Opus 5.5

//! `head_object` against the fake S3, on every runtime and HTTP client: a malformed
//! `Content-Length` is never read as a length.

mod fake_s3;

use fake_s3::{bucket, http, reply, serve, within};
use s3::error::S3Error;
use s3::serde_types::HeadObjectResult;

/// HEADs `/object` on the fake S3 while it serves one 200 reply with `headers` and no body, in
/// a single attempt, so that an error is the reply's and not a retry's against the exited
/// server. Returns what `head_object` returned.
fn head_with(headers: &[(&str, &str)]) -> Result<HeadObjectResult, S3Error> {
    s3::set_retries(0);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (endpoint, server) = runtime.block_on(serve(vec![reply(http("200 OK", headers, b""))]));
    let bucket = bucket(&endpoint);
    #[cfg(feature = "with-tokio")]
    let result = runtime.block_on(within(bucket.head_object("/object")));
    // surf's hyper client runs on tokio 0.2, which only async-std's executor provides.
    #[cfg(feature = "with-async-std")]
    let result = async_std::task::block_on(bucket.head_object("/object"));
    #[cfg(feature = "sync")]
    let result = bucket.head_object("/object");
    runtime.block_on(within(server)).unwrap();
    result.map(|(head, _)| head)
}

/// Whether this build's HTTP client is hyper: reqwest's, or surf's when no async-h1 client,
/// which surf would prefer, is on.
const HYPER: bool = cfg!(any(
    feature = "with-tokio",
    all(
        feature = "with-async-std-hyper",
        not(any(
            feature = "async-std-native-tls",
            feature = "async-std-rustls-tls"
        ))
    )
));

/// hyper hands a HEAD's `Content-Length` over unchecked, since a HEAD has no body to frame;
/// attohttpc and async-h1 refuse the reply.
#[test]
fn malformed_content_length_is_absent() {
    let result = head_with(&[("content-length", "12x")]);

    if HYPER {
        assert_eq!(result.unwrap().content_length, None);
    } else {
        assert!(result.is_err(), "{result:?}");
    }
}

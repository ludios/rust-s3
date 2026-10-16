// Model-output: Claude Fable 5.1
// Model-output: Claude Opus 5.5

//! Observation of the requests the tokio backend hands to reqwest.
//!
//! An application installs one [`RequestObserver`] for the whole process with
//! [`set_request_observer`]. The backend then reports each request as it is sent, the response
//! headers or the send error that came back, and, through the [`BodyObserver`] the observer may
//! return, the bytes of the response body as they are read. This is meant for request logging:
//! the observer sees what was actually sent, signed headers and retries included.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::bucket::Bucket;
use crate::error::S3Error;

/// Sees every request the tokio backend hands to reqwest. Process-wide, set once.
///
/// Callbacks run inline on the requesting task: keep them short and do not panic in them,
/// a panic unwinds the S3 operation. Observation is at the reqwest boundary: reqwest adds
/// its default headers after `on_request`, redirects and reqwest-internal retries happen
/// inside one attempt, a request cancelled before its headers arrive gets no `on_response`,
/// and a request that fails to build or sign is never seen.
pub trait RequestObserver: Send + Sync + 'static {
    /// Called just before `request` is handed to reqwest. `op` identifies one
    /// `ReqwestRequest` and `attempt` counts its calls to `Client::execute` from 1, so a
    /// retry shows up as the same `op` with `attempt: 2`. Multipart parts are separate
    /// `ReqwestRequest`s and so separate `op`s; the `uploadId` in the URL ties them.
    fn on_request(&self, op: u64, attempt: u32, bucket: &Bucket, request: &reqwest::Request);

    /// Called when the response headers arrived, or when the attempt failed before that:
    /// sending failed, or the bucket's `request_timeout` passed. `elapsed` is measured
    /// around `Client::execute`, i.e. time to headers.
    /// Return `Some` to have the backend tee the response body into it; the return value
    /// is ignored for `Err`.
    fn on_response(
        &self,
        op: u64,
        attempt: u32,
        elapsed: Duration,
        result: Result<&reqwest::Response, &S3Error>,
    ) -> Option<Box<dyn BodyObserver>>;
}

/// Receives the bytes of one response body as the backend reads them.
pub trait BodyObserver: Send + 'static {
    /// Called with each chunk, in order, before the chunk is handed to the reader.
    fn chunk(&mut self, bytes: &[u8]);

    /// Called at most once. Not called at all when the backend discards the response
    /// without reading its body (HEAD, `object_exists`, ETag-only PUTs, and the raw
    /// response of `Request::response`). Under `fail-on-err` those still read a non-2xx
    /// body into the error, a 404 from `object_exists` aside, and so do get `end`.
    fn end(self: Box<Self>, end: BodyEnd<'_>);
}

/// How the reading of a response body stopped.
#[derive(Debug)]
pub enum BodyEnd<'a> {
    /// The body was read to its end.
    Eof,
    /// A read of the body failed: reqwest returned an error, whose Display is often just
    /// "error decoding response body" with the cause in the `source()` chain, or the
    /// bucket's `request_timeout` passed without a chunk, an `S3Error::Io` of kind
    /// `TimedOut`.
    Error(&'a S3Error),
    /// The stream was dropped before `Eof` or `Error` was seen. A consumer that stops after
    /// the last chunk without polling on to `None` lands here too, so this means "not known
    /// to be complete", not "bytes were missing".
    Dropped,
}

static REQUEST_OBSERVER: OnceLock<Arc<dyn RequestObserver>> = OnceLock::new();

/// Installs `observer` as the process-wide request observer. Only the first call succeeds;
/// a later one gets its observer handed back.
///
/// # Example
///
/// ```rust
/// use s3::{BodyObserver, Bucket, RequestObserver};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// struct StderrLog;
///
/// impl RequestObserver for StderrLog {
///     fn on_request(&self, op: u64, attempt: u32, _: &Bucket, request: &s3::reqwest::Request) {
///         eprintln!("op {op} attempt {attempt}: {} {}", request.method(), request.url());
///     }
///
///     fn on_response(
///         &self,
///         op: u64,
///         attempt: u32,
///         elapsed: Duration,
///         result: Result<&s3::reqwest::Response, &s3::error::S3Error>,
///     ) -> Option<Box<dyn BodyObserver>> {
///         match result {
///             Ok(response) => eprintln!("op {op} attempt {attempt}: {} after {elapsed:?}", response.status()),
///             Err(error) => eprintln!("op {op} attempt {attempt}: {error} after {elapsed:?}"),
///         }
///         None
///     }
/// }
///
/// assert!(s3::set_request_observer(Arc::new(StderrLog)).is_ok());
/// ```
pub fn set_request_observer(
    observer: Arc<dyn RequestObserver>,
) -> Result<(), Arc<dyn RequestObserver>> {
    REQUEST_OBSERVER.set(observer)
}

/// The installed request observer, if any.
pub(crate) fn request_observer() -> Option<&'static dyn RequestObserver> {
    REQUEST_OBSERVER.get().map(|observer| &**observer)
}

// Model-output: Claude Fable 5.1

extern crate base64;
extern crate md5;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use maybe_async::maybe_async;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Instant;
use time::OffsetDateTime;

use super::request_trait::{Request, ResponseData, ResponseDataStream};
use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::error::S3Error;
use crate::observer::{BodyEnd, BodyObserver, request_observer};
use crate::retry;
use crate::utils::now_utc;

use tokio_stream::StreamExt;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClientOptions {
    pub request_timeout: Option<std::time::Duration>,
    pub proxy: Option<reqwest::Proxy>,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_certs: bool,
    #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))]
    pub accept_invalid_hostnames: bool,
}

#[cfg(feature = "with-tokio")]
pub(crate) fn client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
    let client = reqwest::Client::builder();

    let client = if let Some(timeout) = options.request_timeout {
        client.timeout(timeout)
    } else {
        client
    };

    let client = if let Some(ref proxy) = options.proxy {
        client.proxy(proxy.clone())
    } else {
        client
    };

    cfg_if::cfg_if! {
        if #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))] {
            let client = client.danger_accept_invalid_certs(options.accept_invalid_certs);
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(any(feature = "tokio-native-tls", feature = "tokio-rustls-tls"))] {
            let client = client.danger_accept_invalid_hostnames(options.accept_invalid_hostnames);
        }
    }

    Ok(client.build()?)
}

/// Hands out the `op` numbers that tell a `RequestObserver` one request's attempts apart from
/// another's. Starts at 1 so that 0 never means a real request.
static NEXT_OP: AtomicU64 = AtomicU64::new(1);

// Temporary structure for making a request
pub struct ReqwestRequest<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
    /// Identifies this request to the `RequestObserver` across all of its attempts.
    op: u64,
    /// How many times this request has been handed to reqwest so far.
    attempts: AtomicU32,
}

/// A response whose headers have arrived, with the observer that is to see its body.
type ObservedResponse = (reqwest::Response, Option<Box<dyn BodyObserver>>);

/// The body stream of one response, teed into its `BodyObserver`. Every body the backend reads
/// goes through this, whether or not an observer is set, so there is one code path.
///
/// The observer is taken out of its `Option` before `end` is called, which is what makes `end`
/// run at most once, `Drop` included.
struct ObservedBody {
    stream: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    observer: Option<Box<dyn BodyObserver>>,
}

impl ObservedBody {
    fn new(response: reqwest::Response, observer: Option<Box<dyn BodyObserver>>) -> Self {
        Self {
            stream: Box::pin(response.bytes_stream()),
            observer,
        }
    }
}

impl Stream for ObservedBody {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(self.stream.as_mut().poll_next(cx));
        match &item {
            Some(Ok(bytes)) => {
                if let Some(observer) = &mut self.observer {
                    observer.chunk(bytes);
                }
            }
            Some(Err(error)) => {
                if let Some(observer) = self.observer.take() {
                    observer.end(BodyEnd::Error(error));
                }
            }
            None => {
                if let Some(observer) = self.observer.take() {
                    observer.end(BodyEnd::Eof);
                }
            }
        }
        Poll::Ready(item)
    }
}

impl Drop for ObservedBody {
    /// Reports a body that was let go before its end was seen. Never polls or drains: the
    /// consumer's decision to stop reading is what is being reported.
    fn drop(&mut self) {
        if let Some(observer) = self.observer.take() {
            observer.end(BodyEnd::Dropped);
        }
    }
}

/// Reads `body` to its end. Unlike `Response::bytes`, a read that fails midway has already
/// handed the observer the bytes that did arrive.
async fn collect(mut body: ObservedBody) -> Result<Vec<u8>, S3Error> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

/// The error `fail-on-err` reports for a non-2xx response: its status and its body, decoded
/// lossily as `Response::text` does. A body that cannot be read yields that read's error instead.
async fn http_fail_with_body(
    response: reqwest::Response,
    observer: Option<Box<dyn BodyObserver>>,
) -> S3Error {
    let status = response.status().as_u16();
    match collect(ObservedBody::new(response, observer)).await {
        Ok(body) => S3Error::HttpFailWithBody(status, String::from_utf8_lossy(&body).into_owned()),
        Err(error) => error,
    }
}

impl<'a> ReqwestRequest<'a> {
    pub async fn new(
        bucket: &'a Bucket,
        path: &'a str,
        command: Command<'a>,
    ) -> Result<ReqwestRequest<'a>, S3Error> {
        bucket.credentials_refresh().await?;
        Ok(Self {
            bucket,
            path,
            command,
            datetime: now_utc(),
            sync: false,
            op: NEXT_OP.fetch_add(1, Ordering::Relaxed),
            attempts: AtomicU32::new(0),
        })
    }

    /// Builds the signed request and sends it once, without retrying or looking at the
    /// status. Returns as soon as the response headers have arrived; the body is unread.
    /// The installed `RequestObserver`, if any, sees the attempt and may attach a body
    /// observer to the response.
    async fn execute(&self) -> Result<ObservedResponse, S3Error> {
        let headers = self
            .headers()
            .await?
            .iter()
            .map(|(k, v)| {
                (
                    reqwest::header::HeaderName::from_str(k.as_str()),
                    reqwest::header::HeaderValue::from_str(v.to_str().unwrap_or_default()),
                )
            })
            .filter(|(k, v)| k.is_ok() && v.is_ok())
            .map(|(k, v)| (k.unwrap(), v.unwrap()))
            .collect();

        let client = self.bucket.http_client();

        let method = match self.command.http_verb() {
            HttpMethod::Delete => reqwest::Method::DELETE,
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Head => reqwest::Method::HEAD,
        };

        let request = client
            .request(method, self.url()?.as_str())
            .headers(headers)
            .body(self.request_body()?)
            .build()?;

        // A request that failed to build above consumed no attempt number. The observer is
        // read once so that both callbacks of an attempt go to the same one.
        let observer = request_observer();
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(observer) = observer {
            observer.on_request(self.op, attempt, self.bucket, &request);
        }
        let started = Instant::now();
        let result = client.execute(request).await;
        let body_observer = observer.and_then(|observer| {
            observer.on_response(self.op, attempt, started.elapsed(), result.as_ref())
        });
        Ok((result?, body_observer))
    }

    /// `execute` plus the `fail-on-err` policy: a non-2xx response becomes an error carrying
    /// the body, which the body observer sees being read.
    async fn response_with_observer(&self) -> Result<ObservedResponse, S3Error> {
        let (response, observer) = self.execute().await?;

        if cfg!(feature = "fail-on-err") && !response.status().is_success() {
            return Err(http_fail_with_body(response, observer).await);
        }

        Ok((response, observer))
    }
}

#[maybe_async]
impl<'a> Request for ReqwestRequest<'a> {
    type Response = reqwest::Response;
    type HeaderMap = reqwest::header::HeaderMap;

    /// The raw response. Whatever the caller reads of its body goes unobserved.
    async fn response(&self) -> Result<Self::Response, S3Error> {
        let (response, _) = self.response_with_observer().await?;
        Ok(response)
    }

    async fn response_status(&self) -> Result<u16, S3Error> {
        retry! {
            async {
                let (response, observer) = self.execute().await?;
                let status = response.status().as_u16();

                if status == 404 {
                    return Ok(status);
                }

                if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                    return Err(http_fail_with_body(response, observer).await);
                }

                Ok(status)
            }.await
        }
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let (response, observer) = retry! {self.response_with_observer().await }?;
        let status_code = response.status().as_u16();
        let mut headers = response.headers().clone();
        let response_headers = headers
            .clone()
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    v.to_str()
                        .unwrap_or("could-not-decode-header-value")
                        .to_string(),
                )
            })
            .collect::<HashMap<String, String>>();
        // When etag=true, we extract the ETag header and return it as the body.
        // This is used for PUT operations (regular puts, multipart chunks) where:
        // 1. S3 returns an empty or non-useful response body
        // 2. The ETag header contains the essential information we need
        // 3. The calling code expects to get the ETag via response_data.as_str()
        //
        // Note: This approach means we discard any actual response body when etag=true,
        // but for the operations that use this (PUTs), the body is typically empty
        // or contains redundant information already available in headers.
        //
        // TODO: Refactor this to properly return the response body and access ETag
        // from headers instead of replacing the body. This would be a breaking change.
        let body_vec = if etag {
            if let Some(etag) = headers.remove("ETag") {
                Bytes::from(etag.to_str()?.to_string())
            } else {
                Bytes::from("")
            }
        } else {
            Bytes::from(collect(ObservedBody::new(response, observer)).await?)
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        use tokio::io::AsyncWriteExt;
        let (response, observer) = retry! {self.response_with_observer().await}?;

        let status_code = response.status();
        let mut stream = ObservedBody::new(response, observer);

        while let Some(item) = stream.next().await {
            writer.write_all(&item?).await?;
        }

        Ok(status_code.as_u16())
    }

    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error> {
        let (response, observer) = retry! {self.response_with_observer().await}?;
        let status_code = response.status();
        let stream = ObservedBody::new(response, observer).map_err(S3Error::Reqwest);

        Ok(ResponseDataStream {
            bytes: Box::pin(stream),
            status_code: status_code.as_u16(),
        })
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let (response, _) = retry! {self.response_with_observer().await}?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }

    fn datetime(&self) -> OffsetDateTime {
        self.datetime
    }

    fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }

    fn command(&self) -> Command<'_> {
        self.command.clone()
    }

    fn path(&self) -> String {
        self.path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request::Request;
    use crate::request::tokio_backend::ReqwestRequest;
    use awscreds::Credentials;
    use http::header::{HOST, RANGE};

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    #[tokio::test]
    async fn url_uses_https_by_default() {
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials()).unwrap();
        let path = "/my-first/path";
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_https_by_default_path_style() {
        let region = "custom-region".parse().unwrap();
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-first/path";
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "https");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();

        assert_eq!(*host, "custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_scheme_from_custom_region_if_defined() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials()).unwrap();
        let path = "/my-second/path";
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
    }

    #[tokio::test]
    async fn url_uses_scheme_from_custom_region_if_defined_with_path_style() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-second/path";
        let request = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();

        assert_eq!(request.url().unwrap().scheme(), "http");

        let headers = request.headers().await.unwrap();
        let host = headers.get(HOST).unwrap();
        assert_eq!(*host, "custom-region".to_string());
    }

    #[tokio::test]
    async fn test_get_object_range_header() {
        let region = "http://custom-region".parse().unwrap();
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())
            .unwrap()
            .with_path_style();
        let path = "/my-second/path";

        let request = ReqwestRequest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: None,
            },
        )
        .await
        .unwrap();
        let headers = request.headers().await.unwrap();
        let range = headers.get(RANGE).unwrap();
        assert_eq!(range, "bytes=0-");

        let request = ReqwestRequest::new(
            &bucket,
            path,
            Command::GetObjectRange {
                start: 0,
                end: Some(1),
            },
        )
        .await
        .unwrap();
        let headers = request.headers().await.unwrap();
        let range = headers.get(RANGE).unwrap();
        assert_eq!(range, "bytes=0-1");
    }
}

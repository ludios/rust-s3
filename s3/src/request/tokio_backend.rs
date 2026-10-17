// Model-output: Claude Fable 5.1
// Model-output: Claude Opus 5.5

extern crate base64;
extern crate md5;

use bytes::Bytes;
use futures_util::Stream;
use maybe_async::maybe_async;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::time::Sleep;

use super::request_trait::{Request, ResponseData, ResponseDataStream};
use crate::bucket::Bucket;
use crate::command::Command;
use crate::command::HttpMethod;
use crate::error::S3Error;
use crate::observer::{BodyEnd, BodyObserver, request_observer};
use crate::request::{RequestAttemptError, timeout_error};
use crate::utils::now_utc;

use tokio_stream::StreamExt;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClientOptions {
    pub proxy: Option<reqwest::Proxy>,
    #[cfg(any(
        feature = "tokio-native-tls",
        feature = "tokio-rustls-tls",
        feature = "tokio-rustls-tls-ring"
    ))]
    pub accept_invalid_certs: bool,
    #[cfg(any(
        feature = "tokio-native-tls",
        feature = "tokio-rustls-tls",
        feature = "tokio-rustls-tls-ring"
    ))]
    pub accept_invalid_hostnames: bool,
}

#[cfg(feature = "with-tokio")]
pub(crate) fn client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
    // Request timeouts are applied to individual requests so changing a
    // Bucket's timeout does not require rebuilding this client (or lose its
    // proxy/TLS configuration).
    //
    // An object's Content-Encoding is metadata about its stored bytes, which are what a
    // download must return, so the client neither asks for a compressed transfer nor decodes
    // one. Each of these is a no-op unless some crate in the build enables the matching
    // reqwest feature, which would otherwise switch both on here.
    let client = reqwest::Client::builder()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate();

    let client = if let Some(ref proxy) = options.proxy {
        client.proxy(proxy.clone())
    } else {
        client
    };

    // With both TLS features, reqwest uses native TLS, as it always has. rustls alone gets a
    // configuration of its own, which also keeps it on rustls if another crate in the build
    // switches on reqwest's native TLS.
    cfg_if::cfg_if! {
        if #[cfg(feature = "tokio-native-tls")] {
            let client = client
                .tls_danger_accept_invalid_certs(options.accept_invalid_certs)
                .tls_danger_accept_invalid_hostnames(options.accept_invalid_hostnames);
        } else if #[cfg(feature = "tokio-rustls-tls")] {
            let client = client.tls_backend_preconfigured(super::rustls_tls::client_config(
                options.accept_invalid_certs,
                options.accept_invalid_hostnames,
            ));
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

/// A response whose headers have arrived, with the observer that is to see its body and the
/// bucket's `request_timeout`, which bounds each wait for a chunk of that body.
struct ObservedResponse {
    response: reqwest::Response,
    observer: Option<Box<dyn BodyObserver>>,
    idle_limit: Option<Duration>,
}

impl ObservedResponse {
    /// The body, with its idle timer created here, inside the runtime, so that the stream may
    /// then be polled from anywhere.
    fn body(self) -> ObservedBody {
        ObservedBody {
            stream: Box::pin(self.response.bytes_stream()),
            observer: self.observer,
            idle: self
                .idle_limit
                .map(|limit| (limit, Box::pin(tokio::time::sleep(limit)))),
            idle_armed: false,
        }
    }
}

/// `limit` from now as a timer deadline, capped as tokio caps `sleep` for a limit too far
/// ahead to represent.
fn idle_deadline(limit: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(limit)
        .unwrap_or(now + Duration::from_secs(86400 * 365 * 30))
}

/// The body stream of one response, teed into its `BodyObserver` and bounded by the bucket's
/// `request_timeout`. Every body the backend reads goes through this, whether or not an
/// observer is set, so there is one code path.
///
/// The observer is taken out of its `Option` before `end` is called, which is what makes `end`
/// run at most once, `Drop` included.
struct ObservedBody {
    stream: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    observer: Option<Box<dyn BodyObserver>>,
    /// How long one wait for the next chunk may last, and the timer that enforces it, if
    /// bounded at all. Armed by a poll that found no chunk, disarmed by the next chunk or by
    /// firing, so that a poll after a timeout waits a fresh limit. A body that keeps arriving
    /// never times out, however long it is.
    idle: Option<(Duration, Pin<Box<Sleep>>)>,
    idle_armed: bool,
}

impl Stream for ObservedBody {
    type Item = Result<Bytes, S3Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        // The stream is polled first, so a chunk that is ready goes out even if the timer
        // lapsed while the consumer was busy with something else.
        let item = match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(item) => {
                this.idle_armed = false;
                item.map(|chunk| chunk.map_err(S3Error::Reqwest))
            }
            Poll::Pending => {
                let Some((limit, timer)) = &mut this.idle else {
                    return Poll::Pending;
                };
                if !this.idle_armed {
                    timer.as_mut().reset(idle_deadline(*limit));
                    this.idle_armed = true;
                }
                ready!(timer.as_mut().poll(cx));
                this.idle_armed = false;
                Some(Err(S3Error::Io(timeout_error(
                    "response body data",
                    *limit,
                ))))
            }
        };
        match &item {
            Some(Ok(bytes)) => {
                if let Some(observer) = &mut this.observer {
                    observer.chunk(bytes);
                }
            }
            Some(Err(error)) => {
                if let Some(observer) = this.observer.take() {
                    observer.end(BodyEnd::Error(error));
                }
            }
            None => {
                if let Some(observer) = this.observer.take() {
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
/// handed the observer the bytes that did arrive. Like it, the result is allocated at its exact
/// size, and a body that arrived as one chunk is returned without a copy.
async fn collect(mut body: ObservedBody) -> Result<Bytes, S3Error> {
    let mut chunks = Vec::new();
    while let Some(chunk) = body.next().await {
        chunks.push(chunk?);
    }
    Ok(match chunks.len() {
        1 => chunks.swap_remove(0),
        _ => Bytes::from(chunks.concat()),
    })
}

/// The error `fail-on-err` reports for a non-2xx response: its status and its body, decoded
/// as UTF-8 lossily whatever its charset, retryable or not by status. A body that cannot be
/// read yields that read's error instead, which is never retried.
async fn http_fail_with_body(observed: ObservedResponse) -> RequestAttemptError {
    let status = observed.response.status().as_u16();
    match collect(observed.body()).await {
        Ok(body) => {
            S3Error::HttpFailWithBody(status, String::from_utf8_lossy(&body).into_owned()).into()
        }
        Err(error) => RequestAttemptError::do_not_retry(error),
    }
}

/// Builds the signed request and sends it once, without retrying or looking at the status.
/// Returns as soon as the response headers have arrived, or fails once the bucket's
/// `request_timeout` has passed without them; the body is unread.
/// The installed `RequestObserver`, if any, sees the attempt and may attach a body
/// observer to the response.
async fn execute(request: &ReqwestRequest<'_>) -> Result<ObservedResponse, RequestAttemptError> {
    let headers = request
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

    let client = request.bucket.http_client();
    let method = match request.command.http_verb() {
        HttpMethod::Delete => reqwest::Method::DELETE,
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Head => reqwest::Method::HEAD,
    };

    let built = client
        .request(method, request.url()?.as_str())
        .headers(headers)
        .body(request.request_body()?)
        .build()?;

    // A request that failed to build above consumed no attempt number. The observer is
    // read once so that both callbacks of an attempt go to the same one.
    let observer = request_observer();
    let attempt = request.attempts.fetch_add(1, Ordering::Relaxed) + 1;
    if let Some(observer) = observer {
        observer.on_request(request.op, attempt, request.bucket, &built);
    }
    let started = Instant::now();
    let idle_limit = request.bucket.request_timeout;
    let result = match idle_limit {
        None => client
            .execute(built)
            .await
            .map_err(RequestAttemptError::from),
        Some(limit) => match tokio::time::timeout(limit, client.execute(built)).await {
            Ok(result) => result.map_err(RequestAttemptError::from),
            Err(_) => Err(RequestAttemptError::transient(S3Error::Io(timeout_error(
                "response headers",
                limit,
            )))),
        },
    };
    let body_observer = observer.and_then(|observer| {
        observer.on_response(
            request.op,
            attempt,
            started.elapsed(),
            result.as_ref().map_err(|error| &error.error),
        )
    });
    Ok(ObservedResponse {
        response: result?,
        observer: body_observer,
        idle_limit,
    })
}

/// `execute` plus the `fail-on-err` policy: a non-2xx response becomes an error carrying
/// the body, which the body observer sees being read.
async fn request_response_attempt(
    request: &ReqwestRequest<'_>,
) -> Result<ObservedResponse, RequestAttemptError> {
    let observed = execute(request).await?;

    if cfg!(feature = "fail-on-err") && !observed.response.status().is_success() {
        return Err(http_fail_with_body(observed).await);
    }

    Ok(observed)
}

#[maybe_async]
impl<'a> Request for ReqwestRequest<'a> {
    type Response = reqwest::Response;
    type HeaderMap = reqwest::header::HeaderMap;

    /// The raw response. Whatever the caller reads of its body goes unobserved and is not
    /// bounded by the bucket's `request_timeout`.
    async fn response(&self) -> Result<Self::Response, S3Error> {
        request_response_attempt(self)
            .await
            .map(|observed| observed.response)
            .map_err(|error| error.error)
    }

    async fn response_status(&self) -> Result<u16, S3Error> {
        crate::request::retry_request(&self.command, || async {
            let observed = execute(self).await?;
            let status = observed.response.status().as_u16();

            if status == 404 {
                return Ok(status);
            }

            if cfg!(feature = "fail-on-err") && !observed.response.status().is_success() {
                return Err(http_fail_with_body(observed).await);
            }

            Ok(status)
        })
        .await
    }

    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let observed =
            crate::request::retry_request(&self.command, || request_response_attempt(self)).await?;
        let status_code = observed.response.status().as_u16();
        let mut headers = observed.response.headers().clone();
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
            collect(observed.body()).await?
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error> {
        use tokio::io::AsyncWriteExt;
        let observed =
            crate::request::retry_request(&self.command, || request_response_attempt(self)).await?;

        let status_code = observed.response.status();
        let mut stream = observed.body();

        while let Some(item) = stream.next().await {
            writer.write_all(&item?).await?;
        }

        Ok(status_code.as_u16())
    }

    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error> {
        let observed =
            crate::request::retry_request(&self.command, || request_response_attempt(self)).await?;
        let status_code = observed.response.status().as_u16();
        let headers = observed.response.headers().clone();

        Ok(ResponseDataStream::new(
            Box::pin(observed.body()),
            status_code,
            headers,
        ))
    }

    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let observed =
            crate::request::retry_request(&self.command, || request_response_attempt(self)).await?;
        let status_code = observed.response.status().as_u16();
        let headers = observed.response.headers().clone();
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
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::request::Request;
    use crate::request::tokio_backend::ReqwestRequest;
    use awscreds::Credentials;
    use http::header::{HOST, RANGE};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    fn timeout_test_bucket(endpoint: String, timeout: Option<Duration>) -> Bucket {
        let mut bucket = *Bucket::new(
            "timeout-test",
            crate::region::Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint,
            },
            fake_credentials(),
        )
        .unwrap()
        .with_path_style();
        bucket.set_request_timeout(timeout);
        bucket
    }

    fn delayed_http_response(
        response: &'static [u8],
        delay: Duration,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        mock_http_response(vec![(delay, response)])
    }

    fn stalled_http_body(
        initial_response: &'static [u8],
        trailing_body: &'static [u8],
        delay: Duration,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        mock_http_response(vec![
            (Duration::ZERO, initial_response),
            (delay, trailing_body),
        ])
    }

    /// Serves one request, answering with `script`: each step waits out its delay, which a
    /// message on the returned channel cuts short to end the server, then writes its bytes.
    fn mock_http_response(
        script: Vec<(Duration, &'static [u8])>,
    ) -> (String, mpsc::Sender<()>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let accept_deadline = std::time::Instant::now() + Duration::from_secs(8);
            let (mut stream, _) = loop {
                if !matches!(stop_rx.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                    return;
                }
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= accept_deadline {
                            return;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return,
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).unwrap_or(0) == 0 {
                    return;
                }
                request.push(byte[0]);
                assert!(
                    request.len() < 16 * 1024,
                    "request header exceeded fixture bound"
                );
            }
            for (delay, bytes) in script {
                if !matches!(
                    stop_rx.recv_timeout(delay),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    return;
                }
                if stream.write_all(bytes).is_err() {
                    return;
                }
            }
            let _ = stream.flush();
        });
        (endpoint, stop_tx, server)
    }

    fn timeout_then_success_status_server() -> (String, mpsc::Sender<()>, thread::JoinHandle<usize>)
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop_tx, stop_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut attempts = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(8);
            let Some(mut first) = accept_mock_request(&listener, &stop_rx, deadline) else {
                return attempts;
            };
            attempts += 1;
            if !read_mock_request(&mut first) {
                return attempts;
            }
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let first_response_deadline = std::time::Instant::now() + Duration::from_secs(3);
            loop {
                if !matches!(stop_rx.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                    return attempts;
                }
                match listener.accept() {
                    Ok((mut second, _)) => {
                        let _ = second.set_nonblocking(false);
                        let _ = second.set_read_timeout(Some(Duration::from_secs(2)));
                        let _ = second.set_write_timeout(Some(Duration::from_secs(2)));
                        attempts += 1;
                        if read_mock_request(&mut second) {
                            let _ = second.write_all(response);
                            let _ = second.flush();
                        }
                        return attempts;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= first_response_deadline {
                            let _ = first.write_all(response);
                            let _ = first.flush();
                            return attempts;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return attempts,
                }
            }
        });
        (endpoint, stop_tx, server)
    }

    fn accept_mock_request(
        listener: &TcpListener,
        stop: &mpsc::Receiver<()>,
        deadline: std::time::Instant,
    ) -> Option<std::net::TcpStream> {
        loop {
            if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty))
                || std::time::Instant::now() >= deadline
            {
                return None;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).ok()?;
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                    return Some(stream);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return None,
            }
        }
    }

    fn read_mock_request(stream: &mut std::net::TcpStream) -> bool {
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte) {
                Ok(0) | Err(_) => return false,
                Ok(_) => request.push(byte[0]),
            }
            if request.len() >= 16 * 1024 {
                return false;
            }
        }
        true
    }

    #[tokio::test]
    async fn request_timeout_covers_delayed_response_headers() {
        let (endpoint, stop, server) = delayed_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), request.response()).await;
        let _ = stop.send(());
        server.join().unwrap();
        let result = result.expect("request should finish within the fixture bound");
        assert!(
            matches!(&result, Err(crate::error::S3Error::Io(error))
                if error.kind() == std::io::ErrorKind::TimedOut),
            "delayed headers should exceed the request timeout"
        );
    }

    #[tokio::test]
    async fn request_timeout_resets_with_each_body_chunk() {
        // Three gaps add up to more than the limit; each alone is within it.
        let gap = Duration::from_millis(400);
        let (endpoint, stop, server) = mock_http_response(vec![
            (
                Duration::ZERO,
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n",
            ),
            (gap, b"ab"),
            (gap, b"cd"),
            (gap, b"ef"),
        ]);
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_secs(1)));

        let response =
            tokio::time::timeout(Duration::from_secs(5), bucket.get_object("/object")).await;
        let _ = stop.send(());
        server.join().unwrap();
        let response = response
            .expect("download should finish within the fixture bound")
            .expect("a body that keeps arriving must not time out");
        assert_eq!(response.as_slice(), b"abcdef");
    }

    #[tokio::test]
    async fn request_timeout_covers_body_stream_and_writer_consumption() {
        let (endpoint, stop, server) = stalled_http_body(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
            b"def",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let mut body_stream = bucket.get_object_stream("/object").await.unwrap();
        let mut prefix = Vec::new();
        while prefix.len() < 3 {
            let chunk = tokio::time::timeout(Duration::from_secs(1), body_stream.bytes.next())
                .await
                .expect("first body bytes should arrive promptly")
                .expect("response body should contain its initial bytes")
                .unwrap();
            prefix.extend_from_slice(&chunk);
        }
        assert_eq!(&prefix[..3], b"abc");
        // Each poll of the stalled body waits out the limit; a poll after a timeout does the
        // same again rather than failing at once.
        for _ in 0..2 {
            let started = std::time::Instant::now();
            let next = tokio::time::timeout(Duration::from_secs(2), body_stream.bytes.next())
                .await
                .expect("body timeout should be bounded");
            assert!(matches!(
                next.expect("body should report its timeout"),
                Err(crate::error::S3Error::Io(ref error))
                    if error.kind() == std::io::ErrorKind::TimedOut
            ));
            assert!(started.elapsed() >= Duration::from_millis(400));
        }
        let _ = stop.send(());
        server.join().unwrap();

        let (endpoint, stop, server) = stalled_http_body(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
            b"def",
            Duration::from_millis(1500),
        );
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let mut writer = Vec::new();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            bucket.get_object_to_writer("/object", &mut writer),
        )
        .await;
        let _ = stop.send(());
        server.join().unwrap();
        let result = result.expect("writer timeout should be bounded");
        assert!(matches!(
            result,
            Err(crate::error::S3Error::Io(ref error))
                if error.kind() == std::io::ErrorKind::TimedOut
        ));
        assert_eq!(writer, b"abc", "the complete prefix should be written once");
    }

    #[tokio::test]
    async fn request_timeout_can_be_removed_after_bucket_creation() {
        let (endpoint, stop, server) = delayed_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            Duration::from_millis(1500),
        );
        let mut bucket = *timeout_test_bucket(endpoint, None)
            .with_request_timeout(Duration::from_millis(500))
            .unwrap();
        bucket.set_request_timeout(None);
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), request.response()).await;
        let _ = stop.send(());
        server.join().unwrap();
        let response = response
            .expect("request should finish within the fixture bound")
            .expect("None should remove the prior per-request timeout");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn response_status_applies_timeout_and_retries_the_request() {
        let (endpoint, stop, server) = timeout_then_success_status_server();
        let bucket = timeout_test_bucket(endpoint, Some(Duration::from_millis(500)));
        let request = ReqwestRequest::new(&bucket, "/object", Command::GetObject)
            .await
            .unwrap();

        let status = tokio::time::timeout(Duration::from_secs(5), request.response_status()).await;
        let _ = stop.send(());
        let attempts = server.join().unwrap();
        let status = status
            .expect("status retry should remain bounded")
            .expect("the second response should succeed");
        assert_eq!(status, 200);
        assert_eq!(attempts, 2);
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
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));
        assert_eq!(headers.get("Accept").unwrap(), "application/octet-stream");
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("range"));
        assert!(!signed_headers.contains("content-length"));
        assert!(!signed_headers.contains("content-type"));

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
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("range"));
        assert!(!signed_headers.contains("content-length"));
        assert!(!signed_headers.contains("content-type"));

        let get = ReqwestRequest::new(&bucket, path, Command::GetObject)
            .await
            .unwrap();
        let headers = get.headers().await.unwrap();
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));

        let put = ReqwestRequest::new(
            &bucket,
            path,
            Command::PutObject {
                content: b"abc",
                content_type: "application/test",
                custom_headers: None,
                multipart: None,
            },
        )
        .await
        .unwrap();
        let headers = put.headers().await.unwrap();
        assert_eq!(headers.get("Content-Length").unwrap(), "3");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/test");
        let authorization = headers.get("Authorization").unwrap().to_str().unwrap();
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("content-length"));
        assert!(signed_headers.contains("content-type"));
    }
}

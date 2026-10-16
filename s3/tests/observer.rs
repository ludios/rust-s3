// Model-output: Claude Fable 5.1

//! Exercises the `RequestObserver` hook end to end: public `Bucket` methods against the fake S3
//! of `fake_s3`, with one recording observer installed.
//!
//! This is its own test binary because the observer is process-global and cannot be
//! uninstalled; the scenarios share it and the event log, so they run one at a time.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, Reply, bucket, http, reply, serve, within};
use futures_util::StreamExt;
use s3::error::S3Error;
use s3::{BodyEnd, BodyObserver, Bucket, RequestObserver};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;

const XML_ERROR: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>";

/// What the recording observer saw, in the order it saw it. The chunks of a body are folded
/// into its `Body` event so that assertions do not depend on how TCP split the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Request {
        op: u64,
        attempt: u32,
        bucket: String,
        method: String,
        url: String,
    },
    Response {
        op: u64,
        attempt: u32,
        status: u16,
    },
    SendError {
        op: u64,
        attempt: u32,
        error: String,
    },
    Body {
        op: u64,
        attempt: u32,
        bytes: Vec<u8>,
        end: End,
    },
}

/// `BodyEnd` with the borrowed error replaced by its Display.
#[derive(Debug, Clone, PartialEq, Eq)]
enum End {
    Eof,
    Error(String),
    Dropped,
}

static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

fn record(event: Event) {
    EVENTS.lock().unwrap().push(event);
}

/// Takes everything recorded so far.
fn take_events() -> Vec<Event> {
    std::mem::take(&mut *EVENTS.lock().unwrap())
}

struct Recorder;

impl RequestObserver for Recorder {
    fn on_request(&self, op: u64, attempt: u32, bucket: &Bucket, request: &s3::reqwest::Request) {
        record(Event::Request {
            op,
            attempt,
            bucket: bucket.name(),
            method: request.method().to_string(),
            url: request.url().to_string(),
        });
    }

    /// Returns a body recorder for send errors too, to check that the backend ignores it.
    fn on_response(
        &self,
        op: u64,
        attempt: u32,
        _elapsed: Duration,
        result: Result<&s3::reqwest::Response, &s3::reqwest::Error>,
    ) -> Option<Box<dyn BodyObserver>> {
        match result {
            Ok(response) => record(Event::Response {
                op,
                attempt,
                status: response.status().as_u16(),
            }),
            Err(error) => record(Event::SendError {
                op,
                attempt,
                error: error.to_string(),
            }),
        }
        Some(Box::new(BodyRecorder {
            op,
            attempt,
            bytes: Vec::new(),
        }))
    }
}

struct BodyRecorder {
    op: u64,
    attempt: u32,
    bytes: Vec<u8>,
}

impl BodyObserver for BodyRecorder {
    fn chunk(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn end(self: Box<Self>, end: BodyEnd<'_>) {
        let end = match end {
            BodyEnd::Eof => End::Eof,
            BodyEnd::Error(error) => End::Error(error.to_string()),
            BodyEnd::Dropped => End::Dropped,
        };
        record(Event::Body {
            op: self.op,
            attempt: self.attempt,
            bytes: self.bytes,
            end,
        });
    }
}

/// Serialises the scenarios, which share the process-wide observer and the event log.
static SCENARIO: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The `op` of the previous scenario's request, to check that each request gets a new one.
static PREVIOUS_OP: AtomicU64 = AtomicU64::new(0);

/// Starts a scenario: installs the recorder (the first caller does), takes the scenario lock,
/// and drops any events left over from a failed scenario.
async fn begin() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SCENARIO.lock().await;
    let _ = s3::set_request_observer(Arc::new(Recorder));
    take_events();
    guard
}

/// The `op` shared by all of `events`, which must be a fresh one. A scenario makes one request,
/// so every event it records carries the same `op`.
fn op_of(events: &[Event]) -> u64 {
    let ops: Vec<u64> = events
        .iter()
        .map(|event| match event {
            Event::Request { op, .. }
            | Event::Response { op, .. }
            | Event::SendError { op, .. }
            | Event::Body { op, .. } => *op,
        })
        .collect();
    let op = *ops.first().expect("no events were recorded");
    assert!(
        ops.iter().all(|&o| o == op),
        "events of several ops: {events:?}"
    );
    assert!(
        op > PREVIOUS_OP.swap(op, Ordering::Relaxed),
        "op {op} was used before"
    );
    op
}

fn request(op: u64, attempt: u32, method: &str, endpoint: &str, path: &str) -> Event {
    Event::Request {
        op,
        attempt,
        bucket: BUCKET.to_owned(),
        method: method.to_owned(),
        url: format!("{endpoint}/{BUCKET}{path}"),
    }
}

fn response(op: u64, attempt: u32, status: u16) -> Event {
    Event::Response {
        op,
        attempt,
        status,
    }
}

fn body(op: u64, attempt: u32, bytes: &[u8], end: End) -> Event {
    Event::Body {
        op,
        attempt,
        bytes: bytes.to_vec(),
        end,
    }
}

#[tokio::test]
async fn get_object_200_is_observed_in_order() {
    let _scenario = begin().await;
    let (endpoint, server) = serve(vec![reply(http("200 OK", &[], b"hello, world"))]).await;
    let bucket = bucket(&endpoint);

    let data = within(bucket.get_object("/hello.txt")).await.unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(data.status_code(), 200);
    assert_eq!(data.as_slice(), b"hello, world");
    assert!(
        heads[0].starts_with("GET /test-bucket/hello.txt HTTP/1.1\r\n"),
        "{}",
        heads[0]
    );
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "GET", &endpoint, "/hello.txt"),
            response(op, 1, 200),
            body(op, 1, b"hello, world", End::Eof),
        ]
    );
}

#[tokio::test]
async fn get_object_to_writer_observes_the_body_it_writes() {
    let _scenario = begin().await;
    let (endpoint, server) = serve(vec![reply(http("200 OK", &[], b"hello, world"))]).await;
    let bucket = bucket(&endpoint);

    let mut written = Vec::new();
    let status = within(bucket.get_object_to_writer("/hello.txt", &mut written))
        .await
        .unwrap();
    within(server).await.unwrap();

    assert_eq!(status, 200);
    assert_eq!(written, b"hello, world");
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "GET", &endpoint, "/hello.txt"),
            response(op, 1, 200),
            body(op, 1, b"hello, world", End::Eof),
        ]
    );
}

#[cfg(feature = "fail-on-err")]
#[tokio::test]
async fn get_object_500_is_retried_under_one_op_with_each_error_body_observed() {
    let _scenario = begin().await;
    let error = http(
        "500 Internal Server Error",
        &[("content-type", "application/xml")],
        XML_ERROR,
    );
    let (endpoint, server) = serve(vec![reply(error.clone()), reply(error)]).await;
    let bucket = bucket(&endpoint);

    let result = within(bucket.get_object("/hello.txt")).await;
    let heads = within(server).await.unwrap();

    assert_eq!(heads.len(), 2);
    match result {
        Err(S3Error::HttpFailWithBody(500, text)) => assert_eq!(text.as_bytes(), XML_ERROR),
        other => panic!("expected HttpFailWithBody(500, ..), got {other:?}"),
    }
    let events = take_events();
    let op = op_of(&events);
    let attempt = |attempt| {
        vec![
            request(op, attempt, "GET", &endpoint, "/hello.txt"),
            response(op, attempt, 500),
            body(op, attempt, XML_ERROR, End::Eof),
        ]
    };
    assert_eq!(events, [attempt(1), attempt(2)].concat());
}

#[tokio::test]
async fn get_object_body_cut_short_ends_in_error_without_retry() {
    let _scenario = begin().await;
    let cut_short = http("200 OK", &[("content-length", "10")], b"hello");
    let (endpoint, server) = serve(vec![reply(cut_short)]).await;
    let bucket = bucket(&endpoint);

    let result = within(bucket.get_object("/hello.txt")).await;
    let heads = within(server).await.unwrap();

    assert_eq!(heads.len(), 1);
    assert!(matches!(result, Err(S3Error::Reqwest(_))), "{result:?}");
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events[..2],
        [
            request(op, 1, "GET", &endpoint, "/hello.txt"),
            response(op, 1, 200),
        ]
    );
    match &events[2..] {
        [
            Event::Body {
                bytes,
                end: End::Error(_),
                ..
            },
        ] => assert_eq!(bytes, b"hello"),
        other => panic!("expected one body ending in an error, got {other:?}"),
    }
}

/// `object_exists` sends a HEAD, whose error response announces the length of an error
/// document it does not carry. The error-body read still happens and is observed: it is
/// simply empty.
#[cfg(feature = "fail-on-err")]
#[tokio::test]
async fn object_exists_500_is_retried_with_its_empty_error_body_observed_on_each_attempt() {
    let _scenario = begin().await;
    let xml_len = XML_ERROR.len().to_string();
    let error = http(
        "500 Internal Server Error",
        &[("content-length", &xml_len)],
        b"",
    );
    let (endpoint, server) = serve(vec![reply(error.clone()), reply(error)]).await;
    let bucket = bucket(&endpoint);

    let result = within(bucket.object_exists("/hello.txt")).await;
    let heads = within(server).await.unwrap();

    assert_eq!(heads.len(), 2);
    match result {
        Err(S3Error::HttpFailWithBody(500, text)) => assert_eq!(text, ""),
        other => panic!("expected HttpFailWithBody(500, ..), got {other:?}"),
    }
    let events = take_events();
    let op = op_of(&events);
    let attempt = |attempt| {
        vec![
            request(op, attempt, "HEAD", &endpoint, "/hello.txt"),
            response(op, attempt, 500),
            body(op, attempt, b"", End::Eof),
        ]
    };
    assert_eq!(events, [attempt(1), attempt(2)].concat());
}

#[tokio::test]
async fn object_exists_404_is_one_attempt_with_no_body_events() {
    let _scenario = begin().await;
    let (endpoint, server) = serve(vec![reply(http("404 Not Found", &[], b""))]).await;
    let bucket = bucket(&endpoint);

    let exists = within(bucket.object_exists("/missing.txt")).await.unwrap();
    let heads = within(server).await.unwrap();

    assert!(!exists);
    assert_eq!(heads.len(), 1);
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "HEAD", &endpoint, "/missing.txt"),
            response(op, 1, 404),
        ]
    );
}

#[tokio::test]
async fn head_object_with_a_content_length_has_no_body_events() {
    let _scenario = begin().await;
    let head = http(
        "200 OK",
        &[("content-length", "1234"), ("etag", "\"abc\"")],
        b"",
    );
    let (endpoint, server) = serve(vec![reply(head)]).await;
    let bucket = bucket(&endpoint);

    let (result, status) = within(bucket.head_object("/hello.txt")).await.unwrap();
    within(server).await.unwrap();

    assert_eq!(status, 200);
    assert_eq!(result.content_length, Some(1234));
    assert_eq!(result.e_tag.as_deref(), Some("\"abc\""));
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "HEAD", &endpoint, "/hello.txt"),
            response(op, 1, 200),
        ]
    );
}

#[tokio::test]
async fn put_object_returns_the_etag_and_leaves_the_body_unobserved() {
    let _scenario = begin().await;
    let put = http("200 OK", &[("etag", "\"d41d8cd9\"")], b"<not read/>");
    let (endpoint, server) = serve(vec![reply(put)]).await;
    let bucket = bucket(&endpoint);

    let data = within(bucket.put_object("/hello.txt", b"hello, world"))
        .await
        .unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(data.status_code(), 200);
    assert_eq!(data.as_str().unwrap(), "\"d41d8cd9\"");
    assert!(
        heads[0].starts_with("PUT /test-bucket/hello.txt HTTP/1.1\r\n"),
        "{}",
        heads[0]
    );
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "PUT", &endpoint, "/hello.txt"),
            response(op, 1, 200),
        ]
    );
}

#[tokio::test]
async fn a_stream_read_to_its_end_ends_once_with_eof() {
    let _scenario = begin().await;
    let (endpoint, server) = serve(vec![reply(http("200 OK", &[], b"hello, world"))]).await;
    let bucket = bucket(&endpoint);

    let mut stream = within(bucket.get_object_stream("/hello.txt"))
        .await
        .unwrap();
    let mut read = Vec::new();
    while let Some(chunk) = within(stream.bytes().next()).await {
        read.extend_from_slice(&chunk.unwrap());
    }
    drop(stream);
    within(server).await.unwrap();

    assert_eq!(read, b"hello, world");
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "GET", &endpoint, "/hello.txt"),
            response(op, 1, 200),
            body(op, 1, b"hello, world", End::Eof),
        ]
    );
}

#[tokio::test]
async fn a_stream_dropped_mid_body_ends_once_as_dropped() {
    let _scenario = begin().await;
    let half = Reply {
        bytes: http("200 OK", &[("content-length", "20")], b"first half"),
        hold: true,
    };
    let (endpoint, server) = serve(vec![half]).await;
    let bucket = bucket(&endpoint);

    let mut stream = within(bucket.get_object_stream("/hello.txt"))
        .await
        .unwrap();
    let chunk = within(stream.bytes().next()).await.unwrap().unwrap();
    drop(stream);
    within(server).await.unwrap();

    assert!(!chunk.is_empty());
    let events = take_events();
    let op = op_of(&events);
    assert_eq!(
        events,
        vec![
            request(op, 1, "GET", &endpoint, "/hello.txt"),
            response(op, 1, 200),
            body(op, 1, &chunk, End::Dropped),
        ]
    );
}

#[tokio::test]
async fn a_send_failure_is_reported_per_attempt_with_no_body_events() {
    let _scenario = begin().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let bucket = bucket(&endpoint);

    let result = within(bucket.get_object("/hello.txt")).await;

    assert!(matches!(result, Err(S3Error::Reqwest(_))), "{result:?}");
    let events = take_events();
    let op = op_of(&events);
    let attempt = |attempt| {
        [
            request(op, attempt, "GET", &endpoint, "/hello.txt"),
            Event::SendError {
                op,
                attempt,
                error: match &events[1] {
                    Event::SendError { error, .. } => error.clone(),
                    other => panic!("expected a send error, got {other:?}"),
                },
            },
        ]
    };
    assert_eq!(events, [attempt(1), attempt(2)].concat());
}

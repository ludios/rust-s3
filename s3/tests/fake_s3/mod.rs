// Model-output: Claude Fable 5.1
// Model-output: Claude Opus 5.5

//! A fake S3 that speaks minimal HTTP/1.1 on a local socket, for tests of public `Bucket`
//! methods that need no object storage service: it serves scripted replies and hands back the
//! heads of the requests it received.

#![allow(dead_code)] // Each test binary uses its own subset.

use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub const BUCKET: &str = "test-bucket";

/// Waits for one step of a scenario, failing the test instead of hanging if it takes too long.
pub async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("scenario step timed out")
}

/// One reply of the fake server: what to write for the request it receives, and whether to
/// close afterwards or to hold the connection open until the client closes it.
pub struct Reply {
    pub bytes: Vec<u8>,
    pub hold: bool,
}

pub fn reply(bytes: Vec<u8>) -> Reply {
    Reply { bytes, hold: false }
}

/// Builds an HTTP/1.1 response with `connection: close`, `content-length` matching `body`
/// unless `headers` sets its own, then `headers` and `body`.
pub fn http(status: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut out = format!("HTTP/1.1 {status}\r\nconnection: close\r\n").into_bytes();
    if !headers.iter().any(|(name, _)| *name == "content-length") {
        out.extend(format!("content-length: {}\r\n", body.len()).into_bytes());
    }
    for (name, value) in headers {
        out.extend(format!("{name}: {value}\r\n").into_bytes());
    }
    out.extend(b"\r\n");
    out.extend(body);
    out
}

/// The value of the header `name` in `head`, the head of an HTTP request, if it has one.
pub fn header_of<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (field, value) = line.split_once(':')?;
        field.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// The query parameters of the request whose head is `head`, decoded.
pub fn query_of(head: &str) -> HashMap<String, String> {
    let target = head.split_whitespace().nth(1).unwrap();
    url::Url::parse(&format!("http://fake{target}"))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

/// Reads one HTTP/1.1 request from `socket` and returns its head (request line and headers).
/// The body announced by `content-length` is read too, so that the socket can be closed
/// without resetting the connection.
async fn read_request(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let head_len = loop {
        if let Some(end) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "client closed before sending a full request head");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8(buf[..head_len].to_vec()).unwrap();
    let content_length: usize =
        header_of(&head, "content-length").map_or(0, |value| value.parse().unwrap());
    let mut body_len = buf.len() - head_len;
    while body_len < content_length {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "client closed before sending its full body");
        body_len += n;
    }
    head
}

/// A fake S3 that serves `replies` one per connection, in order, then exits. Each connection
/// carries one request, read in full before its reply is written. Returns the endpoint for
/// `Region::Custom` and a handle yielding the heads of the requests received.
pub async fn serve(replies: Vec<Reply>) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut heads = Vec::new();
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            heads.push(read_request(&mut socket).await);
            socket.write_all(&reply.bytes).await.unwrap();
            if reply.hold {
                // The client closes when it drops the response; wait for that.
                let _ = socket.read(&mut [0; 1]).await;
            } else {
                socket.shutdown().await.unwrap();
            }
        }
        heads
    });
    (endpoint, task)
}

/// A path-style bucket with fake credentials at `endpoint`, with the default retries.
pub fn bucket(endpoint: &str) -> Box<Bucket> {
    let credentials = Credentials::new(
        Some("test_access_key"),
        Some("test_secret_key"),
        None,
        None,
        None,
    )
    .unwrap();
    let region = Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    };
    Bucket::new(BUCKET, region, credentials)
        .unwrap()
        .with_path_style()
}

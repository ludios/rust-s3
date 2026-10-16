// Model-output: Claude Fable 5.1

//! `put_object_stream` against the fake S3: the parts in flight are driven while the next
//! chunk is still being read. The request observer is process-wide and set once, so this
//! binary holds one test.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{bucket, http, reply, serve, within};
use s3::bucket::CHUNK_SIZE;
use s3::{BodyObserver, Bucket, RequestObserver};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::oneshot;

/// Fires its gate when the response to the first part arrives: by then that part has been
/// sent in full and answered.
struct GateOpener(Mutex<Option<oneshot::Sender<()>>>);

impl RequestObserver for GateOpener {
    fn on_request(&self, _: u64, _: u32, _: &Bucket, _: &s3::reqwest::Request) {}

    fn on_response(
        &self,
        _: u64,
        _: u32,
        _: Duration,
        result: Result<&s3::reqwest::Response, &s3::error::S3Error>,
    ) -> Option<Box<dyn BodyObserver>> {
        let first_part = result.is_ok_and(|response| {
            response
                .url()
                .query()
                .is_some_and(|query| query.split('&').any(|pair| pair == "partNumber=1"))
        });
        if first_part && let Some(gate) = self.0.lock().unwrap().take() {
            let _ = gate.send(());
        }
        None
    }
}

/// Two full chunks of `x`. The second is held back until the gate fires, so that reading it
/// takes at least as long as the first part takes to be sent and answered. An upload that
/// drives its parts only between reads never gets past this reader.
struct GatedReader {
    gate: Option<oneshot::Receiver<()>>,
    remaining: usize,
}

impl AsyncRead for GatedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining <= CHUNK_SIZE
            && let Some(gate) = &mut self.gate
        {
            if Pin::new(gate).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.gate = None;
        }
        let n = buf.remaining().min(self.remaining);
        buf.initialize_unfilled_to(n).fill(b'x');
        buf.advance(n);
        self.remaining -= n;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn put_object_stream_sends_a_part_while_the_next_chunk_is_read() {
    let (gate_tx, gate_rx) = oneshot::channel();
    assert!(s3::set_request_observer(Arc::new(GateOpener(Mutex::new(Some(gate_tx))))).is_ok());
    let initiate = http(
        "200 OK",
        &[],
        b"<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>big.bin</Key>\
          <UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
    );
    let part = http("200 OK", &[("etag", "\"part\"")], b"");
    let complete = http("200 OK", &[], b"<CompleteMultipartUploadResult/>");
    let (endpoint, server) = serve(vec![
        reply(initiate),
        reply(part.clone()),
        reply(part),
        reply(complete),
    ])
    .await;
    let bucket = bucket(&endpoint);
    let mut reader = GatedReader {
        gate: Some(gate_rx),
        remaining: 2 * CHUNK_SIZE,
    };

    let response = within(
        bucket
            .put_object_stream_builder("/big.bin")
            .with_max_concurrent_chunks(2)
            .execute_stream(&mut reader),
    )
    .await
    .unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(response.status_code(), 200);
    assert_eq!(response.uploaded_bytes(), 2 * CHUNK_SIZE);
    assert_eq!(heads.len(), 4);
    assert!(heads[0].starts_with("POST ") && heads[0].contains("uploads"));
    let mut parts: Vec<&str> = heads[1..3]
        .iter()
        .map(|head| head.split_whitespace().nth(1).unwrap())
        .collect();
    parts.sort();
    assert!(
        parts[0].contains("partNumber=1") && parts[1].contains("partNumber=2"),
        "{parts:?}"
    );
    assert!(heads[3].starts_with("POST ") && heads[3].contains("uploadId=upload-id"));
}

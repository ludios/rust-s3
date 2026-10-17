// Model-output: Claude Opus 5.5

//! `Bucket` end to end against a real S3-compatible service, an ephemeral garage server per
//! test: streamed and manual multipart uploads, whole, ranged and streamed reads, and deletes,
//! on whichever runtime and HTTP client the features select.
//!
//! Garage gives a multipart object the ETag S3 does, built from the MD5s of its parts, so an
//! ETag shows where an upload was split. Unlike S3 it takes parts under 5 MiB, so the async
//! uploads cross several part boundaries with small parts; `sync` has only its 8 MiB default.

mod garage_server;

use garage_server::GarageServer;
use s3::Bucket;
use s3::error::S3Error;
use s3::request::ResponseData;
use s3::serde_types::{ObjectIdentifier, Part};
use std::io::{Cursor, Read};

/// The length of every part but the last in the streamed uploads here
#[cfg(not(feature = "sync"))]
const PART: usize = 256 * 1024;
#[cfg(feature = "sync")]
const PART: usize = s3::bucket::CHUNK_SIZE;

/// The content type of the uploads here, so that a HEAD can tell it from a default
const CONTENT_TYPE: &str = "application/x-garage-test";

/// A key that must be percent-encoded and signed as such: space, `+`, `=`, `&`, `%`, `~` and
/// non-ASCII
const AWKWARD_KEY: &str = "/dir/sub dir/a+b=c&d%e~é.bin";

/// The test attribute of the async runtime the features select. Tests are written async, and
/// `maybe_async::test` makes them plain tests under `sync`; it cannot see into macro arguments,
/// so `.await` stays out of `assert!` and the like.
#[cfg(feature = "with-async-std")]
use async_std::test as async_test;
#[cfg(feature = "with-tokio")]
use tokio::test as async_test;

/// `len` bytes with a period of 251, a prime, so that bytes shifted by a power-of-two part length
/// differ from the ones expected there.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The ETag S3 gives an object stored by one PUT: the quoted hex MD5 of its bytes.
fn put_etag(content: &[u8]) -> String {
    format!("\"{:x}\"", md5::compute(content))
}

/// The ETag S3 gives an object completed from `parts`, in order: the quoted hex MD5 of their
/// binary MD5s, then `-` and their count.
fn multipart_etag<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> String {
    let mut md5s = Vec::new();
    let mut count = 0;
    for part in parts {
        md5s.extend_from_slice(&md5::compute(part).0);
        count += 1;
    }
    format!("\"{:x}-{count}\"", md5::compute(&md5s))
}

/// The HTTP status `result` carries, whether `fail-on-err` made it an error or not.
fn status_of(result: Result<ResponseData, S3Error>) -> u16 {
    match result {
        Ok(response) => response.status_code(),
        Err(S3Error::HttpFailWithBody(status, _)) => status,
        Err(error) => panic!("expected an HTTP status, got {error}"),
    }
}

/// The ids of the multipart uploads in progress in `bucket`, in the order listed.
#[maybe_async::maybe_async]
async fn uploads_in_progress(bucket: &Bucket) -> Vec<String> {
    let pages = bucket.list_multiparts_uploads(None, None).await.unwrap();
    pages
        .iter()
        .flat_map(|page| &page.uploads)
        .map(|upload| upload.id.clone())
        .collect()
}

/// Upload `part` as part `number` of the multipart upload `upload_id` to `path`.
#[maybe_async::maybe_async]
async fn put_part(bucket: &Bucket, path: &str, upload_id: &str, number: u32, part: &[u8]) -> Part {
    #[cfg(not(feature = "sync"))]
    let part = part.to_vec();
    bucket
        .put_multipart_chunk(part, path, number, upload_id, CONTENT_TYPE)
        .await
        .unwrap()
}

/// Read `stream` to its end.
#[cfg(not(feature = "sync"))]
async fn read_all(stream: &mut s3::request::ResponseDataStream) -> Vec<u8> {
    use futures_util::StreamExt;
    let mut read = Vec::new();
    while let Some(chunk) = stream.bytes().next().await {
        read.extend_from_slice(&chunk.unwrap());
    }
    read
}

/// The source of a streamed upload: it yields `content`, then either reports its end once and
/// fails if read again, as a reader need not be fused, or fails at once, as a broken file or
/// socket would.
struct Source {
    content: Cursor<Vec<u8>>,
    /// Whether the read at the end of `content` fails rather than reporting the end
    fails: bool,
    /// Whether the end has been reported
    ended: bool,
}

impl Source {
    /// A source of `content` that reports its end once.
    fn ending(content: &[u8]) -> Source {
        let content = Cursor::new(content.to_vec());
        Source {
            content,
            fails: false,
            ended: false,
        }
    }

    /// A source of `content` that fails with "the source failed" at its end.
    fn failing(content: &[u8]) -> Source {
        Source {
            fails: true,
            ..Source::ending(content)
        }
    }
}

impl Read for Source {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.content.read(buf)?;
        if read > 0 || buf.is_empty() {
            Ok(read)
        } else if self.fails {
            Err(std::io::Error::other("the source failed"))
        } else if self.ended {
            Err(std::io::Error::other("read again after reporting its end"))
        } else {
            self.ended = true;
            Ok(0)
        }
    }
}

#[cfg(feature = "with-tokio")]
impl tokio::io::AsyncRead for Source {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let read = std::io::Read::read(self.get_mut(), buf.initialize_unfilled());
        std::task::Poll::Ready(read.map(|read| buf.advance(read)))
    }
}

#[cfg(feature = "with-async-std")]
impl async_std::io::Read for Source {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(std::io::Read::read(self.get_mut(), buf))
    }
}

/// Stream `content` to `path` from `Source::ending`, with `CONTENT_TYPE` and `PART`-long
/// parts, and return the response's status.
#[maybe_async::maybe_async]
async fn put_stream(bucket: &Bucket, path: &str, content: &[u8]) -> u16 {
    let mut source = Source::ending(content);
    #[cfg(not(feature = "sync"))]
    {
        let response = bucket
            .put_object_stream_builder(path)
            .with_content_type(CONTENT_TYPE)
            .with_chunk_size(PART)
            .execute_stream(&mut source)
            .await
            .unwrap();
        assert_eq!(response.uploaded_bytes(), content.len(), "{path}");
        response.status_code()
    }
    #[cfg(feature = "sync")]
    bucket
        .put_object_stream_with_content_type(&mut source, path, CONTENT_TYPE)
        .unwrap()
}

/// Below `PART` one PUT stores the object; from `PART` on, a multipart upload of
/// `PART`-long parts and a shorter last one, if any. Either way the object reads back whole,
/// with the content type, and no upload is left in progress.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn streamed_uploads_split_at_part_boundaries() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");

    for len in [0, 1, PART - 1, PART, PART + 1, 3 * PART + 1000] {
        let path = format!("/stream/{len}");
        let content = pattern(len);
        let status = put_stream(&bucket, &path, &content).await;
        assert_eq!(status, 200, "{path}");

        let read = bucket.get_object(&path).await.unwrap();
        assert!(read.as_slice() == content, "{path} read back differs");
        let expected_etag = if len < PART {
            put_etag(&content)
        } else {
            multipart_etag(content.chunks(PART))
        };
        let (head, _) = bucket.head_object(&path).await.unwrap();
        assert_eq!(head.e_tag, Some(expected_etag), "{path}");
        assert_eq!(head.content_type.as_deref(), Some(CONTENT_TYPE), "{path}");
    }
    let in_progress = uploads_in_progress(&bucket).await;
    assert!(in_progress.is_empty(), "{in_progress:?}");
}

/// `put_object_stream` cuts 8 MiB parts by default.
#[cfg(not(feature = "sync"))]
#[async_test]
async fn put_object_stream_uploads_chunk_size_parts_by_default() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let content = pattern(s3::bucket::CHUNK_SIZE + 1);

    let response = bucket
        .put_object_stream(&mut Source::ending(&content), "/default")
        .await
        .unwrap();

    assert_eq!(response.status_code(), 200);
    assert_eq!(response.uploaded_bytes(), content.len());
    let read = bucket.get_object("/default").await.unwrap();
    assert!(read.as_slice() == content);
    let (head, _) = bucket.head_object("/default").await.unwrap();
    let expected_etag = multipart_etag(content.chunks(s3::bucket::CHUNK_SIZE));
    assert_eq!(head.e_tag, Some(expected_etag));
}

/// The builder's cache control and metadata reach the object, whether one PUT stores it or a
/// multipart upload does.
#[cfg(not(feature = "sync"))]
#[async_test]
async fn streamed_upload_builder_headers_reach_the_object() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");

    for (path, len) in [("/single", PART - 1), ("/multipart", 2 * PART + 1)] {
        let response = bucket
            .put_object_stream_builder(path)
            .with_content_type(CONTENT_TYPE)
            .with_cache_control("public, max-age=120")
            .unwrap()
            .with_metadata("marker", path)
            .unwrap()
            .with_chunk_size(PART)
            .execute_stream(&mut Source::ending(&pattern(len)))
            .await
            .unwrap();
        assert_eq!(response.status_code(), 200, "{path}");

        let (head, _) = bucket.head_object(path).await.unwrap();
        assert_eq!(head.content_type.as_deref(), Some(CONTENT_TYPE), "{path}");
        assert_eq!(
            head.cache_control.as_deref(),
            Some("public, max-age=120"),
            "{path}"
        );
        let marker = head
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("marker"));
        assert_eq!(marker.map(String::as_str), Some(path), "{path}");
    }
}

/// A source that fails after the first part is up: the upload returns the source's error and
/// aborts its multipart upload, so neither the object nor the upload remains.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn a_streamed_upload_whose_source_fails_is_aborted() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let mut source = Source::failing(&pattern(PART + 1));

    #[cfg(not(feature = "sync"))]
    let result = bucket
        .put_object_stream_builder("/failed")
        .with_chunk_size(PART)
        .execute_stream(&mut source)
        .await
        .map(|response| response.status_code());
    #[cfg(feature = "sync")]
    let result = bucket.put_object_stream(&mut source, "/failed");

    let error = result.expect_err("the upload succeeded from a failing source");
    assert!(error.to_string().contains("the source failed"), "{error}");
    let in_progress = uploads_in_progress(&bucket).await;
    assert!(in_progress.is_empty(), "{in_progress:?}");
    let exists = bucket.object_exists("/failed").await.unwrap();
    assert!(!exists);
}

/// Parts uploaded out of order are listed as one upload in progress, invisible as an object,
/// until the completion assembles them by part number.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn a_multipart_upload_is_listed_until_completed_from_its_parts() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let content = pattern(250_007);
    let parts: Vec<&[u8]> = content.chunks(100_000).collect();

    let upload = bucket
        .initiate_multipart_upload(AWKWARD_KEY, CONTENT_TYPE)
        .await
        .unwrap();
    let upload_id = upload.upload_id.as_str();
    let third = put_part(&bucket, AWKWARD_KEY, upload_id, 3, parts[2]).await;
    let first = put_part(&bucket, AWKWARD_KEY, upload_id, 1, parts[0]).await;
    let second = put_part(&bucket, AWKWARD_KEY, upload_id, 2, parts[1]).await;
    assert_eq!(third.etag, put_etag(parts[2]));

    let pages = bucket
        .list_multiparts_uploads(Some("dir/"), None)
        .await
        .unwrap();
    let listed: Vec<(&str, &str)> = pages
        .iter()
        .flat_map(|page| &page.uploads)
        .map(|upload| (upload.key.as_str(), upload.id.as_str()))
        .collect();
    assert_eq!(listed, [(&AWKWARD_KEY[1..], upload_id)]);
    let pending = bucket.get_object(AWKWARD_KEY).await;
    assert_eq!(status_of(pending), 404);

    let completed = bucket
        .complete_multipart_upload(AWKWARD_KEY, upload_id, vec![first, second, third])
        .await
        .unwrap();
    assert_eq!(completed.status_code(), 200);
    let in_progress = uploads_in_progress(&bucket).await;
    assert!(in_progress.is_empty(), "{in_progress:?}");
    let (head, _) = bucket.head_object(AWKWARD_KEY).await.unwrap();
    assert_eq!(head.e_tag, Some(multipart_etag(parts)));
    let read = bucket.get_object(AWKWARD_KEY).await.unwrap();
    assert!(read.as_slice() == content);
    let across = bucket
        .get_object_range(AWKWARD_KEY, 99_990, Some(200_009))
        .await
        .unwrap();
    assert!(across.as_slice() == &content[99_990..=200_009]);
}

/// An aborted upload is no longer listed, cannot be completed, and leaves no object.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn an_aborted_multipart_upload_leaves_nothing() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");

    let upload = bucket
        .initiate_multipart_upload("/aborted", CONTENT_TYPE)
        .await
        .unwrap();
    let part = put_part(&bucket, "/aborted", &upload.upload_id, 1, &pattern(1000)).await;
    let in_progress = uploads_in_progress(&bucket).await;
    assert_eq!(in_progress, [upload.upload_id.clone()]);

    bucket
        .abort_upload("/aborted", &upload.upload_id)
        .await
        .unwrap();

    let in_progress = uploads_in_progress(&bucket).await;
    assert!(in_progress.is_empty(), "{in_progress:?}");
    let completed = bucket
        .complete_multipart_upload("/aborted", &upload.upload_id, vec![part])
        .await;
    assert_eq!(status_of(completed), 404);
    let exists = bucket.object_exists("/aborted").await.unwrap();
    assert!(!exists);
}

/// A listing one upload per page resumes from the key and upload id markers of the page
/// before, and so lists each upload once.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn multipart_upload_listing_pages_by_key_and_upload_id_markers() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let mut created = Vec::new();
    for _ in 0..2 {
        let upload = bucket
            .initiate_multipart_upload("/paged", CONTENT_TYPE)
            .await
            .unwrap();
        created.push(upload.upload_id);
    }

    let (first, status) = bucket
        .list_multiparts_uploads_page(None, None, None, None, Some(1))
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(first.is_truncated);
    assert_eq!(first.uploads.len(), 1);
    let (second, status) = bucket
        .list_multiparts_uploads_page(
            None,
            None,
            Some(first.next_marker.clone().expect("no NextKeyMarker")),
            Some(
                first
                    .next_upload_id_marker
                    .clone()
                    .expect("no NextUploadIdMarker"),
            ),
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(!second.is_truncated);
    assert_eq!(second.uploads.len(), 1);

    let mut listed = vec![first.uploads[0].id.clone(), second.uploads[0].id.clone()];
    listed.sort();
    created.sort();
    assert_eq!(listed, created);
    let mut listed_at_once = uploads_in_progress(&bucket).await;
    listed_at_once.sort();
    assert_eq!(listed_at_once, created);
}

/// A stored object exists; a GET returns it whole, and a ranged GET the inclusive range, cut
/// at the object's end, or 416 when it starts past it; to a writer and, async, as a stream
/// alike.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn reads_return_the_whole_object_or_the_requested_range() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let content = pattern(300_000);
    let len = content.len() as u64;
    let put = bucket.put_object(AWKWARD_KEY, &content).await.unwrap();
    assert_eq!(put.status_code(), 200);

    let whole = bucket.get_object(AWKWARD_KEY).await.unwrap();
    assert_eq!(whole.status_code(), 200);
    assert!(whole.as_slice() == content);
    let (head, status) = bucket.head_object(AWKWARD_KEY).await.unwrap();
    assert_eq!(status, 200);
    assert_eq!(head.content_length, Some(len as i64));
    assert_eq!(head.e_tag, Some(put_etag(&content)));
    let exists = bucket.object_exists(AWKWARD_KEY).await.unwrap();
    assert!(exists);

    let ranges: [(u64, Option<u64>, &[u8]); 5] = [
        (100, Some(1000), &content[100..=1000]),
        (100, Some(100), &content[100..=100]),
        (len - 1, Some(len - 1), &content[len as usize - 1..]),
        (len - 10, None, &content[len as usize - 10..]),
        (1000, Some(len + 100), &content[1000..]),
    ];
    for (start, end, expected) in ranges {
        let range = bucket
            .get_object_range(AWKWARD_KEY, start, end)
            .await
            .unwrap();
        assert_eq!(range.status_code(), 206, "{start}-{end:?}");
        assert!(range.as_slice() == expected, "{start}-{end:?} differs");
    }
    let past_the_end = bucket.get_object_range(AWKWARD_KEY, len, None).await;
    assert_eq!(status_of(past_the_end), 416);

    let mut written = Vec::new();
    let status = bucket
        .get_object_to_writer(AWKWARD_KEY, &mut written)
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(written == content);
    let mut written = Vec::new();
    let status = bucket
        .get_object_range_to_writer(AWKWARD_KEY, 1000, Some(250_000), &mut written)
        .await
        .unwrap();
    assert_eq!(status, 206);
    assert!(written == content[1000..=250_000]);

    #[cfg(not(feature = "sync"))]
    {
        let mut stream = bucket.get_object_stream(AWKWARD_KEY).await.unwrap();
        assert_eq!(stream.status_code, 200);
        let streamed = read_all(&mut stream).await;
        assert!(streamed == content);
        let mut stream = bucket
            .get_object_range_stream(AWKWARD_KEY, 1000, Some(250_000))
            .await
            .unwrap();
        assert_eq!(stream.status_code, 206);
        let streamed = read_all(&mut stream).await;
        assert!(streamed == content[1000..=250_000]);
    }
}

/// A deleted object no longer exists or reads, and deleting a missing key succeeds, one at a
/// time and in bulk.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn deletes_remove_objects_and_succeed_for_missing_keys() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    for path in ["/delete/one", "/delete/two", "/delete/three", "/kept"] {
        bucket.put_object(path, b"content").await.unwrap();
    }

    let deleted = bucket.delete_object("/delete/one").await.unwrap();
    assert_eq!(deleted.status_code(), 204);
    let exists = bucket.object_exists("/delete/one").await.unwrap();
    assert!(!exists);
    let read = bucket.get_object("/delete/one").await;
    assert_eq!(status_of(read), 404);
    let range = bucket.get_object_range("/delete/one", 0, None).await;
    assert_eq!(status_of(range), 404);
    let deleted_again = bucket.delete_object("/delete/one").await.unwrap();
    assert_eq!(deleted_again.status_code(), 204);

    let result = bucket
        .delete_objects(vec![
            ObjectIdentifier::new("/delete/two"),
            ObjectIdentifier::new("delete/three"),
            ObjectIdentifier::new("/delete/never"),
        ])
        .await
        .unwrap();
    let mut deleted: Vec<&str> = result
        .deleted
        .iter()
        .map(|object| object.key.as_str())
        .collect();
    deleted.sort();
    assert_eq!(deleted, ["delete/never", "delete/three", "delete/two"]);
    assert!(result.errors.is_empty(), "{:?}", result.errors);

    let listed: Vec<String> = bucket
        .list(String::new(), None)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|page| page.contents)
        .map(|object| object.key)
        .collect();
    assert_eq!(listed, ["kept"]);
}

/// `copy_object_internal` copies an object, from a key that the copy source must
/// percent-encode, to another key.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn copy_object_internal_copies_the_object() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let content = pattern(3000);
    bucket.put_object(AWKWARD_KEY, &content).await.unwrap();

    let status = bucket
        .copy_object_internal(AWKWARD_KEY, "/copy")
        .await
        .unwrap();

    assert_eq!(status, 200);
    let copied = bucket.get_object("/copy").await.unwrap();
    assert!(copied.as_slice() == content);
}

/// The cache control and metadata headers given to `put_object_with_headers` reach the object.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn put_object_with_headers_stores_cache_control_and_metadata() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("max-age=42"),
    );
    headers.insert("x-amz-meta-marker", http::HeaderValue::from_static("put"));

    let put = bucket
        .put_object_with_headers("/headers", b"content", Some(headers))
        .await
        .unwrap();

    assert_eq!(put.status_code(), 200);
    let (head, _) = bucket.head_object("/headers").await.unwrap();
    assert_eq!(head.cache_control.as_deref(), Some("max-age=42"));
    let marker = head
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("marker"));
    assert_eq!(marker.map(String::as_str), Some("put"));
}

/// A listing of at most two entries per page resumes from each page's continuation token until
/// a page is not truncated, and so lists each key under the prefix once; the delimiter rolls
/// the deeper key up into a common prefix.
#[maybe_async::test(feature = "sync", async(not(feature = "sync"), async_test))]
async fn object_listing_pages_by_continuation_token() {
    let garage = GarageServer::start();
    let bucket = garage.bucket("test");
    for path in ["/list/a", "/list/b", "/list/c", "/list/deeper/d", "/other"] {
        bucket.put_object(path, b"content").await.unwrap();
    }

    let mut keys = Vec::new();
    let mut prefixes = Vec::new();
    let mut token = None;
    for page_number in 1.. {
        assert!(page_number <= 4, "the listing did not end after 4 pages");
        let (page, status) = bucket
            .list_page(
                "list/".to_owned(),
                Some("/".to_owned()),
                token,
                None,
                Some(2),
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
        let page_prefixes = page.common_prefixes.unwrap_or_default();
        let entries = page.contents.len() + page_prefixes.len();
        assert!(entries <= 2, "page {page_number} has {entries} entries");
        keys.extend(page.contents.into_iter().map(|object| object.key));
        prefixes.extend(page_prefixes.into_iter().map(|prefix| prefix.prefix));
        if !page.is_truncated {
            break;
        }
        token = Some(
            page.next_continuation_token
                .expect("a truncated page has no token"),
        );
    }

    assert_eq!(keys, ["list/a", "list/b", "list/c"]);
    assert_eq!(prefixes, ["list/deeper/"]);
}

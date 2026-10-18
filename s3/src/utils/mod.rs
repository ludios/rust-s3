// Model-output: Claude Fable 5.1
// Model-output: Claude Opus 5.5

mod time_utils;

pub use time_utils::*;

use std::str::FromStr;

use crate::error::S3Error;
use crate::request::ResponseData;
use crate::{bucket::CHUNK_SIZE, serde_types::HeadObjectResult};

use std::fs::File;

use std::io::Read;
use std::path::Path;

#[cfg(feature = "with-tokio")]
use tokio::io::{AsyncRead, AsyncReadExt};

#[cfg(feature = "with-async-std")]
use async_std::io::Read as AsyncRead;

#[cfg(feature = "with-async-std")]
use async_std::io::ReadExt as AsyncReadExt;

#[derive(Debug)]
pub struct PutStreamResponse {
    status_code: u16,
    uploaded_bytes: usize,
}

impl PutStreamResponse {
    pub fn new(status_code: u16, uploaded_bytes: usize) -> Self {
        Self {
            status_code,
            uploaded_bytes,
        }
    }
    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn uploaded_bytes(&self) -> usize {
        self.uploaded_bytes
    }
}

/// The ETag, unquoted, that `put_object_stream` gives the file at `path`: the hex MD5 of its
/// bytes when it is shorter than `CHUNK_SIZE` and goes up in one PUT, otherwise the hex MD5 of
/// its `CHUNK_SIZE`-long parts' MD5s, then `-` and the part count. A file of an exact multiple
/// of `CHUNK_SIZE` ends with a full part.
///
/// # Example
/// ```rust,no_run
/// use s3::utils::etag_for_path;
///
/// let path = "test_etag";
/// let etag = etag_for_path(path).unwrap();
/// println!("{}", etag);
/// ```
pub fn etag_for_path(path: impl AsRef<Path>) -> Result<String, S3Error> {
    let mut file = File::open(path)?;
    let mut digests = Vec::new();
    let mut parts = 0;
    loop {
        let chunk = read_chunk(&mut file)?;
        if parts == 0 && chunk.len() < CHUNK_SIZE {
            return Ok(format!("{:x}", md5::compute(&chunk)));
        }
        if chunk.is_empty() {
            break;
        }
        digests.extend_from_slice(&md5::compute(&chunk).0);
        parts += 1;
        if chunk.len() < CHUNK_SIZE {
            break;
        }
    }
    Ok(format!("{:x}-{parts}", md5::compute(digests)))
}

pub fn read_chunk<R: Read + ?Sized>(reader: &mut R) -> Result<Vec<u8>, S3Error> {
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    let mut take = reader.take(CHUNK_SIZE as u64);
    take.read_to_end(&mut chunk)?;

    Ok(chunk)
}

/// Up to `chunk_size` bytes of `reader`, fewer only at its end.
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub async fn read_chunk_async<R: AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    chunk_size: usize,
) -> Result<Vec<u8>, S3Error> {
    let mut chunk = Vec::with_capacity(chunk_size);
    let mut take = reader.take(chunk_size as u64);
    take.read_to_end(&mut chunk).await?;

    Ok(chunk)
}

pub trait GetAndConvertHeaders {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T>;
    fn get_string(&self, header: &str) -> Option<String>;
}

impl GetAndConvertHeaders for http::header::HeaderMap {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T> {
        self.get(header)?.to_str().ok()?.parse::<T>().ok()
    }
    fn get_string(&self, header: &str) -> Option<String> {
        Some(self.get(header)?.to_str().ok()?.to_owned())
    }
}

/// The count in the header `name` of `headers`, such as `Content-Length`: `None` unless every
/// field of it is the same run of ASCII digits, so that a sign, a blank or a second, differing
/// field is no count rather than a misread one.
fn get_count(headers: &http::HeaderMap, name: &str) -> Option<i64> {
    let mut fields = headers.get_all(name).iter();
    let first = fields.next()?;
    if fields.any(|field| field != first) {
        return None;
    }
    let digits = first.to_str().ok()?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The fields of a HEAD response's `headers`. A field is `None` when its header is missing, is
/// not visible ASCII, or does not parse as the field's type, and a count when `get_count` finds
/// none: a malformed `Content-Length` is no length, not a length of 0. `metadata` holds the
/// visible-ASCII `x-amz-meta-*` headers.
impl From<&http::HeaderMap> for HeadObjectResult {
    fn from(headers: &http::HeaderMap) -> Self {
        let metadata = headers
            .iter()
            .filter_map(|(key, value)| {
                let name = key.as_str().strip_prefix("x-amz-meta-")?;
                Some((name.to_owned(), value.to_str().ok()?.to_owned()))
            })
            .collect();
        HeadObjectResult {
            accept_ranges: headers.get_string("accept-ranges"),
            cache_control: headers.get_string("Cache-Control"),
            content_disposition: headers.get_string("Content-Disposition"),
            content_encoding: headers.get_string("Content-Encoding"),
            content_language: headers.get_string("Content-Language"),
            content_length: get_count(headers, "Content-Length"),
            content_type: headers.get_string("Content-Type"),
            delete_marker: headers.get_and_convert("x-amz-delete-marker"),
            e_tag: headers.get_string("ETag"),
            expiration: headers.get_string("x-amz-expiration"),
            expires: headers.get_string("Expires"),
            last_modified: headers.get_string("Last-Modified"),
            metadata: Some(metadata),
            missing_meta: get_count(headers, "x-amz-missing-meta"),
            object_lock_legal_hold_status: headers.get_string("x-amz-object-lock-legal-hold"),
            object_lock_mode: headers.get_string("x-amz-object-lock-mode"),
            object_lock_retain_until_date: headers
                .get_string("x-amz-object-lock-retain-until-date"),
            parts_count: get_count(headers, "x-amz-mp-parts-count"),
            replication_status: headers.get_string("x-amz-replication-status"),
            request_charged: headers.get_string("x-amz-request-charged"),
            restore: headers.get_string("x-amz-restore"),
            sse_customer_algorithm: headers
                .get_string("x-amz-server-side-encryption-customer-algorithm"),
            sse_customer_key_md5: headers
                .get_string("x-amz-server-side-encryption-customer-key-MD5"),
            ssekms_key_id: headers.get_string("x-amz-server-side-encryption-aws-kms-key-id"),
            server_side_encryption: headers.get_string("x-amz-server-side-encryption"),
            storage_class: headers.get_string("x-amz-storage-class"),
            version_id: headers.get_string("x-amz-version-id"),
            website_redirect_location: headers.get_string("x-amz-website-redirect-location"),
        }
    }
}

pub(crate) fn error_from_response_data(response_data: ResponseData) -> Result<S3Error, S3Error> {
    let utf8_content = String::from_utf8(response_data.as_slice().to_vec())?;
    Err(S3Error::HttpFailWithBody(
        response_data.status_code(),
        utf8_content,
    ))
}

/// Retries a given expression a specified number of times with exponential backoff.
///
/// This macro attempts to execute the provided expression up to `N` times, where `N`
/// is the value set by `set_retries`. If the expression returns `Ok`, it returns the value.
/// If the expression returns `Err`, it logs a warning and retries after a delay that increases
/// exponentially with each retry.
///
/// The delay between retries is calculated as `1 * retry_cnt.pow(2)` seconds, where `retry_cnt`
/// is the current retry attempt.
///
/// This macro supports both asynchronous and synchronous contexts:
/// - For `tokio` users, it uses `tokio::time::sleep`.
/// - For `async-std` users, it uses `async_std::task::sleep`.
/// - For synchronous contexts, it uses `std::thread::sleep`.
///
/// # Features
///
/// - `with-tokio`: Uses `tokio::time::sleep` for async retries.
/// - `with-async-std`: Uses `async_std::task::sleep` for async retries.
/// - `sync`: Uses `std::thread::sleep` for sync retries.
///
/// # Errors
///
/// If all retry attempts fail, the last error is returned.
#[macro_export]
macro_rules! retry {
    ($e:expr) => {{
        let mut retry_cnt: u64 = 0;
        let max_retries = $crate::get_retries() as u64;

        loop {
            match $e {
                Ok(v) => break Ok(v),
                Err(e) => {
                    log::warn!("Retrying {e}");
                    if retry_cnt >= max_retries {
                        break Err(e);
                    }
                    retry_cnt += 1;
                    let delay = std::time::Duration::from_secs(1 * retry_cnt.pow(2));
                    #[cfg(feature = "with-tokio")]
                    tokio::time::sleep(delay).await;
                    #[cfg(feature = "with-async-std")]
                    async_std::task::sleep(delay).await;
                    #[cfg(feature = "sync")]
                    std::thread::sleep(delay);
                    continue;
                }
            }
        }
    }};
}

#[cfg(test)]
mod test {
    use crate::serde_types::HeadObjectResult;
    use crate::utils::etag_for_path;
    use std::fs::File;
    use std::io::Cursor;
    use std::io::prelude::*;
    use std::path::PathBuf;

    fn object(size: u32) -> Vec<u8> {
        (0..size).map(|_| 33).collect()
    }

    fn etag_test_path(test_name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        path.push(format!(
            "rust-s3-{test_name}-{}-{unique}",
            std::process::id()
        ));
        path
    }

    #[test]
    fn test_etag_large_file() {
        let path = etag_test_path("etag-large-file");
        std::fs::remove_file(&path).unwrap_or(());
        let test: Vec<u8> = object(10_000_000);

        let mut file = File::create(&path).unwrap();
        file.write_all(&test).unwrap();

        let etag = etag_for_path(&path).unwrap();

        std::fs::remove_file(&path).unwrap_or(());

        assert_eq!(etag, "e438487f09f09c042b2de097765e5ac2-2");
    }

    /// The empty file and files of one and two whole chunks: one PUT's ETag for the first,
    /// and for the others a part count without an empty part after the full ones.
    #[test]
    fn test_etag_at_chunk_multiples() {
        use crate::bucket::CHUNK_SIZE;

        for parts in [0, 1, 2] {
            let path = etag_test_path(&format!("etag-{parts}-chunks"));
            let test = object((parts * CHUNK_SIZE) as u32);
            std::fs::write(&path, &test).unwrap();

            let etag = etag_for_path(&path).unwrap();
            std::fs::remove_file(&path).unwrap_or(());

            let expected = if parts == 0 {
                format!("{:x}", md5::compute(b""))
            } else {
                let md5s: Vec<u8> = test
                    .chunks(CHUNK_SIZE)
                    .flat_map(|part| md5::compute(part).0)
                    .collect();
                format!("{:x}-{parts}", md5::compute(md5s))
            };
            assert_eq!(etag, expected, "{parts} chunks");
        }
    }

    #[test]
    fn test_etag_small_file() {
        let path = etag_test_path("etag-small-file");
        std::fs::remove_file(&path).unwrap_or(());
        let test: Vec<u8> = object(1000);

        let mut file = File::create(&path).unwrap();
        file.write_all(&test).unwrap();

        let etag = etag_for_path(&path).unwrap();

        std::fs::remove_file(&path).unwrap_or(());

        assert_eq!(etag, "8122ef1c2b2331f7986349560248cf56");
    }

    #[test]
    fn test_read_chunk_all_zero() {
        let blob = vec![0u8; 10_000_000];
        let mut blob = Cursor::new(blob);

        let result = super::read_chunk(&mut blob).unwrap();

        assert_eq!(result.len(), crate::bucket::CHUNK_SIZE);
    }

    #[test]
    fn test_read_chunk_multi_chunk() {
        let blob = vec![1u8; 10_000_000];
        let mut blob = Cursor::new(blob);

        let result = super::read_chunk(&mut blob).unwrap();
        assert_eq!(result.len(), crate::bucket::CHUNK_SIZE);

        let result = super::read_chunk(&mut blob).unwrap();
        assert_eq!(result.len(), 1_611_392);
    }

    /// The HEAD result of a response whose headers are `fields`, in order.
    fn head(fields: &[(&'static str, &'static str)]) -> HeadObjectResult {
        let mut headers = http::HeaderMap::new();
        for (name, value) in fields {
            headers.append(*name, http::HeaderValue::from_static(value));
        }
        HeadObjectResult::from(&headers)
    }

    #[test]
    fn head_content_length_is_only_agreeing_digits() {
        let length = |fields: &[(&'static str, &'static str)]| head(fields).content_length;
        assert_eq!(length(&[("content-length", "1234")]), Some(1234));
        assert_eq!(
            length(&[("content-length", "0"), ("content-length", "0")]),
            Some(0)
        );
        assert_eq!(length(&[]), None);
        for malformed in [
            "",
            "12x",
            "+0",
            "-0",
            "-5",
            " 5",
            "5, 5",
            "99999999999999999999",
        ] {
            assert_eq!(
                length(&[("content-length", malformed)]),
                None,
                "{malformed:?}"
            );
        }
        assert_eq!(
            length(&[("content-length", "0"), ("content-length", "1234")]),
            None
        );
    }

    #[test]
    fn head_counts_and_booleans_that_do_not_parse_are_absent() {
        let well_formed = head(&[
            ("x-amz-mp-parts-count", "3"),
            ("x-amz-missing-meta", "2"),
            ("x-amz-delete-marker", "true"),
        ]);
        assert_eq!(well_formed.parts_count, Some(3));
        assert_eq!(well_formed.missing_meta, Some(2));
        assert_eq!(well_formed.delete_marker, Some(true));

        let malformed = head(&[
            ("x-amz-mp-parts-count", "many"),
            ("x-amz-missing-meta", "-2"),
            ("x-amz-delete-marker", "yes"),
        ]);
        assert_eq!(malformed.parts_count, None);
        assert_eq!(malformed.missing_meta, None);
        assert_eq!(malformed.delete_marker, None);
    }
}

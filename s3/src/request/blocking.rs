// Model-output: Claude Fable 5.1

extern crate base64;
extern crate md5;

use std::io;
use std::io::Write;
use std::sync::OnceLock;

use attohttpc::header::{ACCEPT_ENCODING, CONTENT_ENCODING, HeaderName};

use crate::bucket::Bucket;
use crate::command::Command;
use crate::error::S3Error;
use crate::utils::now_utc;
use bytes::Bytes;
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::command::HttpMethod;
use crate::request::{Request, RequestAttemptError, ResponseData};

// Temporary structure for making a request
pub struct AttoRequest<'a> {
    pub bucket: &'a Bucket,
    pub path: &'a str,
    pub command: Command<'a>,
    pub datetime: OffsetDateTime,
    pub sync: bool,
}

/// A session carrying `request`'s signed headers, with the bucket's `request_timeout` as its
/// connect and read timeouts, so that a transfer that keeps moving never times out, however
/// large. Without one, attohttpc's own 30-second defaults for both stay.
fn session(request: &AttoRequest<'_>) -> Result<attohttpc::Session, S3Error> {
    let mut session = attohttpc::Session::new();
    for (name, value) in request.headers()?.iter() {
        session.header(HeaderName::from_bytes(name.as_ref())?, value.to_str()?);
    }
    if let Some(limit) = request.bucket.request_timeout {
        session.connect_timeout(limit);
        session.read_timeout(limit);
    }
    Ok(session)
}

/// Whether this build's attohttpc decodes a response body whose `Content-Encoding` is gzip or
/// deflate before handing it over. It does whenever any crate in the build enables its
/// `compress` feature, and nothing can switch that off. The same feature makes a default
/// request advertise `Accept-Encoding`, which is what this looks for: both hang off attohttpc's
/// `flate2` dependency.
fn attohttpc_decodes_bodies() -> bool {
    static DECODES: OnceLock<bool> = OnceLock::new();
    *DECODES.get_or_init(|| {
        attohttpc::RequestBuilder::new(attohttpc::Method::GET, "http://localhost/")
            .prepare()
            .headers()
            .contains_key(ACCEPT_ENCODING)
    })
}

/// The `Content-Encoding` of `response` if attohttpc decoded its body on the way in, which
/// per [`attohttpc_decodes_bodies`] it may do to a gzip or deflate one. The response to a
/// HEAD has no body to decode.
fn decoded_encoding(request: &AttoRequest<'_>, response: &attohttpc::Response) -> Option<String> {
    if request.command.http_verb() == HttpMethod::Head || !attohttpc_decodes_bodies() {
        return None;
    }
    let encodings: Vec<&str> = response
        .headers()
        .get_all(CONTENT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    let decoded = encodings
        .iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|item| item.eq_ignore_ascii_case("gzip") || item.eq_ignore_ascii_case("deflate"));
    decoded.then(|| encodings.join(", "))
}

/// Sends `request` once, through [`session`], to its URL with its body, and returns whatever
/// response arrives.
fn send(request: &AttoRequest<'_>) -> Result<attohttpc::Response, RequestAttemptError> {
    let session = session(request)?;
    let url = request.url()?;
    let builder = match request.command.http_verb() {
        HttpMethod::Get => session.get(url),
        HttpMethod::Delete => session.delete(url),
        HttpMethod::Put => session.put(url),
        HttpMethod::Post => session.post(url),
        HttpMethod::Head => session.head(url),
    };
    Ok(builder.bytes(&request.request_body()?).send()?)
}

/// Sends `request` once and returns its response, or the failure its status reports under
/// `fail-on-err`, or a refusal if attohttpc decoded its body: the stored bytes are gone.
fn atto_response_attempt(
    request: &AttoRequest<'_>,
) -> Result<attohttpc::Response, RequestAttemptError> {
    let response = send(request)?;
    if cfg!(feature = "fail-on-err") && !response.status().is_success() {
        let status = response.status().as_u16();
        let body = response
            .text()
            .map_err(|error| RequestAttemptError::do_not_retry(S3Error::Atto(error)))?;
        return Err(S3Error::HttpFailWithBody(status, body).into());
    }
    match decoded_encoding(request, &response) {
        Some(encoding) => Err(S3Error::AttoDecodedBody(encoding).into()),
        None => Ok(response),
    }
}

impl<'a> Request for AttoRequest<'a> {
    type Response = attohttpc::Response;
    type HeaderMap = attohttpc::header::HeaderMap;

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

    fn response(&self) -> Result<Self::Response, S3Error> {
        atto_response_attempt(self).map_err(|error| error.error)
    }

    fn response_status(&self) -> Result<u16, S3Error> {
        crate::request::retry_request_sync(&self.command, || {
            let response = send(self)?;
            let status = response.status().as_u16();
            if status == 404 {
                Ok(status)
            } else if cfg!(feature = "fail-on-err") && !response.status().is_success() {
                let text = response
                    .text()
                    .map_err(|error| RequestAttemptError::do_not_retry(S3Error::Atto(error)))?;
                Err(S3Error::HttpFailWithBody(status, text).into())
            } else {
                Ok(status)
            }
        })
    }

    fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error> {
        let response =
            crate::request::retry_request_sync(&self.command, || atto_response_attempt(self))?;
        let status_code = response.status().as_u16();

        let response_headers = response
            .headers()
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
            if let Some(etag) = response.headers().get("ETag") {
                Bytes::from(etag.to_str()?.to_string())
            } else {
                Bytes::from("")
            }
        } else {
            // HEAD requests don't have a response body
            if self.command.http_verb() == HttpMethod::Head {
                Bytes::from("")
            } else {
                Bytes::from(response.bytes()?)
            }
        };
        Ok(ResponseData::new(body_vec, status_code, response_headers))
    }

    fn response_data_to_writer<T: Write + ?Sized>(&self, writer: &mut T) -> Result<u16, S3Error> {
        let mut response =
            crate::request::retry_request_sync(&self.command, || atto_response_attempt(self))?;

        let status_code = response.status();
        io::copy(&mut response, writer)?;

        Ok(status_code.as_u16())
    }

    fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error> {
        let response =
            crate::request::retry_request_sync(&self.command, || atto_response_attempt(self))?;
        let status_code = response.status().as_u16();
        let headers = response.headers().clone();
        Ok((headers, status_code))
    }
}

impl<'a> AttoRequest<'a> {
    pub fn new<'b>(
        bucket: &'b Bucket,
        path: &'b str,
        command: Command<'b>,
    ) -> Result<AttoRequest<'b>, S3Error> {
        bucket.credentials_refresh()?;
        Ok(AttoRequest {
            bucket,
            path,
            command,
            datetime: now_utc(),
            sync: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::error::S3Error;
    use crate::request::Request;
    use crate::request::blocking::AttoRequest;
    use anyhow::Result;
    use awscreds::Credentials;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    // Fake keys - otherwise using Credentials::default will use actual user
    // credentials if they exist.
    fn fake_credentials() -> Credentials {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secert_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        Credentials::new(Some(access_key), Some(secert_key), None, None, None).unwrap()
    }

    /// Serves one request on a local socket, answering with `script`: each step sleeps for
    /// its delay, then writes its bytes. Returns the endpoint.
    fn serve_script(script: Vec<(Duration, &'static [u8])>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).unwrap_or(0) == 0 {
                    return;
                }
                request.push(byte[0]);
            }
            for (delay, bytes) in script {
                thread::sleep(delay);
                if stream.write_all(bytes).is_err() {
                    return;
                }
            }
        });
        endpoint
    }

    fn timeout_bucket(endpoint: String, limit: Duration) -> Bucket {
        let region = crate::region::Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint,
        };
        let mut bucket = *Bucket::new("timeout-test", region, fake_credentials())
            .unwrap()
            .with_path_style();
        bucket.set_request_timeout(Some(limit));
        bucket
    }

    #[test]
    fn request_timeout_resets_with_each_body_chunk() {
        // Three gaps add up to more than the limit; each alone is within it.
        let gap = Duration::from_millis(400);
        let endpoint = serve_script(vec![
            (
                Duration::ZERO,
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n",
            ),
            (gap, b"ab"),
            (gap, b"cd"),
            (gap, b"ef"),
        ]);
        let bucket = timeout_bucket(endpoint, Duration::from_secs(1));

        let response = bucket
            .get_object("/object")
            .expect("a body that keeps arriving must not time out");
        assert_eq!(response.as_slice(), b"abcdef");
    }

    #[test]
    fn request_timeout_fails_a_stalled_body() {
        let endpoint = serve_script(vec![
            (
                Duration::ZERO,
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
            ),
            (Duration::from_secs(3), b"def"),
        ]);
        let bucket = timeout_bucket(endpoint, Duration::from_millis(300));

        let error = bucket
            .get_object("/object")
            .expect_err("a stalled body must time out");
        assert!(
            matches!(&error, S3Error::Atto(error)
                if matches!(error.kind(), attohttpc::ErrorKind::Io(io)
                    if matches!(io.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut))),
            "{error:?}"
        );
    }

    #[test]
    fn url_uses_https_by_default() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";
        let request = AttoRequest::new(&bucket, path, Command::GetObject).unwrap();

        assert_eq!(request.url()?.scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "my-first-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_https_by_default_path_style() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let bucket = bucket.with_path_style();
        let path = "/my-first/path";
        let request = AttoRequest::new(&bucket, path, Command::GetObject).unwrap();

        assert_eq!(request.url()?.scheme(), "https");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();

        assert_eq!(*host, "custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let path = "/my-second/path";
        let request = AttoRequest::new(&bucket, path, Command::GetObject).unwrap();

        assert_eq!(request.url()?.scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "my-second-bucket.custom-region".to_string());
        Ok(())
    }

    #[test]
    fn url_uses_scheme_from_custom_region_if_defined_with_path_style() -> Result<()> {
        let region = "http://custom-region".parse()?;
        let bucket = Bucket::new("my-second-bucket", region, fake_credentials())?;
        let bucket = bucket.with_path_style();
        let path = "/my-second/path";
        let request = AttoRequest::new(&bucket, path, Command::GetObject).unwrap();

        assert_eq!(request.url()?.scheme(), "http");

        let headers = request.headers().unwrap();
        let host = headers.get("Host").unwrap();
        assert_eq!(*host, "custom-region".to_string());

        Ok(())
    }

    #[test]
    fn head_object_omits_body_headers_from_request_and_signature() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let request = AttoRequest::new(&bucket, "/my-first/path", Command::HeadObject).unwrap();

        let headers = request.headers().unwrap();

        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));
        assert!(headers.contains_key("x-amz-content-sha256"));

        let authorization = headers.get("Authorization").unwrap().to_str()?;
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();

        assert!(!signed_headers.contains("content-length"));
        assert!(!signed_headers.contains("content-type"));

        Ok(())
    }

    #[test]
    fn range_get_omits_empty_body_headers_from_signature() -> Result<()> {
        let region = "custom-region".parse()?;
        let bucket = Bucket::new("my-first-bucket", region, fake_credentials())?;
        let path = "/my-first/path";

        for (start, end, expected_range) in [(0, None, "bytes=0-"), (10, Some(20), "bytes=10-20")] {
            let request = AttoRequest::new(&bucket, path, Command::GetObjectRange { start, end })?;
            let headers = request.headers()?;
            assert_eq!(headers.get("Range").unwrap(), expected_range);
            assert_eq!(headers.get("Accept").unwrap(), "application/octet-stream");
            assert!(!headers.contains_key("Content-Length"));
            assert!(!headers.contains_key("Content-Type"));
            let authorization = headers.get("Authorization").unwrap().to_str()?;
            let signed_headers = authorization
                .split("SignedHeaders=")
                .nth(1)
                .and_then(|value| value.split(',').next())
                .unwrap();
            assert!(signed_headers.contains("range"));
            assert!(!signed_headers.contains("content-length"));
            assert!(!signed_headers.contains("content-type"));
        }

        let get = AttoRequest::new(&bucket, path, Command::GetObject)?;
        let headers = get.headers()?;
        assert!(!headers.contains_key("Content-Length"));
        assert!(!headers.contains_key("Content-Type"));

        let put = AttoRequest::new(
            &bucket,
            path,
            Command::PutObject {
                content: b"abc",
                content_type: "application/test",
                custom_headers: None,
                multipart: None,
            },
        )?;
        let headers = put.headers()?;
        assert_eq!(headers.get("Content-Length").unwrap(), "3");
        assert_eq!(headers.get("Content-Type").unwrap(), "application/test");
        let authorization = headers.get("Authorization").unwrap().to_str()?;
        let signed_headers = authorization
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .unwrap();
        assert!(signed_headers.contains("content-length"));
        assert!(signed_headers.contains("content-type"));

        Ok(())
    }
}

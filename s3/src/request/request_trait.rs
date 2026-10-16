// Model-output: Claude Fable 5.1
// Model-output: Claude Opus 5.5

use base64::Engine;
use base64::engine::general_purpose;
use hmac::Mac;
use quick_xml::se::to_string;
use std::collections::HashMap;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use std::pin::Pin;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use std::task::{Context, Poll, ready};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc2822;
use url::Url;

use crate::LONG_DATETIME;
use crate::bucket::Bucket;
use crate::command::Command;
use crate::creds::error::CredentialsError;
use crate::error::S3Error;
use crate::request::request_url::RequestUrl;
use crate::signing;
use bytes::Bytes;
use http::HeaderMap;
use http::header::{
    ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST, HeaderName, RANGE,
};

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use futures_util::Stream;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
use futures_util::stream::{self, StreamExt};

#[derive(Debug)]

pub struct ResponseData {
    bytes: Bytes,
    status_code: u16,
    headers: HashMap<String, String>,
}

#[cfg(all(
    test,
    any(feature = "sync", feature = "with-tokio", feature = "with-async-std")
))]
mod generated_bodyless_header_tests {
    use super::Request;
    use crate::bucket::Bucket;
    use crate::command::Command;
    use crate::error::S3Error;
    use crate::region::Region;
    use awscreds::Credentials;
    use http::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};

    #[cfg(feature = "sync")]
    type BackendRequest<'a> = crate::request::blocking::AttoRequest<'a>;
    #[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
    type BackendRequest<'a> = crate::request::tokio_backend::ReqwestRequest<'a>;
    #[cfg(all(
        not(feature = "sync"),
        not(feature = "with-tokio"),
        feature = "with-async-std"
    ))]
    type BackendRequest<'a> = crate::request::async_std_backend::SurfRequest<'a>;

    #[maybe_async::maybe_async]
    async fn make_request<'a>(
        bucket: &'a Bucket,
        command: Command<'a>,
    ) -> Result<BackendRequest<'a>, S3Error> {
        BackendRequest::new(bucket, "key", command).await
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn custom_endpoint_path_stays_in_url_but_not_host_authority() {
        use http::header::{AUTHORIZATION, HOST};
        use url::Url;

        let credentials =
            Credentials::new(Some("test-key"), Some("test-secret"), None, None, None).unwrap();
        let bucket = Bucket::new(
            "bucket",
            Region::Custom {
                region: "us-east-1".to_owned(),
                endpoint: "https://project.example:8443/storage/v1/s3".to_owned(),
            },
            credentials,
        )
        .unwrap()
        .with_path_style();

        let request = make_request(&bucket, Command::GetObject).await.unwrap();
        let request_url = request.url().unwrap();
        assert_eq!(
            request_url.as_str(),
            "https://project.example:8443/storage/v1/s3/bucket/key"
        );

        let headers = request.headers().await.unwrap();
        assert_eq!(headers[HOST], "project.example:8443");
        let mut signing_headers = headers.clone();
        signing_headers.remove(AUTHORIZATION);
        signing_headers.remove(http::header::DATE);
        let canonical_request = request.canonical_request(&signing_headers).unwrap();
        let canonical_lines = canonical_request.lines().collect::<Vec<_>>();
        assert_eq!(canonical_lines[0], "GET");
        assert_eq!(canonical_lines[1], "/storage/v1/s3/bucket/key");
        assert!(canonical_request.contains("\nhost:project.example:8443\n"));
        assert!(!canonical_request.contains("\nhost:project.example:8443/storage"));
        let signed_headers = headers[AUTHORIZATION]
            .to_str()
            .unwrap()
            .split("SignedHeaders=")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap();
        assert!(signed_headers.split(';').any(|header| header == "host"));

        let presigned = bucket.presign_get("/key", 3600, None).await.unwrap();
        let presigned_url = Url::parse(&presigned).unwrap();
        assert_eq!(presigned_url.host_str(), Some("project.example"));
        assert_eq!(presigned_url.port(), Some(8443));
        assert_eq!(presigned_url.path(), "/storage/v1/s3/bucket/key");
        assert!(
            presigned_url
                .query_pairs()
                .any(|(key, _)| key == "X-Amz-Signature")
        );
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn delete_object_does_not_sign_generated_body_headers() {
        let credentials =
            Credentials::new(Some("test-key"), Some("test-secret"), None, None, None).unwrap();
        let mut bucket = Bucket::new(
            "bucket",
            "us-east-1".parse::<Region>().unwrap(),
            credentials,
        )
        .unwrap();
        bucket.add_header("x-test-caller-extra", "kept");
        let request = make_request(&bucket, Command::DeleteObject).await.unwrap();
        let headers = request.headers().await.unwrap();

        assert!(!headers.contains_key(CONTENT_LENGTH));
        assert!(!headers.contains_key(CONTENT_TYPE));
        assert_eq!(headers["x-test-caller-extra"], "kept");
        let authorization = headers[AUTHORIZATION].to_str().unwrap();
        assert!(!authorization.contains("content-length"));
        assert!(!authorization.contains("content-type"));
        assert!(authorization.contains("x-test-caller-extra"));

        for command in [Command::GetObject, Command::HeadObject] {
            let request = make_request(&bucket, command).await.unwrap();
            let headers = request.headers().await.unwrap();
            assert!(!headers.contains_key(CONTENT_LENGTH));
            assert!(!headers.contains_key(CONTENT_TYPE));
        }

        bucket.add_header("content-length", "0");
        bucket.add_header("content-type", "application/caller");
        let request = make_request(&bucket, Command::DeleteObject).await.unwrap();
        let headers = request.headers().await.unwrap();
        assert_eq!(headers[CONTENT_LENGTH], "0");
        assert_eq!(headers[CONTENT_TYPE], "application/caller");
    }

    #[maybe_async::test(
        feature = "sync",
        async(all(not(feature = "sync"), feature = "with-tokio"), tokio::test),
        async(
            all(not(feature = "sync"), feature = "with-async-std"),
            async_std::test
        )
    )]
    async fn generated_body_headers_remain_for_put_and_xml_post() {
        let credentials =
            Credentials::new(Some("test-key"), Some("test-secret"), None, None, None).unwrap();
        let bucket = Bucket::new(
            "bucket",
            "us-east-1".parse::<Region>().unwrap(),
            credentials,
        )
        .unwrap();

        let empty_put = Command::PutObject {
            content: b"",
            content_type: "application/test",
            custom_headers: None,
            multipart: None,
        };
        let request = make_request(&bucket, empty_put).await.unwrap();
        let headers = request.headers().await.unwrap();
        assert_eq!(headers[CONTENT_LENGTH], "0");
        assert_eq!(headers[CONTENT_TYPE], "application/test");

        let put = Command::PutObject {
            content: b"abc",
            content_type: "application/test",
            custom_headers: None,
            multipart: None,
        };
        let request = make_request(&bucket, put).await.unwrap();
        let headers = request.headers().await.unwrap();
        assert_eq!(headers[CONTENT_LENGTH], "3");
        assert_eq!(headers[CONTENT_TYPE], "application/test");
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .contains("content-length")
        );
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .contains("content-type")
        );

        let request = make_request(&bucket, Command::CopyObject { from: "/source" })
            .await
            .unwrap();
        let headers = request.headers().await.unwrap();
        assert!(!headers.contains_key(CONTENT_LENGTH));
        assert!(!headers.contains_key(CONTENT_TYPE));
        assert_eq!(headers["x-amz-copy-source"], "/source");

        let post = Command::CompleteMultipartUpload {
            upload_id: "upload-id",
            data: crate::serde_types::CompleteMultipartUploadData {
                parts: vec![crate::serde_types::Part {
                    part_number: 1,
                    etag: "etag".into(),
                }],
            },
        };
        let expected_length = post.content_length().unwrap();
        let request = make_request(&bucket, post).await.unwrap();
        let headers = request.headers().await.unwrap();
        assert_eq!(headers[CONTENT_LENGTH], expected_length.to_string());
        assert_eq!(headers[CONTENT_TYPE], "application/xml");
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .contains("content-length")
        );
        assert!(
            headers[AUTHORIZATION]
                .to_str()
                .unwrap()
                .contains("content-type")
        );
    }
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub type DataStream = Pin<Box<dyn Stream<Item = StreamItem> + Send>>;
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub type StreamItem = Result<Bytes, S3Error>;

/// A response whose body is still arriving: `status_code` and `headers` are complete, and
/// `bytes` yields the body as the server sends it. The `AsyncRead` impl reads from `bytes` and
/// holds back the unread tail of a chunk; `bytes()` puts that tail back in front, so a reader
/// that switches to chunks midway should go through the accessor, not the field.
#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
pub struct ResponseDataStream {
    pub bytes: DataStream,
    pub status_code: u16,
    /// The response headers, so that a ranged GET's `Content-Range` and `Content-Length` can
    /// be checked before any of the body is read.
    pub headers: HeaderMap,
    /// What `AsyncRead` has taken from `bytes` but not yet handed out: the tail of the last
    /// chunk that did not fit the caller's buffer, or empty.
    pending: Bytes,
    /// Whether `AsyncRead` has seen `bytes` end. It then never polls `bytes` again: a body
    /// under a timeout may answer a late poll with that timeout rather than a second end, and
    /// some streams panic.
    ended: bool,
}

#[cfg(any(feature = "with-tokio", feature = "with-async-std"))]
impl ResponseDataStream {
    /// `bytes` yields the body as the server sends it; `status_code` and `headers` are the
    /// complete response head.
    ///
    /// ```
    /// use bytes::Bytes;
    /// use futures_util::{StreamExt, stream};
    /// use http::HeaderMap;
    /// use s3::request::ResponseDataStream;
    ///
    /// # async fn example() {
    /// let chunks = vec![Ok(Bytes::from("Hello, ")), Ok(Bytes::from("World!"))];
    /// let mut response =
    ///     ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());
    /// let mut body = Vec::new();
    /// while let Some(chunk) = response.bytes().next().await {
    ///     body.extend_from_slice(&chunk.unwrap());
    /// }
    /// assert_eq!(body, b"Hello, World!");
    /// # }
    /// ```
    pub fn new(bytes: DataStream, status_code: u16, headers: HeaderMap) -> ResponseDataStream {
        ResponseDataStream {
            bytes,
            status_code,
            headers,
            pending: Bytes::new(),
            ended: false,
        }
    }

    /// The rest of the body as chunks, beginning with whatever `AsyncRead` took from `bytes`
    /// but did not hand out. With `async_std::io::ReadExt` in scope, `stream.bytes()` resolves
    /// to that trait's byte iterator instead; call `ResponseDataStream::bytes(&mut stream)`.
    pub fn bytes(&mut self) -> &mut DataStream {
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            let rest = std::mem::replace(&mut self.bytes, Box::pin(stream::empty()));
            self.bytes = Box::pin(stream::iter([Ok(pending)]).chain(rest));
        }
        &mut self.bytes
    }

    /// Polls `bytes` until unread body bytes are at hand, then takes up to `max` of them; an
    /// empty result means the body has ended. Backs both `AsyncRead` impls.
    fn poll_take(&mut self, cx: &mut Context<'_>, max: usize) -> Poll<std::io::Result<Bytes>> {
        while self.pending.is_empty() {
            if self.ended {
                return Poll::Ready(Ok(Bytes::new()));
            }
            match ready!(self.bytes.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => self.pending = chunk,
                Some(Err(S3Error::Io(error))) => return Poll::Ready(Err(error)),
                Some(Err(error)) => return Poll::Ready(Err(std::io::Error::other(error))),
                None => self.ended = true,
            }
        }
        let amt = std::cmp::min(self.pending.len(), max);
        Poll::Ready(Ok(self.pending.split_to(amt)))
    }
}

impl From<ResponseData> for Vec<u8> {
    fn from(data: ResponseData) -> Vec<u8> {
        data.to_vec()
    }
}

impl ResponseData {
    pub fn new(bytes: Bytes, status_code: u16, headers: HashMap<String, String>) -> ResponseData {
        ResponseData {
            bytes,
            status_code,
            headers,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn bytes_mut(&mut self) -> &mut Bytes {
        &mut self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice())
    }

    pub fn to_string(&self) -> Result<String, std::str::Utf8Error> {
        std::str::from_utf8(self.as_slice()).map(|s| s.to_string())
    }

    pub fn headers(&self) -> HashMap<String, String> {
        self.headers.clone()
    }
}

use std::fmt;

impl fmt::Display for ResponseData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Status code: {}\n Data: {}",
            self.status_code(),
            self.to_string()
                .unwrap_or_else(|_| "Data could not be cast to UTF string".to_string())
        )
    }
}

#[cfg(feature = "with-tokio")]
impl tokio::io::AsyncRead for ResponseDataStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let chunk = ready!(self.poll_take(cx, buf.remaining()))?;
        buf.put_slice(&chunk);
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "with-async-std")]
impl async_std::io::Read for ResponseDataStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let chunk = ready!(self.poll_take(cx, buf.len()))?;
        buf[..chunk.len()].copy_from_slice(&chunk);
        Poll::Ready(Ok(chunk.len()))
    }
}

/// Where `request` goes, from its bucket, path, and command.
fn request_url<R: Request + ?Sized>(request: &R) -> Result<RequestUrl, S3Error> {
    RequestUrl::new(&request.bucket(), &request.path(), &request.command())
}

#[maybe_async::maybe_async]
pub trait Request {
    type Response;
    type HeaderMap;

    async fn response(&self) -> Result<Self::Response, S3Error>;
    async fn response_data(&self, etag: bool) -> Result<ResponseData, S3Error>;
    #[cfg(feature = "with-tokio")]
    async fn response_data_to_writer<T: tokio::io::AsyncWrite + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(feature = "with-async-std")]
    async fn response_data_to_writer<T: async_std::io::Write + Send + Unpin + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(feature = "sync")]
    fn response_data_to_writer<T: std::io::Write + Send + ?Sized>(
        &self,
        writer: &mut T,
    ) -> Result<u16, S3Error>;
    #[cfg(any(feature = "with-async-std", feature = "with-tokio"))]
    async fn response_data_to_stream(&self) -> Result<ResponseDataStream, S3Error>;
    async fn response_header(&self) -> Result<(Self::HeaderMap, u16), S3Error>;
    async fn response_status(&self) -> Result<u16, S3Error> {
        let (_, status_code) = self.response_header().await?;
        Ok(status_code)
    }
    fn datetime(&self) -> OffsetDateTime;
    fn bucket(&self) -> Bucket;
    fn command(&self) -> Command<'_>;
    fn path(&self) -> String;

    async fn signing_key(&self) -> Result<Vec<u8>, S3Error> {
        signing::signing_key(
            &self.datetime(),
            &self
                .bucket()
                .secret_key()
                .await?
                .expect("Secret key must be provided to sign headers, found None"),
            &self.bucket().region(),
            "s3",
        )
    }

    fn request_body(&self) -> Result<Vec<u8>, S3Error> {
        let result = if let Command::PutObject { content, .. } = self.command() {
            Vec::from(content)
        } else if let Command::PutObjectTagging { tags } = self.command() {
            Vec::from(tags)
        } else if let Command::UploadPart { content, .. } = self.command() {
            Vec::from(content)
        } else if let Command::CompleteMultipartUpload { data, .. } = &self.command() {
            let body = data.to_string();
            body.as_bytes().to_vec()
        } else if let Command::CreateBucket { config } = &self.command() {
            if let Some(payload) = config.location_constraint_payload() {
                Vec::from(payload)
            } else {
                Vec::new()
            }
        } else if let Command::PutBucketLifecycle { configuration, .. } = &self.command() {
            quick_xml::se::to_string(configuration)?.as_bytes().to_vec()
        } else if let Command::PutBucketCors { configuration, .. } = &self.command() {
            let cors = configuration.to_string();
            cors.as_bytes().to_vec()
        } else if let Command::PutBucketPolicy { policy } = &self.command() {
            policy.as_bytes().to_vec()
        } else if let Command::DeleteObjects { data } = &self.command() {
            data.to_string().as_bytes().to_vec()
        } else {
            Vec::new()
        };
        Ok(result)
    }

    fn long_date(&self) -> Result<String, S3Error> {
        Ok(self.datetime().format(LONG_DATETIME)?)
    }

    fn string_to_sign(&self, request: &str) -> Result<String, S3Error> {
        signing::string_to_sign(&self.datetime(), &self.bucket().region(), request)
    }

    /// The URL this request is sent to. Fails with `S3Error::EmptyKey` for an object command
    /// without a key, and with `S3Error::DotSegmentKey` for a key with a `.` or `..` segment,
    /// which URL parsing would resolve into another key, or in path style into another bucket.
    fn url(&self) -> Result<Url, S3Error> {
        request_url(self)?.to_url()
    }

    /// The presigned URL of a `PresignGet`, `PresignPut`, or `PresignDelete`: the request's
    /// URL, then the signing parameters, the caller's queries, and last the signature over all
    /// of it, the `Host` header, and for a PUT the caller's headers.
    #[maybe_async::maybe_async]
    async fn presigned(&self) -> Result<String, S3Error> {
        let (expiry, custom_headers, custom_queries) = match self.command() {
            Command::PresignGet {
                expiry_secs,
                custom_queries,
            } => (expiry_secs, None, custom_queries),
            Command::PresignPut {
                expiry_secs,
                custom_headers,
                custom_queries,
            } => (expiry_secs, custom_headers, custom_queries),
            Command::PresignDelete { expiry_secs } => (expiry_secs, None, None),
            _ => unreachable!(),
        };
        let bucket = self.bucket();
        let credentials = bucket.credentials().await?;
        let secret_key = credentials.secret_key.ok_or(S3Error::Credentials(
            CredentialsError::ConfigMissingSecretKey,
        ))?;
        let token = credentials.security_token.or(credentials.session_token);
        let mut url = request_url(self)?;
        let auth_params = signing::authorization_query_params_no_sig(
            credentials.access_key.as_deref().unwrap_or_default(),
            &self.datetime(),
            &bucket.region(),
            expiry,
            custom_headers.as_ref(),
            token.as_deref(),
        )?;
        let custom_queries = custom_queries.into_iter().flatten();
        url.query.extend(
            auth_params
                .into_iter()
                .chain(custom_queries)
                .map(|(name, value)| (name, Some(value))),
        );
        let mut headers = HeaderMap::new();
        headers.insert(HOST, url.authority.parse()?);
        for (name, value) in custom_headers.iter().flatten() {
            headers.insert(name.clone(), value.clone());
        }
        let canonical_request = signing::canonical_request(
            &self.command().http_verb().to_string(),
            &url.to_url()?,
            &headers,
            "UNSIGNED-PAYLOAD",
        )?;
        let string_to_sign = self.string_to_sign(&canonical_request)?;
        let signing_key =
            signing::signing_key(&self.datetime(), &secret_key, &bucket.region(), "s3")?;
        let mut hmac = signing::HmacSha256::new_from_slice(&signing_key)?;
        hmac.update(string_to_sign.as_bytes());
        url.query.push((
            "X-Amz-Signature".to_owned(),
            Some(hex::encode(hmac.finalize().into_bytes())),
        ));
        Ok(url.to_string())
    }

    fn canonical_request(&self, headers: &HeaderMap) -> Result<String, S3Error> {
        signing::canonical_request(
            &self.command().http_verb().to_string(),
            &self.url()?,
            headers,
            &self.command().sha256()?,
        )
    }

    #[maybe_async::maybe_async]
    async fn authorization(&self, headers: &HeaderMap) -> Result<String, S3Error> {
        let canonical_request = self.canonical_request(headers)?;
        let string_to_sign = self.string_to_sign(&canonical_request)?;
        let mut hmac = signing::HmacSha256::new_from_slice(&self.signing_key().await?)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let signed_header = signing::signed_header_string(headers);
        signing::authorization_header(
            &self
                .bucket()
                .access_key()
                .await?
                .expect("No access_key provided"),
            &self.datetime(),
            &self.bucket().region(),
            &signed_header,
            &signature,
        )
    }

    #[maybe_async::maybe_async]
    async fn headers(&self) -> Result<HeaderMap, S3Error> {
        // Generate this once, but it's used in more than one place.
        let sha256 = self.command().sha256()?;

        // Start with extra_headers, that way our headers replace anything with
        // the same name.

        let mut headers = HeaderMap::new();

        for (k, v) in self.bucket().extra_headers.iter() {
            if k.as_str().starts_with("x-amz-meta-") {
                // metadata is invalid on any multipart command other than initiate
                match self.command() {
                    Command::UploadPart { .. }
                    | Command::AbortMultipartUpload { .. }
                    | Command::CompleteMultipartUpload { .. }
                    | Command::PutObject {
                        multipart: Some(_), ..
                    } => continue,
                    _ => (),
                }
            }
            headers.insert(k.clone(), v.clone());
        }

        // Append custom headers for PUT request if any
        if let Command::PutObject { custom_headers, .. } = self.command()
            && let Some(custom_headers) = custom_headers
        {
            for (k, v) in custom_headers.iter() {
                headers.insert(k.clone(), v.clone());
            }
        }

        headers.insert(HOST, request_url(self)?.authority.parse()?);

        if let Command::CopyObject { from } = self.command() {
            headers.insert(HeaderName::from_static("x-amz-copy-source"), from.parse()?);
        } else if !matches!(
            self.command().http_verb(),
            crate::command::HttpMethod::Get
                | crate::command::HttpMethod::Head
                | crate::command::HttpMethod::Delete
        ) {
            headers.insert(
                CONTENT_LENGTH,
                self.command().content_length()?.to_string().parse()?,
            );
            headers.insert(CONTENT_TYPE, self.command().content_type().parse()?);
        }
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            sha256.parse()?,
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            self.long_date()?.parse()?,
        );

        if let Some(session_token) = self.bucket().session_token().await? {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                session_token.parse()?,
            );
        } else if let Some(security_token) = self.bucket().security_token().await? {
            headers.insert(
                HeaderName::from_static("x-amz-security-token"),
                security_token.parse()?,
            );
        }

        if let Command::PutObjectTagging { tags } = self.command() {
            let digest = md5::compute(tags);
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::PutObject { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::UploadPart { content, .. } = self.command() {
            let digest = md5::compute(content);
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::GetObject {} = self.command() {
            headers.insert(ACCEPT, "application/octet-stream".to_string().parse()?);
        // headers.insert(header::ACCEPT_CHARSET, HeaderValue::from_str("UTF-8")?);
        } else if let Command::GetObjectRange { start, end } = self.command() {
            headers.insert(ACCEPT, "application/octet-stream".to_string().parse()?);

            let mut range = format!("bytes={}-", start);

            if let Some(end) = end {
                range.push_str(&end.to_string());
            }

            headers.insert(RANGE, range.parse()?);
        } else if let Command::CreateBucket { ref config } = self.command() {
            config.add_headers(&mut headers)?;
        } else if let Command::PutBucketLifecycle { ref configuration } = self.command() {
            let digest = md5::compute(to_string(configuration)?.as_bytes());
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::PutBucketCors {
            expected_bucket_owner,
            configuration,
            ..
        } = self.command()
        {
            let digest = md5::compute(configuration.to_string().as_bytes());
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);

            headers.insert(
                HeaderName::from_static("x-amz-expected-bucket-owner"),
                expected_bucket_owner.parse()?,
            );
        } else if let Command::PutBucketPolicy { policy } = self.command() {
            let digest = md5::compute(policy.as_bytes());
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        } else if let Command::GetBucketCors {
            expected_bucket_owner,
        } = self.command()
        {
            headers.insert(
                HeaderName::from_static("x-amz-expected-bucket-owner"),
                expected_bucket_owner.parse()?,
            );
        } else if let Command::DeleteBucketCors {
            expected_bucket_owner,
        } = self.command()
        {
            headers.insert(
                HeaderName::from_static("x-amz-expected-bucket-owner"),
                expected_bucket_owner.parse()?,
            );
        } else if let Command::GetObjectAttributes {
            expected_bucket_owner,
            ..
        } = self.command()
        {
            headers.insert(
                HeaderName::from_static("x-amz-expected-bucket-owner"),
                expected_bucket_owner.parse()?,
            );
            headers.insert(
                HeaderName::from_static("x-amz-object-attributes"),
                "ETag".parse()?,
            );
        } else if let Command::DeleteObjects { ref data } = self.command() {
            let body = data.to_string();
            let digest = md5::compute(body.as_bytes());
            let hash = general_purpose::STANDARD.encode(digest.as_ref());
            headers.insert(HeaderName::from_static("content-md5"), hash.parse()?);
        }

        // This must be last, as it signs the other headers, omitted if no secret key is provided
        if self.bucket().secret_key().await?.is_some() {
            let authorization = self.authorization(&headers).await?;
            headers.insert(AUTHORIZATION, authorization.parse()?);
        }

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(DATE, self.datetime().format(&Rfc2822)?.parse()?);

        Ok(headers)
    }
}

#[cfg(all(test, feature = "with-tokio"))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::stream;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_async_read_implementation() {
        // Create a mock stream with test data
        let chunks = vec![
            Ok(Bytes::from("Hello, ")),
            Ok(Bytes::from("World!")),
            Ok(Bytes::from(" This is a test.")),
        ];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        // Read all data using AsyncRead
        let mut buffer = Vec::new();
        response_stream.read_to_end(&mut buffer).await.unwrap();

        assert_eq!(buffer, b"Hello, World! This is a test.");
    }

    #[tokio::test]
    async fn test_async_read_with_small_buffer_preserves_large_chunk() {
        let expected = b"This is a much longer string that won't fit in a small buffer";
        let chunks = vec![Ok(Bytes::copy_from_slice(expected))];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        let mut output = Vec::new();
        let mut buffer = [0u8; 10];

        loop {
            let n = response_stream.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..n]);
        }

        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn test_async_read_with_error() {
        // The chunk before the error arrives whole, over short reads, and the error keeps
        // its io::ErrorKind
        let chunks: Vec<StreamItem> = vec![
            Ok(Bytes::from("Some data")),
            Err(S3Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Test error",
            ))),
        ];
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut buffer = [0u8; 4];
        let mut output = Vec::new();
        let error = loop {
            match response_stream.read(&mut buffer).await {
                Ok(0) => panic!("the body's error was not reported"),
                Ok(n) => output.extend_from_slice(&buffer[..n]),
                Err(error) => break error,
            }
        };

        assert_eq!(output, b"Some data");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn test_async_read_copy() {
        // Test using tokio::io::copy which is a common use case
        let chunks = vec![
            Ok(Bytes::from("First chunk\n")),
            Ok(Bytes::from("Second chunk\n")),
            Ok(Bytes::from("Third chunk\n")),
        ];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        let mut output = Vec::new();
        tokio::io::copy(&mut response_stream, &mut output)
            .await
            .unwrap();

        assert_eq!(output, b"First chunk\nSecond chunk\nThird chunk\n");
    }

    /// A body read with a buffer far smaller than its chunks. Every read but a chunk's last
    /// leaves a remainder; the reader must stay flat across more than 100k of them.
    #[tokio::test]
    async fn test_async_read_many_short_reads() {
        let body: Vec<u8> = (0..800_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<StreamItem> = body
            .chunks(100_000)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut output = Vec::with_capacity(body.len());
        let mut buffer = [0u8; 7];
        let mut reads = 0usize;
        loop {
            let n = response_stream.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..n]);
            reads += 1;
        }

        assert!(reads > 100_000, "{reads} reads");
        assert!(output == body, "streamed bytes differ from the body");
    }

    /// A reader that takes the head of the body through `AsyncRead` and the rest as chunks.
    #[tokio::test]
    async fn test_async_read_then_bytes_keeps_the_tail() {
        let chunks: Vec<StreamItem> = vec![Ok(Bytes::from("Hello")), Ok(Bytes::from("World"))];
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut head = [0u8; 3];
        response_stream.read_exact(&mut head).await.unwrap();
        let mut rest = Vec::new();
        while let Some(chunk) = response_stream.bytes().next().await {
            rest.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(&head, b"Hel");
        assert_eq!(rest, b"loWorld");
    }

    /// `stream::unfold` panics when polled after its end, so the read past the end must be
    /// answered without polling the body again.
    #[tokio::test]
    async fn test_async_read_past_the_end_does_not_poll_the_body() {
        let chunks: Vec<StreamItem> = vec![Ok(Bytes::from("data"))];
        let body = stream::unfold(chunks.into_iter(), |mut rest| async move {
            rest.next().map(|chunk| (chunk, rest))
        });
        let mut response_stream = ResponseDataStream::new(Box::pin(body), 200, HeaderMap::new());

        let mut output = Vec::new();
        response_stream.read_to_end(&mut output).await.unwrap();
        let mut buffer = [0u8; 4];
        let n = response_stream.read(&mut buffer).await.unwrap();

        assert_eq!(output, b"data");
        assert_eq!(n, 0);
    }
}

#[cfg(all(test, feature = "with-async-std"))]
mod async_std_tests {
    use super::*;
    use async_std::io::ReadExt;
    use bytes::Bytes;
    use futures_util::stream;

    #[async_std::test]
    async fn test_async_read_implementation() {
        // Create a mock stream with test data
        let chunks = vec![
            Ok(Bytes::from("Hello, ")),
            Ok(Bytes::from("World!")),
            Ok(Bytes::from(" This is a test.")),
        ];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        // Read all data using AsyncRead
        let mut buffer = Vec::new();
        response_stream.read_to_end(&mut buffer).await.unwrap();

        assert_eq!(buffer, b"Hello, World! This is a test.");
    }

    #[async_std::test]
    async fn test_async_read_with_small_buffer_preserves_large_chunk() {
        let expected = b"This is a much longer string that won't fit in a small buffer";
        let chunks = vec![Ok(Bytes::copy_from_slice(expected))];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        let mut output = Vec::new();
        let mut buffer = [0u8; 10];

        loop {
            let n = response_stream.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..n]);
        }

        assert_eq!(output, expected);
    }

    #[async_std::test]
    async fn test_async_read_with_error() {
        // The chunk before the error arrives whole, over short reads, and the error keeps
        // its io::ErrorKind
        let chunks: Vec<StreamItem> = vec![
            Ok(Bytes::from("Some data")),
            Err(S3Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Test error",
            ))),
        ];
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut buffer = [0u8; 4];
        let mut output = Vec::new();
        let error = loop {
            match response_stream.read(&mut buffer).await {
                Ok(0) => panic!("the body's error was not reported"),
                Ok(n) => output.extend_from_slice(&buffer[..n]),
                Err(error) => break error,
            }
        };

        assert_eq!(output, b"Some data");
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[async_std::test]
    async fn test_async_read_copy() {
        // Test using async_std::io::copy which is a common use case
        let chunks = vec![
            Ok(Bytes::from("First chunk\n")),
            Ok(Bytes::from("Second chunk\n")),
            Ok(Bytes::from("Third chunk\n")),
        ];

        let stream = stream::iter(chunks);
        let data_stream: DataStream = Box::pin(stream);

        let mut response_stream = ResponseDataStream::new(data_stream, 200, HeaderMap::new());

        let mut output = Vec::new();
        async_std::io::copy(&mut response_stream, &mut output)
            .await
            .unwrap();

        assert_eq!(output, b"First chunk\nSecond chunk\nThird chunk\n");
    }

    /// A body read with a buffer far smaller than its chunks. Every read but a chunk's last
    /// leaves a remainder; the reader must stay flat across more than 100k of them.
    #[async_std::test]
    async fn test_async_read_many_short_reads() {
        let body: Vec<u8> = (0..800_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<StreamItem> = body
            .chunks(100_000)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut output = Vec::with_capacity(body.len());
        let mut buffer = [0u8; 7];
        let mut reads = 0usize;
        loop {
            let n = response_stream.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..n]);
            reads += 1;
        }

        assert!(reads > 100_000, "{reads} reads");
        assert!(output == body, "streamed bytes differ from the body");
    }

    /// A reader that takes the head of the body through `AsyncRead` and the rest as chunks.
    /// `ReadExt` is in scope here, so its by-value `bytes()` would shadow the accessor.
    #[async_std::test]
    async fn test_async_read_then_bytes_keeps_the_tail() {
        let chunks: Vec<StreamItem> = vec![Ok(Bytes::from("Hello")), Ok(Bytes::from("World"))];
        let mut response_stream =
            ResponseDataStream::new(Box::pin(stream::iter(chunks)), 200, HeaderMap::new());

        let mut head = [0u8; 3];
        response_stream.read_exact(&mut head).await.unwrap();
        let mut rest = Vec::new();
        while let Some(chunk) = ResponseDataStream::bytes(&mut response_stream).next().await {
            rest.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(&head, b"Hel");
        assert_eq!(rest, b"loWorld");
    }

    /// `stream::unfold` panics when polled after its end, so the read past the end must be
    /// answered without polling the body again.
    #[async_std::test]
    async fn test_async_read_past_the_end_does_not_poll_the_body() {
        let chunks: Vec<StreamItem> = vec![Ok(Bytes::from("data"))];
        let body = stream::unfold(chunks.into_iter(), |mut rest| async move {
            rest.next().map(|chunk| (chunk, rest))
        });
        let mut response_stream = ResponseDataStream::new(Box::pin(body), 200, HeaderMap::new());

        let mut output = Vec::new();
        response_stream.read_to_end(&mut output).await.unwrap();
        let mut buffer = [0u8; 4];
        let n = response_stream.read(&mut buffer).await.unwrap();

        assert_eq!(output, b"data");
        assert_eq!(n, 0);
    }
}

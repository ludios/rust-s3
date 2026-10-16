// Model-output: Claude Fable 5.1

//! The URL a command is sent to or presigned for, built from the bucket, the key, and the
//! command, and kept as parts. Two things need the parts rather than a `Url`: SigV4 signs the
//! `Host` header as the endpoint spells it, and a presigned URL must spell it the same way,
//! while `Url` drops a scheme's default port; and the path and query must reach the server as
//! built, which `Url` only guarantees for text it has nothing to rewrite.

use std::fmt;

use url::Url;

use crate::bucket::Bucket;
use crate::command::Command;
use crate::error::S3Error;
use crate::signing::uri_encode;

/// Where a request goes: `scheme://authority{path}?{query}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RequestUrl {
    pub(crate) scheme: String,
    /// Host and port as the endpoint spells them: the `Host` header.
    pub(crate) authority: String,
    /// The absolute path, percent-encoded: the endpoint's own path, `/bucket` in path style,
    /// then `/key`.
    pub(crate) path: String,
    /// The query as names and values, decoded, in the order sent.
    pub(crate) query: Vec<(String, String)>,
}

/// `host`, as `Region::host` gives it, split into its authority and the path that may follow:
/// a custom endpoint may carry one, as Supabase Storage's
/// `https://<project>.supabase.co/storage/v1/s3` does. RFC 9110 §7.2 defines `Host` as
/// `uri-host [ ":" port ]`, so that path cannot travel in the header.
fn split_authority(host: &str) -> (&str, &str) {
    host.split_at(host.find('/').unwrap_or(host.len()))
}

/// Whether `key` has a `.` or `..` segment, which URL parsing would resolve away.
fn has_dot_segment(key: &str) -> bool {
    key.split('/')
        .any(|segment| segment == "." || segment == "..")
}

/// The query that addresses part `part_number` of multipart upload `upload_id`.
fn part_query(part_number: u32, upload_id: &str) -> Vec<(String, String)> {
    vec![
        ("partNumber".to_owned(), part_number.to_string()),
        ("uploadId".to_owned(), upload_id.to_owned()),
    ]
}

/// The query `command` carries: its subresource, then its parameters in the order the S3
/// documentation lists them.
fn command_query(command: &Command<'_>) -> Vec<(String, String)> {
    let pair = |name: &str, value: &str| (name.to_owned(), value.to_owned());
    let flag = |name: &str| pair(name, "");
    let optional = |query: &mut Vec<(String, String)>, name: &str, value: Option<&str>| {
        if let Some(value) = value {
            query.push(pair(name, value));
        }
    };
    match command {
        Command::InitiateMultipartUpload { .. } => vec![flag("uploads")],
        Command::ListMultipartUploads {
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
        } => {
            let mut query = vec![flag("uploads")];
            optional(&mut query, "delimiter", *delimiter);
            optional(&mut query, "prefix", *prefix);
            optional(&mut query, "key-marker", key_marker.as_deref());
            optional(&mut query, "upload-id-marker", upload_id_marker.as_deref());
            let max_uploads = max_uploads.map(|n| n.to_string());
            optional(&mut query, "max-uploads", max_uploads.as_deref());
            query
        }
        Command::PutObject {
            multipart: Some(multipart),
            ..
        } => part_query(multipart.part_number, multipart.upload_id),
        Command::UploadPart {
            part_number,
            upload_id,
            ..
        } => part_query(*part_number, upload_id),
        Command::AbortMultipartUpload { upload_id }
        | Command::CompleteMultipartUpload { upload_id, .. } => vec![pair("uploadId", upload_id)],
        Command::ListObjects {
            prefix,
            delimiter,
            marker,
            max_keys,
        } => {
            let mut query = Vec::new();
            optional(&mut query, "delimiter", delimiter.as_deref());
            query.push(pair("prefix", prefix));
            optional(&mut query, "marker", marker.as_deref());
            let max_keys = max_keys.map(|n| n.to_string());
            optional(&mut query, "max-keys", max_keys.as_deref());
            query
        }
        Command::ListObjectsV2 {
            prefix,
            delimiter,
            continuation_token,
            start_after,
            max_keys,
        } => {
            let mut query = Vec::new();
            optional(&mut query, "delimiter", delimiter.as_deref());
            query.push(pair("prefix", prefix));
            query.push(pair("list-type", "2"));
            optional(
                &mut query,
                "continuation-token",
                continuation_token.as_deref(),
            );
            optional(&mut query, "start-after", start_after.as_deref());
            let max_keys = max_keys.map(|n| n.to_string());
            optional(&mut query, "max-keys", max_keys.as_deref());
            query
        }
        Command::GetObjectTorrent => vec![flag("torrent")],
        Command::GetObjectTagging
        | Command::PutObjectTagging { .. }
        | Command::DeleteObjectTagging => vec![flag("tagging")],
        Command::GetObjectAttributes { version_id, .. } => {
            let mut query = vec![flag("attributes")];
            optional(&mut query, "versionId", version_id.as_deref());
            query
        }
        Command::GetBucketLocation => vec![flag("location")],
        Command::GetBucketLifecycle
        | Command::PutBucketLifecycle { .. }
        | Command::DeleteBucketLifecycle => vec![flag("lifecycle")],
        Command::GetBucketCors { .. }
        | Command::PutBucketCors { .. }
        | Command::DeleteBucketCors { .. } => vec![flag("cors")],
        Command::GetBucketPolicy
        | Command::PutBucketPolicy { .. }
        | Command::DeleteBucketPolicy => vec![flag("policy")],
        Command::DeleteObjects { .. } => vec![flag("delete")],
        Command::HeadObject
        | Command::CopyObject { .. }
        | Command::DeleteObject
        | Command::GetObject
        | Command::GetObjectRange { .. }
        | Command::PutObject {
            multipart: None, ..
        }
        | Command::PresignGet { .. }
        | Command::PresignPut { .. }
        | Command::PresignDelete { .. }
        | Command::CreateBucket { .. }
        | Command::DeleteBucket
        | Command::ListBuckets => Vec::new(),
    }
}

/// The query percent-encoded as SigV4 canonicalizes it, so what is sent is what is signed. A
/// name with an empty value is sent bare, as AWS's own clients send `?uploads`.
fn encoded_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(name, value)| {
            let name = uri_encode(name, true);
            if value.is_empty() {
                name
            } else {
                format!("{name}={}", uri_encode(value, true))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

impl RequestUrl {
    /// Where `command` on `bucket` goes, at the object `key`; a leading `/` is not part of the
    /// key, and `ListBuckets` and `CreateBucket` take none. Fails with `S3Error::EmptyKey` for
    /// an object command without a key, whose URL would address the bucket itself, and with
    /// `S3Error::DotSegmentKey` for a key with a `.` or `..` segment, which URL parsing would
    /// resolve into another key, or in path style into another bucket.
    pub(crate) fn new(bucket: &Bucket, key: &str, command: &Command<'_>) -> Result<Self, S3Error> {
        let stripped = key.strip_prefix('/').unwrap_or(key);
        if stripped.is_empty() && command.addresses_object() {
            return Err(S3Error::EmptyKey(key.to_owned()));
        }
        if has_dot_segment(stripped) {
            return Err(S3Error::DotSegmentKey(key.to_owned()));
        }
        // ListBuckets addresses the service, whichever bucket it is asked through.
        let service = matches!(command, Command::ListBuckets);
        let host = if service {
            bucket.region().host()
        } else {
            bucket.host()
        };
        let (authority, endpoint_path) = split_authority(&host);
        let mut path = endpoint_path.to_owned();
        if bucket.is_path_style() && !service {
            path.push('/');
            path.push_str(&uri_encode(&bucket.name(), true));
        }
        if !matches!(command, Command::ListBuckets | Command::CreateBucket { .. }) {
            path.push('/');
            path.push_str(&uri_encode(stripped, false));
        }
        if path.is_empty() {
            path.push('/');
        }
        let mut query = command_query(command);
        query.extend(
            bucket
                .extra_query()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        );
        Ok(RequestUrl {
            scheme: bucket.scheme(),
            authority: authority.to_owned(),
            path,
            query,
        })
    }

    /// The URL as the HTTP client and the canonical request see it. `Url` would resolve a `.`
    /// or `..` segment of the bucket name or of the endpoint's path (keys are refused in
    /// `new`), so a path it did not keep is an error rather than a request for something else.
    pub(crate) fn to_url(&self) -> Result<Url, S3Error> {
        let url = Url::parse(&self.to_string())?;
        if url.path() != self.path {
            return Err(S3Error::PathRewritten {
                built: self.path.clone(),
                parsed: url.path().to_owned(),
            });
        }
        Ok(url)
    }
}

impl fmt::Display for RequestUrl {
    /// The URL with the authority as the endpoint spells it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}{}", self.scheme, self.authority, self.path)?;
        if !self.query.is_empty() {
            write!(f, "?{}", encoded_query(&self.query))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::Region;
    use awscreds::Credentials;

    /// `name` at `endpoint`, in path style.
    fn bucket(name: &str, endpoint: &str) -> Bucket {
        let credentials = Credentials::new(Some("k"), Some("s"), None, None, None).unwrap();
        let region = Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: endpoint.to_owned(),
        };
        *Bucket::new(name, region, credentials)
            .unwrap()
            .with_path_style()
    }

    #[test]
    fn split_authority_keeps_a_plain_host() {
        assert_eq!(
            split_authority("s3.eu-central-1.amazonaws.com"),
            ("s3.eu-central-1.amazonaws.com", "")
        );
        assert_eq!(split_authority("127.0.0.1:9000"), ("127.0.0.1:9000", ""));
    }

    #[test]
    fn split_authority_sets_an_endpoint_path_apart() {
        assert_eq!(
            split_authority("project.supabase.co/storage/v1/s3"),
            ("project.supabase.co", "/storage/v1/s3")
        );
        assert_eq!(
            split_authority("127.0.0.1:54321/storage/v1/s3"),
            ("127.0.0.1:54321", "/storage/v1/s3")
        );
    }

    #[test]
    fn a_bucket_name_or_endpoint_path_the_url_parser_would_resolve_is_refused() {
        for (name, endpoint) in [
            ("..", "http://localhost:9000"),
            ("b", "http://localhost:9000/a/../c"),
        ] {
            let url = RequestUrl::new(&bucket(name, endpoint), "key", &Command::GetObject).unwrap();
            let parsed = url.to_url();
            assert!(
                matches!(parsed, Err(S3Error::PathRewritten { .. })),
                "{name} at {endpoint}: {parsed:?}"
            );
        }
    }

    #[test]
    fn a_flag_is_sent_bare_and_values_are_encoded_as_they_are_signed() {
        let url = RequestUrl {
            scheme: "http".to_owned(),
            authority: "h:80".to_owned(),
            path: "/b/k".to_owned(),
            query: vec![
                ("uploads".to_owned(), String::new()),
                ("uploadId".to_owned(), "a+b/c d~".to_owned()),
            ],
        };
        assert_eq!(
            url.to_string(),
            "http://h:80/b/k?uploads&uploadId=a%2Bb%2Fc%20d~"
        );
        assert_eq!(
            url.to_url().unwrap().as_str(),
            "http://h/b/k?uploads&uploadId=a%2Bb%2Fc%20d~"
        );
    }
}

// Model-output: Claude Fable 5.1

//! Where each command is sent and what a presigned URL spells, pinned on every backend: the
//! path and query, the `Host` header, the canonical request that gets signed, and the
//! presigned query, in subdomain and path style, at an endpoint with a port and a path, and
//! under arbitrary keys and queries.

use super::Request;
use crate::bucket::Bucket;
use crate::bucket_ops::BucketConfiguration;
use crate::command::{Command, Multipart};
use crate::error::S3Error;
use crate::region::Region;
use crate::serde_types::{
    BucketLifecycleConfiguration, CompleteMultipartUploadData, CorsConfiguration,
    DeleteObjectsRequest,
};
use crate::signing;
use awscreds::Credentials;
use hmac::Mac;
use http::HeaderMap;
use http::header::{CONTENT_TYPE, HOST};
use proptest::prelude::*;
use std::collections::HashMap;
use time::{Date, Month, OffsetDateTime};
use url::Url;

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

#[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// Completes a `maybe_async` call on this backend's runtime; under `sync` it already is.
#[cfg(feature = "sync")]
macro_rules! run {
    ($call:expr) => {
        $call
    };
}
#[cfg(all(not(feature = "sync"), feature = "with-tokio"))]
macro_rules! run {
    ($call:expr) => {
        RUNTIME.block_on($call)
    };
}
#[cfg(all(
    not(feature = "sync"),
    not(feature = "with-tokio"),
    feature = "with-async-std"
))]
macro_rules! run {
    ($call:expr) => {
        async_std::task::block_on($call)
    };
}

const ACCESS_KEY: &str = "test_access_key";
const SECRET_KEY: &str = "test_secret_key";
/// A key with a space and a `+`, which must not survive as themselves.
const KEY: &str = "/dir/a b+c.txt";
const ENCODED_KEY: &str = "dir/a%20b%2Bc.txt";
/// The query of every presigned URL, in order, before anything a caller adds.
const AUTH_KEYS: [&str; 5] = [
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
];

fn credentials(security_token: Option<&str>) -> Credentials {
    Credentials::new(
        Some(ACCESS_KEY),
        Some(SECRET_KEY),
        security_token,
        None,
        None,
    )
    .unwrap()
}

fn custom(endpoint: &str) -> Region {
    Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

/// `test-bucket` at `endpoint`, addressed in path style.
fn path_style(endpoint: &str) -> Bucket {
    *Bucket::new("test-bucket", custom(endpoint), credentials(None))
        .unwrap()
        .with_path_style()
}

/// The URL `command` at `path` is sent to, and the `Host` header it carries.
#[maybe_async::maybe_async]
async fn sent(bucket: &Bucket, path: &str, command: Command<'_>) -> Result<(Url, String), S3Error> {
    let request = BackendRequest::new(bucket, path, command).await?;
    let url = request.url()?;
    let host = request.headers().await?[HOST].to_str().unwrap().to_owned();
    Ok((url, host))
}

/// Every command, the path it is sent at, and the path and query of its URL in path style on
/// `test-bucket`. In subdomain style the leading `/test-bucket` moves into the host.
fn commands() -> Vec<(&'static str, Command<'static>, String)> {
    let object = |query: &str| format!("/test-bucket/{ENCODED_KEY}{query}");
    let bucket = |query: &str| format!("/test-bucket/{query}");
    let put = |multipart| Command::PutObject {
        content: b"x",
        content_type: "text/plain",
        custom_headers: None,
        multipart,
    };
    vec![
        (KEY, Command::HeadObject, object("")),
        (KEY, Command::GetObject, object("")),
        (
            KEY,
            Command::GetObjectRange {
                start: 0,
                end: Some(9),
            },
            object(""),
        ),
        (KEY, Command::GetObjectTorrent, object("?torrent")),
        (KEY, Command::GetObjectTagging, object("?tagging")),
        (
            KEY,
            Command::PutObjectTagging { tags: "<Tagging/>" },
            object("?tagging"),
        ),
        (KEY, Command::DeleteObjectTagging, object("?tagging")),
        (KEY, Command::DeleteObject, object("")),
        (KEY, Command::CopyObject { from: "/src" }, object("")),
        (KEY, put(None), object("")),
        (
            KEY,
            put(Some(Multipart::new(3, "up-id"))),
            object("?partNumber=3&uploadId=up-id"),
        ),
        (
            KEY,
            Command::UploadPart {
                part_number: 3,
                content: b"x",
                upload_id: "up-id",
            },
            object("?partNumber=3&uploadId=up-id"),
        ),
        (
            KEY,
            Command::InitiateMultipartUpload {
                content_type: "text/plain",
            },
            object("?uploads"),
        ),
        (
            KEY,
            Command::AbortMultipartUpload { upload_id: "up-id" },
            object("?uploadId=up-id"),
        ),
        (
            KEY,
            Command::CompleteMultipartUpload {
                upload_id: "up-id",
                data: CompleteMultipartUploadData { parts: vec![] },
            },
            object("?uploadId=up-id"),
        ),
        (
            KEY,
            Command::GetObjectAttributes {
                expected_bucket_owner: "owner".into(),
                version_id: Some("v1".into()),
            },
            object("?attributes&versionId=v1"),
        ),
        (
            KEY,
            Command::GetObjectAttributes {
                expected_bucket_owner: "owner".into(),
                version_id: None,
            },
            object("?attributes"),
        ),
        (
            "",
            Command::ListMultipartUploads {
                prefix: Some("p"),
                delimiter: Some("/"),
                key_marker: Some("k".into()),
                upload_id_marker: Some("u".into()),
                max_uploads: Some(5),
            },
            bucket("?uploads&delimiter=%2F&prefix=p&key-marker=k&upload-id-marker=u&max-uploads=5"),
        ),
        (
            "",
            Command::ListObjects {
                prefix: "p".into(),
                delimiter: Some("/".into()),
                marker: Some("m".into()),
                max_keys: Some(10),
            },
            bucket("?delimiter=%2F&prefix=p&marker=m&max-keys=10"),
        ),
        (
            "",
            Command::ListObjectsV2 {
                prefix: "p".into(),
                delimiter: Some("/".into()),
                continuation_token: Some("t".into()),
                start_after: Some("s".into()),
                max_keys: Some(10),
            },
            bucket(
                "?delimiter=%2F&prefix=p&list-type=2&continuation-token=t&start-after=s&max-keys=10",
            ),
        ),
        (
            "",
            Command::CreateBucket {
                config: BucketConfiguration::default(),
            },
            "/test-bucket".to_owned(),
        ),
        ("", Command::DeleteBucket, bucket("")),
        ("", Command::GetBucketLocation, bucket("?location")),
        (
            "",
            Command::GetBucketCors {
                expected_bucket_owner: "owner".into(),
            },
            bucket("?cors"),
        ),
        (
            "",
            Command::PutBucketCors {
                expected_bucket_owner: "owner".into(),
                configuration: CorsConfiguration::new(vec![]),
            },
            bucket("?cors"),
        ),
        (
            "",
            Command::DeleteBucketCors {
                expected_bucket_owner: "owner".into(),
            },
            bucket("?cors"),
        ),
        ("", Command::GetBucketLifecycle, bucket("?lifecycle")),
        (
            "",
            Command::PutBucketLifecycle {
                configuration: BucketLifecycleConfiguration::new(vec![]),
            },
            bucket("?lifecycle"),
        ),
        ("", Command::DeleteBucketLifecycle, bucket("?lifecycle")),
        ("", Command::GetBucketPolicy, bucket("?policy")),
        (
            "",
            Command::PutBucketPolicy {
                policy: "{}".into(),
            },
            bucket("?policy"),
        ),
        ("", Command::DeleteBucketPolicy, bucket("?policy")),
        (
            "/",
            Command::DeleteObjects {
                data: DeleteObjectsRequest {
                    objects: vec![],
                    quiet: false,
                },
            },
            bucket("?delete"),
        ),
    ]
}

/// One way of addressing `test-bucket`: what precedes the path-style path and query of
/// `commands()` in the URL, the `Host` header, and whether the bucket stays in the path.
struct Style {
    bucket: Bucket,
    url_prefix: &'static str,
    host: &'static str,
    bucket_in_path: bool,
}

fn styles() -> Vec<Style> {
    vec![
        Style {
            bucket: path_style("http://localhost:9000"),
            url_prefix: "http://localhost:9000",
            host: "localhost:9000",
            bucket_in_path: true,
        },
        Style {
            bucket: *Bucket::new("test-bucket", Region::UsEast1, credentials(None)).unwrap(),
            url_prefix: "https://test-bucket.s3.amazonaws.com",
            host: "test-bucket.s3.amazonaws.com",
            bucket_in_path: false,
        },
        // An endpoint's path precedes the bucket; its port is not the scheme's default, so the
        // URL keeps it too.
        Style {
            bucket: path_style("https://project.example:8443/storage/v1/s3"),
            url_prefix: "https://project.example:8443/storage/v1/s3",
            host: "project.example:8443",
            bucket_in_path: true,
        },
        // `Url` drops a scheme's default port; the `Host` header keeps it as the endpoint
        // spells it.
        Style {
            bucket: path_style("http://minio:80"),
            url_prefix: "http://minio",
            host: "minio:80",
            bucket_in_path: true,
        },
    ]
}

fn expected_url(style: &Style, path_and_query: &str) -> String {
    let tail = if style.bucket_in_path {
        path_and_query
    } else {
        match path_and_query.strip_prefix("/test-bucket").unwrap() {
            "" => "/",
            tail => tail,
        }
    };
    format!("{}{tail}", style.url_prefix)
}

#[test]
fn every_command_is_sent_to_its_pinned_url_and_host() {
    for style in styles() {
        for (path, command, path_and_query) in commands() {
            let name = format!("{command:?} at {path:?} on {}", style.url_prefix);
            let (url, host) = run!(sent(&style.bucket, path, command)).unwrap();
            assert_eq!(
                url.as_str(),
                expected_url(&style, &path_and_query),
                "{name}"
            );
            assert_eq!(host, style.host, "{name}");
        }
    }
}

#[test]
fn list_buckets_is_sent_to_the_service_root_whichever_bucket_asks() {
    let cases = [
        (
            *Bucket::new("", custom("http://localhost:9000"), credentials(None))
                .unwrap()
                .with_path_style(),
            "http://localhost:9000/",
            "localhost:9000",
        ),
        // `Bucket::exists` asks through a subdomain-style bucket, whose own host names it.
        (
            *Bucket::new("test-bucket", Region::UsEast1, credentials(None)).unwrap(),
            "https://s3.amazonaws.com/",
            "s3.amazonaws.com",
        ),
        (
            path_style("https://project.example:8443/storage/v1/s3"),
            "https://project.example:8443/storage/v1/s3/",
            "project.example:8443",
        ),
    ];
    for (bucket, expected_url, expected_host) in cases {
        let (url, host) = run!(sent(&bucket, "", Command::ListBuckets)).unwrap();
        assert_eq!(url.as_str(), expected_url);
        assert_eq!(host, expected_host);
    }
}

#[test]
fn an_empty_parameter_value_keeps_its_equals_sign_and_a_flag_has_none() {
    let bucket = path_style("http://localhost:9000");
    let listing = Command::ListObjectsV2 {
        prefix: String::new(),
        delimiter: None,
        continuation_token: None,
        start_after: None,
        max_keys: None,
    };
    let (url, _) = run!(sent(&bucket, "", listing)).unwrap();
    assert_eq!(
        url.as_str(),
        "http://localhost:9000/test-bucket/?prefix=&list-type=2"
    );
    let initiate = Command::InitiateMultipartUpload {
        content_type: "text/plain",
    };
    let (url, _) = run!(sent(&bucket, KEY, initiate)).unwrap();
    assert_eq!(
        url.as_str(),
        format!("http://localhost:9000/test-bucket/{ENCODED_KEY}?uploads")
    );
}

#[test]
fn reserved_characters_in_ids_are_percent_encoded_and_round_trip() {
    let bucket = path_style("http://localhost:9000");
    let id = "a+b/c=d&e?f g";
    let encoded = "a%2Bb%2Fc%3Dd%26e%3Ff%20g";
    let cases = [
        (
            Command::AbortMultipartUpload { upload_id: id },
            format!("?uploadId={encoded}"),
        ),
        (
            Command::PutObject {
                content: b"x",
                content_type: "text/plain",
                custom_headers: None,
                multipart: Some(Multipart::new(1, id)),
            },
            format!("?partNumber=1&uploadId={encoded}"),
        ),
        (
            Command::GetObjectAttributes {
                expected_bucket_owner: "owner".into(),
                version_id: Some(id.into()),
            },
            format!("?attributes&versionId={encoded}"),
        ),
    ];
    for (command, query) in cases {
        let (url, _) = run!(sent(&bucket, KEY, command)).unwrap();
        assert_eq!(
            url.as_str(),
            format!("http://localhost:9000/test-bucket/{ENCODED_KEY}{query}")
        );
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        assert_eq!(pairs.last().unwrap().1, id);
    }
}

#[test]
fn the_canonical_uri_and_query_are_the_url_path_and_its_sorted_pairs() {
    let bucket = path_style("http://localhost:9000");
    let cases = [
        (
            KEY,
            Command::GetObject,
            format!("/test-bucket/{ENCODED_KEY}"),
            "",
        ),
        (
            KEY,
            Command::InitiateMultipartUpload {
                content_type: "text/plain",
            },
            format!("/test-bucket/{ENCODED_KEY}"),
            "uploads=",
        ),
        (
            "",
            Command::ListObjectsV2 {
                prefix: "p".into(),
                delimiter: Some("/".into()),
                continuation_token: Some("t".into()),
                start_after: Some("s".into()),
                max_keys: Some(10),
            },
            "/test-bucket/".to_owned(),
            "continuation-token=t&delimiter=%2F&list-type=2&max-keys=10&prefix=p&start-after=s",
        ),
    ];
    for (path, command, uri, query) in cases {
        let request = run!(BackendRequest::new(&bucket, path, command)).unwrap();
        let canonical = request.canonical_request(&HeaderMap::new()).unwrap();
        let lines: Vec<&str> = canonical.lines().collect();
        assert_eq!(lines[1], uri);
        assert_eq!(lines[2], query);
    }
}

#[test]
fn extra_query_joins_the_commands_own_query() {
    let mut bucket = path_style("http://localhost:9000");
    bucket.add_query("x-id", "GetObject");
    bucket.add_query("k", "a&b=c");
    let cases = [
        (KEY, Command::GetObject, vec![]),
        (
            KEY,
            Command::InitiateMultipartUpload {
                content_type: "text/plain",
            },
            vec![("uploads", "")],
        ),
        (
            "",
            Command::ListObjectsV2 {
                prefix: "p".into(),
                delimiter: None,
                continuation_token: None,
                start_after: None,
                max_keys: None,
            },
            vec![("prefix", "p"), ("list-type", "2")],
        ),
    ];
    for (path, command, own) in cases {
        let (url, _) = run!(sent(&bucket, path, command)).unwrap();
        let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let mut expected: Vec<(String, String)> = own
            .into_iter()
            .chain([("x-id", "GetObject"), ("k", "a&b=c")])
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        pairs.sort();
        expected.sort();
        assert_eq!(pairs, expected);
    }
}

/// The query of `presigned`, decoded, in order.
fn query_of(presigned: &str) -> Vec<(String, String)> {
    Url::parse(presigned)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

fn names(pairs: &[(String, String)]) -> Vec<&str> {
    pairs.iter().map(|(name, _)| name.as_str()).collect()
}

/// The moment an `X-Amz-Date` such as `20260101T120000Z` spells.
fn amz_date(value: &str) -> OffsetDateTime {
    let part = |range: std::ops::Range<usize>| value[range].parse::<u8>().unwrap();
    let month = Month::try_from(part(4..6)).unwrap();
    Date::from_calendar_date(value[0..4].parse().unwrap(), month, part(6..8))
        .unwrap()
        .with_hms(part(9..11), part(11..13), part(13..15))
        .unwrap()
        .assume_utc()
}

/// Checks the signature of `presigned` the way a server does: recomputed from the URL's own
/// query (all of it but the signature), from `host` as the client will send it, and from
/// `headers`, which a presigned PUT requires the client to send as signed.
fn signature_verifies(presigned: &str, method: &str, host: &str, headers: &HeaderMap) -> bool {
    let mut url = Url::parse(presigned).unwrap();
    let mut pairs = query_of(presigned);
    let (last, signature) = pairs.pop().unwrap();
    assert_eq!(last, "X-Amz-Signature", "{presigned}");
    let datetime = amz_date(
        &pairs
            .iter()
            .find(|(name, _)| name == "X-Amz-Date")
            .unwrap()
            .1,
    );
    url.query_pairs_mut().clear().extend_pairs(&pairs);
    let mut signed = headers.clone();
    signed.insert(HOST, host.parse().unwrap());
    let region = Region::UsEast1;
    let canonical = signing::canonical_request(method, &url, &signed, "UNSIGNED-PAYLOAD").unwrap();
    let string_to_sign = signing::string_to_sign(&datetime, &region, &canonical).unwrap();
    let key = signing::signing_key(&datetime, SECRET_KEY, &region, "s3").unwrap();
    let mut hmac = signing::HmacSha256::new_from_slice(&key).unwrap();
    hmac.update(string_to_sign.as_bytes());
    hex::encode(hmac.finalize().into_bytes()) == signature
}

#[test]
fn presign_get_spells_the_object_url_then_the_auth_query_then_the_signature() {
    let bucket = path_style("http://localhost:9000");
    let presigned = run!(bucket.presign_get(KEY, 3600, None)).unwrap();
    let prefix = format!(
        "http://localhost:9000/test-bucket/{ENCODED_KEY}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test_access_key%2F"
    );
    assert!(presigned.starts_with(&prefix), "{presigned}");
    let pairs = query_of(&presigned);
    assert_eq!(
        names(&pairs),
        [AUTH_KEYS.as_slice(), &["X-Amz-Signature"]].concat()
    );
    assert_eq!(pairs[0].1, "AWS4-HMAC-SHA256");
    assert!(pairs[1].1.starts_with("test_access_key/"), "{presigned}");
    assert!(
        pairs[1].1.ends_with("/us-east-1/s3/aws4_request"),
        "{presigned}"
    );
    assert_eq!(pairs[3].1, "3600");
    assert_eq!(pairs[4].1, "host");
    assert_eq!(pairs[5].1.len(), 64);
    assert!(signature_verifies(
        &presigned,
        "GET",
        "localhost:9000",
        &HeaderMap::new()
    ));
}

#[test]
fn presign_get_custom_queries_follow_the_auth_query_and_are_signed() {
    let bucket = path_style("http://localhost:9000");
    let queries = HashMap::from([(
        "response-content-type".to_owned(),
        "text/plain; charset=utf-8".to_owned(),
    )]);
    let presigned = run!(bucket.presign_get(KEY, 60, Some(queries))).unwrap();
    let pairs = query_of(&presigned);
    assert_eq!(
        names(&pairs),
        [
            AUTH_KEYS.as_slice(),
            &["response-content-type", "X-Amz-Signature"]
        ]
        .concat()
    );
    assert_eq!(pairs[5].1, "text/plain; charset=utf-8");
    assert!(signature_verifies(
        &presigned,
        "GET",
        "localhost:9000",
        &HeaderMap::new()
    ));
}

#[test]
fn presign_put_signs_its_custom_headers_and_queries() {
    let bucket = path_style("http://localhost:9000");
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "text/plain".parse().unwrap());
    let queries = HashMap::from([("x-id".to_owned(), "PutObject".to_owned())]);
    let presigned =
        run!(bucket.presign_put(KEY, 60, Some(headers.clone()), Some(queries))).unwrap();
    let pairs = query_of(&presigned);
    assert_eq!(pairs[4].1, "content-type;host");
    assert_eq!(
        names(&pairs),
        [AUTH_KEYS.as_slice(), &["x-id", "X-Amz-Signature"]].concat()
    );
    assert!(signature_verifies(
        &presigned,
        "PUT",
        "localhost:9000",
        &headers
    ));
}

#[test]
fn presign_with_extra_query_has_one_query_string_signed_whole() {
    let mut bucket = path_style("http://localhost:9000");
    bucket.add_query("x-id", "GetObject");
    let queries = HashMap::from([("acl".to_owned(), String::new())]);
    let presigned = run!(bucket.presign_get(KEY, 60, Some(queries))).unwrap();
    assert_eq!(presigned.matches('?').count(), 1, "{presigned}");
    assert!(presigned.contains("&acl=&X-Amz-Signature="), "{presigned}");
    let pairs = query_of(&presigned);
    assert_eq!(
        names(&pairs),
        [
            &["x-id"][..],
            &AUTH_KEYS[..],
            &["acl", "X-Amz-Signature"][..]
        ]
        .concat()
    );
    assert_eq!(pairs[0].1, "GetObject");
    assert!(signature_verifies(
        &presigned,
        "GET",
        "localhost:9000",
        &HeaderMap::new()
    ));
}

#[test]
fn presign_delete_is_signed_as_a_delete() {
    let bucket = path_style("http://localhost:9000");
    let presigned = run!(bucket.presign_delete(KEY, 60)).unwrap();
    assert_eq!(
        names(&query_of(&presigned)),
        [AUTH_KEYS.as_slice(), &["X-Amz-Signature"]].concat()
    );
    assert!(signature_verifies(
        &presigned,
        "DELETE",
        "localhost:9000",
        &HeaderMap::new()
    ));
}

#[test]
fn presign_carries_the_security_token_after_the_signed_headers() {
    let bucket = *Bucket::new(
        "test-bucket",
        custom("http://localhost:9000"),
        credentials(Some("tok/en+1")),
    )
    .unwrap()
    .with_path_style();
    let presigned = run!(bucket.presign_get(KEY, 60, None)).unwrap();
    let pairs = query_of(&presigned);
    assert_eq!(
        names(&pairs),
        [
            AUTH_KEYS.as_slice(),
            &["X-Amz-Security-Token", "X-Amz-Signature"]
        ]
        .concat()
    );
    assert_eq!(pairs[5].1, "tok/en+1");
    assert!(signature_verifies(
        &presigned,
        "GET",
        "localhost:9000",
        &HeaderMap::new()
    ));
}

#[test]
fn presigned_urls_keep_the_endpoints_authority_verbatim() {
    for (endpoint, prefix, host) in [
        (
            "http://minio:80",
            "http://minio:80/test-bucket/",
            "minio:80",
        ),
        (
            "https://minio:443",
            "https://minio:443/test-bucket/",
            "minio:443",
        ),
        (
            "https://minio:443/storage/v1/s3",
            "https://minio:443/storage/v1/s3/test-bucket/",
            "minio:443",
        ),
        (
            "http://minio:9000",
            "http://minio:9000/test-bucket/",
            "minio:9000",
        ),
        (
            "https://project.example:8443/storage/v1/s3",
            "https://project.example:8443/storage/v1/s3/test-bucket/",
            "project.example:8443",
        ),
    ] {
        let presigned = run!(path_style(endpoint).presign_get(KEY, 60, None)).unwrap();
        assert!(
            presigned.starts_with(&format!("{prefix}{ENCODED_KEY}?")),
            "{presigned}"
        );
        assert!(
            signature_verifies(&presigned, "GET", host, &HeaderMap::new()),
            "{presigned}"
        );
    }
}

/// Keys as applications build them: printable ASCII with every character a URL treats
/// specially, and arbitrary Unicode including controls.
fn any_key() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => r"[a-z0-9 /.%+?#&=~\\-]{0,24}",
        1 => prop::collection::vec(any::<char>(), 0..8).prop_map(String::from_iter),
    ]
}

/// Query names and values as callers pass them.
fn any_queries() -> impl Strategy<Value = HashMap<String, String>> {
    prop::collection::hash_map("[a-z-]{1,8}", "[ -~]{0,10}", 0..4)
}

fn has_dot_segment(key: &str) -> bool {
    key.split('/')
        .any(|segment| segment == "." || segment == "..")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn any_key_reaches_the_path_percent_encoded_or_is_refused_for_a_dot_segment(key in any_key()) {
        let bucket = path_style("http://localhost:9000");
        let stripped = key.strip_prefix('/').unwrap_or(&key);
        match run!(sent(&bucket, &key, Command::GetObject)) {
            Err(S3Error::EmptyKey(refused)) => {
                prop_assert!(stripped.is_empty(), "{key:?} refused as empty");
                prop_assert_eq!(refused, key);
            }
            Err(S3Error::DotSegmentKey(refused)) => {
                prop_assert!(has_dot_segment(stripped), "{key:?} refused");
                prop_assert_eq!(refused, key);
            }
            Err(error) => prop_assert!(false, "{key:?}: {error}"),
            Ok((url, _)) => {
                prop_assert!(!stripped.is_empty(), "{key:?} sent");
                prop_assert!(!has_dot_segment(stripped), "{key:?} sent");
                prop_assert_eq!(url.path(), format!("/test-bucket/{}", signing::uri_encode(stripped, false)));
                let decoded = percent_encoding::percent_decode_str(url.path()).decode_utf8().unwrap();
                prop_assert_eq!(decoded, format!("/test-bucket/{stripped}"));
                prop_assert_eq!(url.query(), None);
                prop_assert_eq!(url.fragment(), None);
                prop_assert_eq!(signing::canonical_uri_string(&url), url.path());
            }
        }
    }

    #[test]
    fn any_extra_query_and_list_parameters_round_trip(
        extra in any_queries(),
        prefix in "[ -~]{0,10}",
        token in "[ -~]{1,10}",
    ) {
        let mut bucket = path_style("http://localhost:9000");
        for (name, value) in &extra {
            bucket.add_query(name, value);
        }
        let command = Command::ListObjectsV2 {
            prefix: prefix.clone(),
            delimiter: None,
            continuation_token: Some(token.clone()),
            start_after: None,
            max_keys: None,
        };
        let (url, _) = run!(sent(&bucket, "", command)).unwrap();
        let mut pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let mut expected: Vec<(String, String)> = extra
            .into_iter()
            .chain([
                ("prefix".to_owned(), prefix),
                ("list-type".to_owned(), "2".to_owned()),
                ("continuation-token".to_owned(), token),
            ])
            .collect();
        pairs.sort();
        expected.sort();
        prop_assert_eq!(pairs, expected);
    }

    #[test]
    fn any_presigned_get_verifies_from_its_own_url(
        key in any_key(),
        extra in any_queries(),
        custom in any_queries(),
        expiry in 1..=604800u32,
    ) {
        let stripped = key.strip_prefix('/').unwrap_or(&key);
        prop_assume!(!stripped.is_empty() && !has_dot_segment(stripped));
        let mut bucket = path_style("http://localhost:9000");
        for (name, value) in &extra {
            bucket.add_query(name, value);
        }
        let presigned = run!(bucket.presign_get(&key, expiry, Some(custom.clone()))).unwrap();
        let url = Url::parse(&presigned).unwrap();
        prop_assert_eq!(url.path(), format!("/test-bucket/{}", signing::uri_encode(stripped, false)));
        let pairs = query_of(&presigned);
        let auth = extra.len()..extra.len() + AUTH_KEYS.len();
        prop_assert_eq!(&names(&pairs)[auth.clone()], AUTH_KEYS.as_slice());
        prop_assert_eq!(&pairs[auth.start + 3].1, &expiry.to_string());
        for (name, value) in extra.iter().chain(&custom) {
            prop_assert!(pairs.contains(&(name.clone(), value.clone())), "{name}={value} in {presigned}");
        }
        prop_assert!(signature_verifies(&presigned, "GET", "localhost:9000", &HeaderMap::new()));
    }

    #[test]
    fn any_upload_id_round_trips_through_the_query(upload_id in "[ -~]{1,16}") {
        let bucket = path_style("http://localhost:9000");
        let command = Command::AbortMultipartUpload { upload_id: &upload_id };
        let (url, _) = run!(sent(&bucket, KEY, command)).unwrap();
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        prop_assert_eq!(pairs, vec![("uploadId".to_owned(), upload_id.clone())]);
        prop_assert_eq!(url.path(), format!("/test-bucket/{ENCODED_KEY}"));
    }
}

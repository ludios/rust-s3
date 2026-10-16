// Model-output: Claude Opus 5.5

//! `list` against the fake S3: the cursor each page leads it to request next, in ListObjects
//! v1 and v2, and the pages it refuses rather than return a partial listing.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, bucket, http, query_of, reply, serve, within};
use s3::error::S3Error;
use s3::serde_types::ListBucketResult;
use std::collections::HashMap;

/// The element of a page whose keys, prefixes and `NextMarker` are URL-encoded.
const URL_ENCODED: &str = "<EncodingType>url</EncodingType>";

/// A `NextMarker` element, the cursor of a ListObjects v1 page.
fn next_marker(marker: &str) -> String {
    format!("<NextMarker>{marker}</NextMarker>")
}

/// A `NextContinuationToken` element, the cursor of a ListObjects v2 page.
fn next_token(token: &str) -> String {
    format!("<NextContinuationToken>{token}</NextContinuationToken>")
}

/// The XML of a ListBucketResult page listing `keys`, then `common_prefixes`. `elements` is
/// inserted verbatim after `IsTruncated`: `URL_ENCODED`, an element from `next_marker` or
/// `next_token`, both, or "".
fn page(is_truncated: bool, elements: &str, keys: &[&str], common_prefixes: &[&str]) -> String {
    let mut xml = format!(
        "<ListBucketResult><Name>{BUCKET}</Name><IsTruncated>{is_truncated}</IsTruncated>{elements}"
    );
    for key in keys {
        xml.push_str(&format!(
            "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><Size>1</Size></Contents>"
        ));
    }
    for prefix in common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml
}

/// `page` without its `IsTruncated` element, as some services send it.
fn without_is_truncated(page: String) -> String {
    page.replace("<IsTruncated>false</IsTruncated>", "")
}

/// Lists the whole bucket with `Bucket::list`, through ListObjects v1 if `v1`, while the fake
/// S3 serves `pages` in order. Returns the listing and the query of each request it made.
async fn list(
    v1: bool,
    pages: Vec<String>,
) -> (
    Result<Vec<ListBucketResult>, S3Error>,
    Vec<HashMap<String, String>>,
) {
    let replies = pages
        .iter()
        .map(|page| reply(http("200 OK", &[], page.as_bytes())))
        .collect();
    let (endpoint, server) = serve(replies).await;
    let mut bucket = bucket(&endpoint);
    if v1 {
        bucket.set_listobjects_v1();
    }
    let result = within(bucket.list(String::new(), None)).await;
    let heads = within(server).await.unwrap();
    (result, heads.iter().map(|head| query_of(head)).collect())
}

#[tokio::test]
async fn list_continues_from_the_cursor_of_each_page() {
    let encoded_marker = format!("{URL_ENCODED}{}", next_marker("x%25+y"));
    let cases = [
        // v1 services send NextMarker only for a delimited listing; otherwise the last key
        // is the marker, or the last common prefix if it sorts after that key.
        (true, page(true, "", &["a", "b"], &[]), "marker", "b"),
        (true, page(true, "", &["a"], &["b/"]), "marker", "b/"),
        (true, page(true, "", &["c"], &["b/"]), "marker", "c"),
        (
            true,
            page(true, &next_marker(""), &["a"], &[]),
            "marker",
            "a",
        ),
        (
            true,
            page(true, &next_marker("b/"), &["a"], &[]),
            "marker",
            "b/",
        ),
        // A URL-encoded marker is decoded before it is compared and sent: "a b" < "a!".
        (
            true,
            page(true, URL_ENCODED, &["a+b%2Fc"], &[]),
            "marker",
            "a b/c",
        ),
        (
            true,
            page(true, URL_ENCODED, &["a+b"], &["a!"]),
            "marker",
            "a!",
        ),
        (
            true,
            page(true, &encoded_marker, &["a"], &[]),
            "marker",
            "x% y",
        ),
        (
            false,
            page(true, &next_token("t/1+"), &["a"], &[]),
            "continuation-token",
            "t/1+",
        ),
        // A cursor continues the listing on a page without IsTruncated.
        (
            true,
            without_is_truncated(page(false, &next_marker("b"), &["a", "b"], &[])),
            "marker",
            "b",
        ),
        (
            false,
            without_is_truncated(page(false, &next_token("t"), &["a"], &[])),
            "continuation-token",
            "t",
        ),
    ];

    for (v1, first_page, cursor_param, cursor) in cases {
        let (result, queries) = list(v1, vec![first_page, page(false, "", &["z"], &[])]).await;
        let pages = result.expect("two-page listing should succeed");
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].next_continuation_token.as_deref(), Some(cursor));
        assert_eq!(pages[1].contents[0].key, "z");
        assert_eq!(queries.len(), 2);
        assert!(!queries[0].contains_key(cursor_param));
        assert_eq!(queries[1][cursor_param], cursor);
    }
}

#[tokio::test]
async fn list_rejects_truncated_pages_without_a_new_cursor() {
    let cases = [
        // v2 has no fallback for a missing token.
        (
            false,
            vec![page(true, "", &["a"], &[])],
            "truncated response has no continuation cursor",
        ),
        (
            false,
            vec![page(true, &next_token(""), &["a"], &[])],
            "truncated response has no continuation cursor",
        ),
        (
            true,
            vec![page(true, "", &[], &[])],
            "truncated response has no continuation cursor",
        ),
        (
            false,
            vec![
                page(true, &next_token("t1"), &["a"], &[]),
                page(true, &next_token("t2"), &["b"], &[]),
                page(true, &next_token("t1"), &["a"], &[]),
            ],
            "pagination cursor repeated",
        ),
        (
            true,
            vec![page(true, "", &["a"], &[]), page(true, "", &["a"], &[])],
            "pagination cursor repeated",
        ),
    ];

    for (v1, pages, expected_reason) in cases {
        let expected_requests = pages.len();
        let (result, queries) = list(v1, pages).await;
        match result {
            Err(S3Error::InvalidListObjectsPagination(reason)) => {
                assert_eq!(reason, expected_reason);
            }
            other => panic!("expected a pagination error, got {other:?}"),
        }
        assert_eq!(queries.len(), expected_requests);
    }
}

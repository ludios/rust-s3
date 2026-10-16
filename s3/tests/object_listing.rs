// Model-output: Claude Opus 5.5

//! `list` against the fake S3: the cursor each truncated page leads it to request next, in
//! ListObjects v1 and v2, and the pages it refuses rather than return a partial listing.

#![cfg(feature = "with-tokio")]

mod fake_s3;

use fake_s3::{BUCKET, Reply, bucket, http, reply, serve, within};
use s3::error::S3Error;
use s3::serde_types::ListBucketResult;
use std::collections::HashMap;

/// A `NextMarker` element, the cursor of a ListObjects v1 page.
fn next_marker(marker: &str) -> String {
    format!("<NextMarker>{marker}</NextMarker>")
}

/// A `NextContinuationToken` element, the cursor of a ListObjects v2 page.
fn next_token(token: &str) -> String {
    format!("<NextContinuationToken>{token}</NextContinuationToken>")
}

/// A ListBucketResult reply listing `keys`, then `common_prefixes`. `cursor` is inserted
/// verbatim: an element from `next_marker` or `next_token`, or "" for none.
fn page(is_truncated: bool, cursor: &str, keys: &[&str], common_prefixes: &[&str]) -> Reply {
    let mut xml = format!(
        "<ListBucketResult><Name>{BUCKET}</Name><IsTruncated>{is_truncated}</IsTruncated>{cursor}"
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
    reply(http("200 OK", &[], xml.as_bytes()))
}

/// The query parameters of the request whose head is `head`.
fn query_of(head: &str) -> HashMap<String, String> {
    let target = head.split_whitespace().nth(1).unwrap();
    url::Url::parse(&format!("http://fake{target}"))
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

/// Lists the whole bucket with `Bucket::list`, through ListObjects v1 if `v1`, while the fake
/// S3 serves `pages` in order. Returns the listing and the query of each request it made.
async fn list(
    v1: bool,
    pages: Vec<Reply>,
) -> (
    Result<Vec<ListBucketResult>, S3Error>,
    Vec<HashMap<String, String>>,
) {
    let (endpoint, server) = serve(pages).await;
    let mut bucket = bucket(&endpoint);
    if v1 {
        bucket.set_listobjects_v1();
    }
    let result = within(bucket.list(String::new(), None)).await;
    let heads = within(server).await.unwrap();
    (result, heads.iter().map(|head| query_of(head)).collect())
}

#[tokio::test]
async fn list_continues_from_the_cursor_of_each_truncated_page() {
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
        (
            false,
            page(true, &next_token("t/1+"), &["a"], &[]),
            "continuation-token",
            "t/1+",
        ),
    ];

    for (v1, first_page, cursor_param, cursor) in cases {
        let (result, queries) = list(v1, vec![first_page, page(false, "", &["z"], &[])]).await;
        let pages = result.expect("two-page listing should succeed");
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[1].contents[0].key, "z");
        assert_eq!(queries.len(), 2);
        assert!(!queries[0].contains_key(cursor_param));
        assert_eq!(queries[1][cursor_param], cursor);
    }
}

/// Whether there is another page is up to `IsTruncated`, not to the presence of a cursor.
#[tokio::test]
async fn list_stops_at_an_untruncated_page_that_carries_a_cursor() {
    let cases = [(true, next_marker("b")), (false, next_token("t"))];

    for (v1, cursor) in cases {
        let (result, queries) = list(v1, vec![page(false, &cursor, &["a", "b"], &[])]).await;
        assert_eq!(result.expect("one-page listing should succeed").len(), 1);
        assert_eq!(queries.len(), 1);
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

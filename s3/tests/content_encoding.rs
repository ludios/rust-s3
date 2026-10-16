// Model-output: Claude Fable 5.1

//! An object stored with a `Content-Encoding` comes back as its stored bytes, whichever
//! decompression features other crates in the build switch on in the HTTP client crate: the
//! tokio client neither asks for a compressed transfer nor decodes one, and attohttpc's
//! decoder, which cannot be switched off, makes the sync backend refuse an object it decoded
//! rather than return it, while a listing it decoded is still read.

#![cfg(any(feature = "with-tokio", feature = "sync"))]

mod fake_s3;

use fake_s3::{Reply, bucket, header_of, http, reply, serve, within};

/// The bytes of an object stored with `Content-Encoding: gzip`: one gzip member of "hello, world".
const STORED: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0xd7,
    0x51, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x01, 0x00, 0x3a, 0x72, 0xab, 0xff, 0x0c, 0x00, 0x00, 0x00,
];

/// The fake S3's reply to a GET of that object.
fn stored_gzip_object() -> Reply {
    reply(http("200 OK", &[("content-encoding", "gzip")], STORED))
}

#[cfg(feature = "with-tokio")]
#[tokio::test]
async fn get_object_returns_a_gzip_object_as_stored() {
    let (endpoint, server) = serve(vec![stored_gzip_object()]).await;
    let response = within(bucket(&endpoint).get_object("/hello.gz"))
        .await
        .unwrap();
    let heads = within(server).await.unwrap();

    assert_eq!(header_of(&heads[0], "accept-encoding"), None);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    assert_eq!(
        response.headers()["content-length"],
        STORED.len().to_string()
    );
    assert_eq!(response.as_slice(), STORED);
}

/// Whether this build's attohttpc decodes gzip bodies, learned from attohttpc itself: it does
/// if and only if some crate in the build enables one of its compression features.
#[cfg(feature = "sync")]
fn attohttpc_decodes(runtime: &tokio::runtime::Runtime) -> bool {
    let (endpoint, server) = runtime.block_on(serve(vec![stored_gzip_object()]));
    let probe = attohttpc::get(format!("{endpoint}/hello.gz"))
        .send()
        .unwrap()
        .bytes()
        .unwrap();
    runtime.block_on(within(server)).unwrap();
    probe != STORED
}

/// The sync backend has two correct outcomes for a gzip object: the stored bytes, or a
/// refusal to hand over ones attohttpc decoded.
#[cfg(feature = "sync")]
#[test]
fn get_object_returns_a_gzip_object_as_stored_or_refuses_a_decoded_one() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attohttpc_decodes = attohttpc_decodes(&runtime);
    let (endpoint, server) = runtime.block_on(serve(vec![stored_gzip_object()]));
    let result = bucket(&endpoint).get_object("/hello.gz");
    let heads = runtime.block_on(within(server)).unwrap();

    if attohttpc_decodes {
        // The same features make attohttpc advertise compression, and nothing can stop that.
        assert_eq!(
            header_of(&heads[0], "accept-encoding"),
            Some("gzip, deflate")
        );
        let error = result.unwrap_err();
        assert!(
            matches!(error, s3::error::S3Error::AttoDecodedBody(ref encoding) if encoding == "gzip"),
            "{error}"
        );
    } else {
        assert_eq!(header_of(&heads[0], "accept-encoding"), None);
        let response = result.unwrap();
        assert_eq!(response.headers()["content-encoding"], "gzip");
        assert_eq!(response.as_slice(), STORED);
    }
}

/// One gzip member of
/// `<ListBucketResult><Name>test-bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>`:
/// an empty listing as a proxy that compresses on the way would send it.
#[cfg(feature = "sync")]
const GZIPPED_LISTING: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0xb3, 0xf1, 0xc9, 0x2c, 0x2e, 0x71,
    0x2a, 0x4d, 0xce, 0x4e, 0x2d, 0x09, 0x4a, 0x2d, 0x2e, 0xcd, 0x29, 0xb1, 0xb3, 0xf1, 0x4b, 0xcc,
    0x4d, 0xb5, 0x2b, 0x49, 0x2d, 0x2e, 0xd1, 0x4d, 0x02, 0x8b, 0xdb, 0xe8, 0x83, 0x45, 0x6c, 0x3c,
    0x8b, 0x43, 0x8a, 0x4a, 0xf3, 0x92, 0x13, 0x4b, 0x52, 0x53, 0xec, 0xd2, 0x12, 0x73, 0x8a, 0x53,
    0x6d, 0xf4, 0x91, 0x85, 0x6c, 0xf4, 0x31, 0x8c, 0x02, 0x00, 0x39, 0x87, 0x35, 0xce, 0x5d, 0x00,
    0x00, 0x00,
];

/// Only object downloads are refused: an API response attohttpc decoded is what the caller
/// needs, so a listing compressed on the way still lists. There is nothing to check unless
/// attohttpc decodes.
#[cfg(feature = "sync")]
#[test]
fn list_reads_a_listing_attohttpc_decoded() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    if !attohttpc_decodes(&runtime) {
        return;
    }
    let listing = reply(http(
        "200 OK",
        &[("content-encoding", "gzip")],
        GZIPPED_LISTING,
    ));
    let (endpoint, server) = runtime.block_on(serve(vec![listing]));
    let pages = bucket(&endpoint).list(String::new(), None).unwrap();
    runtime.block_on(within(server)).unwrap();

    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].name, "test-bucket");
    assert!(pages[0].contents.is_empty());
}

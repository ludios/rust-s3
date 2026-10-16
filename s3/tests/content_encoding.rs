// Model-output: Claude Fable 5.1

//! An object stored with a `Content-Encoding` comes back as its stored bytes, whichever
//! decompression features other crates in the build switch on in the HTTP client crate: the
//! backends never ask for a compressed transfer, reqwest's decoders are switched off, and
//! attohttpc's decoder, which cannot be, makes the sync backend refuse a body it decoded
//! rather than return it.

#![cfg(any(feature = "with-tokio", feature = "sync"))]

mod fake_s3;

use fake_s3::{Reply, bucket, header_of, http, reply, serve};

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
    use fake_s3::within;
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

/// attohttpc decodes gzip bodies if and only if some crate in the build enables its `compress`
/// feature, so the sync backend has two correct outcomes: the stored bytes, or a refusal to
/// hand over decoded ones. Which to expect is learned from attohttpc itself.
#[cfg(feature = "sync")]
#[test]
fn get_object_returns_a_gzip_object_as_stored_or_refuses_a_decoded_one() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (endpoint, server) =
        runtime.block_on(serve(vec![stored_gzip_object(), stored_gzip_object()]));
    let probe = attohttpc::get(format!("{endpoint}/hello.gz"))
        .send()
        .unwrap()
        .bytes()
        .unwrap();
    let attohttpc_decodes = probe != STORED;
    let result = bucket(&endpoint).get_object("/hello.gz");
    let heads = runtime.block_on(server).unwrap();

    if attohttpc_decodes {
        // The same feature makes attohttpc advertise compression, and nothing can stop that.
        assert_eq!(
            header_of(&heads[1], "accept-encoding"),
            Some("gzip, deflate")
        );
        let error = result.unwrap_err();
        assert!(
            matches!(error, s3::error::S3Error::AttoDecodedBody(ref encoding) if encoding == "gzip"),
            "{error}"
        );
    } else {
        assert_eq!(header_of(&heads[1], "accept-encoding"), None);
        let response = result.unwrap();
        assert_eq!(response.headers()["content-encoding"], "gzip");
        assert_eq!(response.as_slice(), STORED);
    }
}

# GPT-6 (Codex) — rust-s3 production code review — 2026-10-16

Reviewed the working tree at `b2077f3fd514b96f4a0ba9355c326aafd5349670`, across all three workspace crates and HTTP backends. No repository files were changed. Ignored provider tests were not run.

The most serious findings involve wrong-key operations, silently changed download contents, and process termination. No P0 issue was identified. Findings marked **Previously recorded** remain open in the current code; otherwise, I found no matching finding in `AUDIT.md` or `PR_TRIAGE.md`.

1. **P1 — URL normalization can read, overwrite, or delete the wrong object.**  
   Location: [s3/src/request/request_trait.rs:779](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:779), particularly `Url::parse` at line 844.

   Object keys pass through a URL parser that resolves `.` and `..` segments. I reproduced `"a/../victim"` becoming `/bucket/victim`, and `"a/./b"` becoming `/bucket/a/b`. Consequently, deleting the valid S3 key `a/../victim` can successfully delete `victim` instead. Signing succeeds because it signs the already-normalized URL. This affects every backend. [AWS documents that S3 paths must not be normalized.](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html)

   **Fix:** Preserve key segments through transport construction. If the current URL/transport APIs cannot do that reliably, reject period-only segments explicitly rather than silently addressing another key.

2. **P1 — Bulk deletion can delete a different key when the requested key contains a carriage return.**  
   Location: [s3/src/serde_types.rs:456](/home/at/code/rust/rust-s3/s3/src/serde_types.rs:456).

   `quick_xml::escape::escape` leaves carriage returns literal. Serializing key `"a\rb"` produces `<Key>a\rb</Key>`, which a conforming XML parser normalizes to `"a\nb"`. I confirmed both the serializer output and that normalization independently. If both objects exist, `delete_objects` can delete the LF-named object while leaving the requested CR-named object. [AWS explicitly requires character references for these characters in XML requests.](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)

   **Fix:** Encode CR as `&#13;` and preserve other special/control characters appropriately in keys and version IDs. Ordinary XML metacharacter escaping is insufficient.

3. **P1 — Dependency feature unification can silently decompress stored object bytes.**  
   Locations: [s3/src/request/tokio_backend.rs:51](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:51), [s3/src/request/blocking.rs:33](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:33).

   Neither backend disables automatic response decompression. Another dependency enabling `reqwest/gzip` or `attohttpc/compress` changes S3 download behavior. I reproduced a stored 34-byte gzip object returning its 14-byte decompressed contents with HTTP 200 in **both Tokio and sync**. Ranged downloads of encoded objects can also fail decoding or cease to represent the requested stored bytes.

   **Fix:** Explicitly disable all reqwest response decoders. Attohttpc 0.30.1 needs a transport change or upstream option: its `allow_compression(false)` only controls advertising compression, and does not disable response decoding.

4. **P1 — Copy sources are not URL-encoded, allowing successful copies of the wrong object.**  
   Locations: [s3/src/bucket.rs:1237](/home/at/code/rust/rust-s3/s3/src/bucket.rs:1237), [s3/src/request/request_trait.rs:996](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:996).

   `copy_object_internal("a%2Fb", "dest")` sends `x-amz-copy-source: bucket/a%2Fb`. That header identifies decoded key `a/b`, rather than the literal key `a%2Fb`. If both exist, the wrong contents can be copied successfully. I verified the generated header; the provider consequence follows from [AWS’s required URL-encoding of copy sources](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html). Spaces and non-ASCII keys also encounter encoding/signing failures.

   **Fix:** Encode the bucket/key copy source exactly once before placing it in the header.

5. **P1 — Automatic redirects forward uploads and credentials, and can turn an unintended operation into success.**  
   Locations: [s3/src/request/tokio_backend.rs:79](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:79), [s3/src/request/blocking.rs:33](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:33).

   Both clients retain their default redirect behavior. In two-origin local reproductions, a 307 forwarded the complete upload body and `x-amz-security-token` to the second origin. Sync also forwarded `Authorization`; Tokio removed that header. The second server’s 200 made `put_object` report success despite no upload occurring at the original endpoint. SSE-C headers are similarly sensitive.

   **Fix:** Disable automatic redirects for signed requests. Supporting S3 redirects requires controlled destination validation and re-signing, rather than replaying the original signed request.

6. **P1 — Small reads from `ResponseDataStream` can overflow the stack and abort the process.**  
   Locations: [s3/src/request/request_trait.rs:380](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:380), corresponding async-std implementation at line 424.

   Each partial read prepends another `once(remainder).chain(previous_stream)` wrapper. Consumed wrappers remain nested until the underlying stream is polled again or dropped. Reading one 65,536-byte chunk a byte at a time reproduced:

   ```text
   read bytes; checking EOF
   thread 'main' has overflowed its stack
   fatal runtime error: stack overflow, aborting
   ```

   Both implementations use the same construction. This affects ordinary consumers that parse a response through small `AsyncRead` buffers.

   **Fix:** Keep a pending `Bytes` buffer and offset in a reader adapter, without recursively wrapping the stream.

7. **P1 — Public `Command::UploadPart` can overwrite the destination object instead of uploading a part.**  
   Locations: [s3/src/request/request_trait.rs:837](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:837), [s3/src/command.rs:421](/home/at/code/rust/rust-s3/s3/src/command.rs:421).

   This variant sends a PUT without `uploadId` or `partNumber`, and signs the empty-payload hash regardless of its content. I verified the generated URL and hash. Nonempty content normally fails integrity verification; **empty content produces a valid ordinary empty-object PUT**, potentially overwriting an existing object successfully. The higher-level multipart helpers use a different, working command path.

   **Fix:** Route this variant through the same multipart URL and payload handling as `PutObject { multipart: Some(...) }`, or deprecate it.

   **Previously recorded:** `AUDIT.md:237`; still open. The empty-body overwrite makes the impact more serious than a failed multipart request.

8. **P2 — A supported rustls feature combination panics during credential fetching.**  
   Location: [s3/Cargo.toml:103](/home/at/code/rust/rust-s3/s3/Cargo.toml:103), together with `tokio-rustls-tls` at line 107.

   With defaults disabled, combining `tokio-rustls-tls,http-credentials-rustls-tls` enables both ring and aws-lc in rustls. Reqwest selects its provider explicitly, but attohttpc’s credential transport calls `ClientConfig::builder()` without one. A local HTTPS credential-fetch reproduction, using locked rustls 0.23.45, panicked with “Could not automatically determine the process-level CryptoProvider”.

   **Fix:** Align the two transports’ provider selection. The existing ring credential feature provides an immediate workaround; do not require applications to discover this through a production panic.

9. **P2 — The documented bare `with-async-std` feature builds but panics on its first request.**  
   Locations: [s3/Cargo.toml:89](/home/at/code/rust/rust-s3/s3/Cargo.toml:89), [s3/src/request/async_std_backend.rs:49](/home/at/code/rust/rust-s3/s3/src/request/async_std_backend.rs:49).

   `--no-default-features --features with-async-std` enables Surf without an HTTP backend. Compilation succeeds, but constructing the internal client panics: “Config without an http client provided to Surf configured without a default client.” I reproduced that on a library request.

   **Fix:** Select a usable transport, or reject this configuration at compile time with a clear message and document the required transport feature.

10. **P2 — V1 listing silently stops after the first truncated page.**  
    Location: [s3/src/bucket.rs:3368](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3368).

    `list` continues solely when `next_continuation_token` exists. AWS V1 listings without a delimiter can legitimately return `IsTruncated=true` without `NextMarker`; the next marker must then be the last returned key. My local reproduction returned `Ok` with one page, `is_truncated=true`, and no continuation. Objects beyond the first page disappear from the result. The same loop also lacks protection against repeated V2 tokens, permitting endless requests and accumulating results.

    **Fix:** Honor `IsTruncated`, implement the V1 last-key fallback, and validate cursor progress. [AWS documents the fallback.](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)

11. **P2 — Signing can mix different generations of credentials.**  
    Locations: [s3/src/request/request_trait.rs:1019](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:1019), signing-key/access-key reads at lines 495 and 944.

    The token, secret key, and access key are fetched through separate lock acquisitions. Another request refreshing the shared credentials between these reads can produce, for example, an old session token with a new access/secret pair. The refresh gate does not protect the signing sequence. Ordinary requests, presigned URLs, and POST policies have this exposure.

    **Fix:** Take one credential snapshot per signing attempt and use it throughout token selection, scope construction, and signing.

    **Previously recorded:** `AUDIT.md:247`; still open.

12. **P2 — Error responses defeat streaming memory bounds.**  
    Locations: [s3/src/request/tokio_backend.rs:179](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:179), [s3/src/request/async_std_backend.rs:213](/home/at/code/rust/rust-s3/s3/src/request/async_std_backend.rs:213), [s3/src/request/blocking.rs:53](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:53).

    Under `fail-on-err`, every backend buffers the entire error body before returning, including for streaming/writer APIs. A faulty provider or proxy returning a large or continuously streamed 503 body can exhaust memory. Tokio additionally retains all chunks and concatenates them into another allocation. A time limit does not impose a useful byte bound.

    **Fix:** Cap retained error diagnostics, mark truncation explicitly, and close the response when the cap is reached. This was verified statically; I did not deliberately exhaust memory.

13. **P2 — Sync request timeouts do not bound TCP connection establishment.**  
    Locations: [s3/src/request/blocking.rs:40](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:40), status path at line 97; similar credential configuration at [aws-creds/src/credentials.rs:277](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:277).

    Only attohttpc’s overall `timeout` is set. Its IP-address and single-address connection paths use the separate default 30-second connect timeout without applying that deadline. With a saturated local listen queue, a request configured for **20 ms remained blocked after 300 ms**, when I terminated it. Synchronous DNS resolution is also outside a reliable bound.

    **Fix:** Apply the configured bound to connection establishment as well, and document or address the DNS limitation.

    **Previously recorded:** The broad sync timeout gap remains acknowledged at `AUDIT.md:268`; this is a concrete reproduced case.

14. **P2 — `get_object_attributes` cannot deserialize its normal requested response.**  
    Locations: [s3/src/request/request_trait.rs:1105](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:1105), [s3/src/serde_types.rs:50](/home/at/code/rust/rust-s3/s3/src/serde_types.rs:50).

    The request asks only for `ETag`, while the response type requires `Checksum`, `ObjectParts`, `StorageClass`, and `ObjectSize`. Parsing a valid ETag-only response reproduced `missing field 'Checksum'`. Even requesting everything would leave problems: checksum algorithms and several pagination fields are optional. Additionally, `version_id=None` sends `versionId=null`, selecting the null version rather than generally selecting the current version.

    **Fix:** Make the requested attributes and response model agree, model optional fields correctly, and omit an absent version ID. [AWS specifies that unrequested attributes are omitted.](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html)

15. **P2 — Canonical headers do not implement SigV4 whitespace and repeated-value rules.**  
    Location: [s3/src/signing.rs:128](/home/at/code/rust/rust-s3/s3/src/signing.rs:128).

    Values are only trimmed. A valid metadata value `"a  b"` remains `"a  b"` in the canonical request, while SigV4 requires sequential spaces to collapse. The helper also emits repeated header values as separate lines instead of one comma-separated value. I verified both outputs. Requests or presigned uploads using such headers can fail signature validation.

    **Fix:** Normalize header whitespace and combine values in their original order before sorting header names. [AWS documents both rules.](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)

16. **P2 — Presigning a bucket with extra query parameters creates a malformed authentication query.**  
    Location: [s3/src/request/request_trait.rs:714](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:714), sync equivalent at line 744.

    The code concatenates a URL that may already contain a query with authorization parameters beginning with `?`. After `bucket.add_query("versionId", "v1")`, I obtained:

    ```text
    .../key?versionId=v1?X-Amz-Algorithm=AWS4-HMAC-SHA256&...
    ```

    There is no separate `X-Amz-Algorithm` parameter; it became part of the version ID value.

    **Fix:** Append authorization parameters through the URL’s query API rather than concatenating another query string.

17. **P2 — Sync presigned PUT URLs omit custom queries that their signatures include.**  
    Location: [s3/src/request/request_trait.rs:611](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:611).

    The sync `presigned` implementation matches `PresignPut` with `..` and substitutes `None` for its custom queries. Signature construction still includes those queries through `presigned_canonical_request`. I confirmed that `tagging` appeared in the Tokio URL but disappeared from the sync URL for the same call. The resulting sync URL fails authentication.

    **Fix:** Forward `custom_queries` exactly as the async implementation does, preferably using one shared implementation.

18. **P2 — Default-port preservation can duplicate endpoint paths in presigned URLs.**  
    Location: [s3/src/request/request_trait.rs:581](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:581), sync equivalent at line 633.

    For endpoint `http://localhost:80/storage`, the reconstruction concatenates `bucket.host()`—which includes `/storage`—with a URL path that already includes it. I reproduced `/storage/storage/bucket/key` in the returned URL, while signing used `/storage/bucket/key`. Explicit standard ports also create disagreement with clients that normalize the authority before sending.

    **Fix:** Derive both the signing authority and returned URL from one normalized URL representation; remove the substring-based port reconstruction.

19. **P2 — Presigned POSTs omit ordinary session tokens.**  
    Location: [s3/src/post_policy.rs:94](/home/at/code/rust/rust-s3/s3/src/post_policy.rs:94).

    Policy construction checks only `security_token`. Credentials from `AWS_SESSION_TOKEN` and `from_sts` populate `session_token`. I supplied such credentials and verified that the generated form omitted `x-amz-security-token`. Uploads signed with those temporary credentials fail authentication.

    **Fix:** Select the effective token from the same credential snapshot used for signing, supporting both token fields consistently.

20. **P2 — Checksum POST policies require a form field that the generated form omits.**  
    Locations: [s3/src/post_policy.rs:202](/home/at/code/rust/rust-s3/s3/src/post_policy.rs:202), field generation at line 136.

    Serializing `AmzChecksumAlgorithm(SHA256)` injects an exact policy condition for `x-amz-checksum-algorithm=SHA256`. Form-field generation walks the unexpanded conditions, so it only emits `x-amz-checksum-sha256`. I confirmed that the signed policy required the algorithm field while `PresignedPost.fields` omitted it. Submitting the generated fields violates the policy.

    **Fix:** Expand these derived conditions before both policy serialization and form-field generation.

21. **P2 — Opaque multipart and version identifiers are interpolated into queries without encoding.**  
    Locations: [s3/src/command.rs:69](/home/at/code/rust/rust-s3/s3/src/command.rs:69), [s3/src/request/request_trait.rs:788](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:788), version handling at line 816.

    Upload IDs are inserted verbatim into upload-part, completion, and abort URLs. Version IDs receive similar treatment. With an upload ID `"opaque+a&b#c"`, I verified that parsed parameters contained upload ID `"opaque a"`, an extra parameter `b`, and a fragment `c`. Providers returning opaque IDs containing reserved characters therefore produce failed requests and potentially failed cleanup.

    **Fix:** Add each identifier using `query_pairs_mut().append_pair`, consistently across operations.

22. **P2 — `location()` requests an object named `?location`, rather than the location subresource.**  
    Location: [s3/src/bucket.rs:2632](/home/at/code/rust/rust-s3/s3/src/bucket.rs:2632).

    The method passes `"?location"` as the object path, and `GetBucketLocation` adds no query parameter. I verified the resulting URL `/bucket/%3Flocation`. Normally this returns an object-not-found error; if that object exists, its contents are incorrectly interpreted as a location response. Parse failures can then be disguised as a successful `Region::Custom`.

    **Fix:** Use an empty path and add `location` as a command query parameter. Propagate malformed responses rather than manufacturing a region.

23. **P2 — `Bucket::exists()` constructs the wrong hostname for default-style buckets.**  
    Location: [s3/src/bucket.rs:794](/home/at/code/rust/rust-s3/s3/src/bucket.rs:794).

    The method empties the bucket name but retains virtual-host addressing. That produces hosts such as `.s3.amazonaws.com`; my custom-endpoint construction produced `http://.localhost:9876`. `list_buckets` already avoids this by selecting path style. Thus `exists()` fails on the default bucket configuration rather than answering the existence question.

    **Fix:** At minimum, switch the dummy bucket to path style. A dedicated HEAD-bucket operation would also avoid requiring permission to list every bucket in the account.

24. **P2 — `object_exists()` treats authorization and service failures as existence when `fail-on-err` is disabled.**  
    Location: [s3/src/bucket.rs:1364](/home/at/code/rust/rust-s3/s3/src/bucket.rs:1364).

    The method returns `status_code != 404`. I reproduced `Ok(true)` for HTTP 503; 403 and other failures behave similarly. Unlike APIs returning `ResponseData`, this API exposes no status for the caller to inspect, so disabling the feature converts failures into an affirmative answer.

    **Fix:** Return true only for a successful HEAD, false for 404, and an error for other statuses regardless of the raw-response feature policy.

25. **P2 — Tag XML permits malformed requests and XML injection.**  
    Location: [s3/src/bucket.rs:3090](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3090).

    Tag names and values are interpolated directly into XML. I captured a request containing `<Value>a&b</Value>`. More seriously, a value containing closing tags can turn one application-supplied tag into several valid tags in the signed request. Validation of the parsed tags at the provider does not preserve the application’s intended tag structure.

    **Fix:** Serialize tags through an XML serializer, or escape text and reject unsupported characters before signing.

26. **P2 — Malformed tagging responses are reported as successful empty tag sets.**  
    Location: [s3/src/bucket.rs:3253](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3253).

    Parsing is guarded by `if let Ok(...)`, and failures are ignored. A local server returning HTTP 200 with the incomplete body `<Tagging>` reproduced `Ok(([], 200))`. Wrong roots and malformed tag structures can similarly become empty or placeholder data. Applications synchronizing tags can mistake a damaged response for the absence of tags.

    **Fix:** Deserialize and validate the expected document structure, propagating malformed XML and missing required fields as errors.

27. **P2 — Sync streaming uploads lose the requested content type for small objects.**  
    Location: [s3/src/bucket.rs:2375](/home/at/code/rust/rust-s3/s3/src/bucket.rs:2375).

    After initiating and aborting a multipart upload, the small-object fallback calls `put_object`, which always uses `application/octet-stream`. I requested `text/plain` and captured `content-type: application/octet-stream` on the successful final PUT. The outcome changes at the multipart threshold.

    **Fix:** Read the first chunk before initiating multipart upload, as the async path does, and use `put_object_with_content_type` for the fallback. This also removes unnecessary initiation/abort requests and their permission requirements.

28. **P2 — Fixed 8 MiB parts impose an undocumented upload ceiling around 78 GiB.**  
    Locations: [s3/src/bucket.rs:335](/home/at/code/rust/rust-s3/s3/src/bucket.rs:335), part-number increments at lines 2258 and 2405.

    Streaming uploads always use 8 MiB parts and never check the part-number limit. AWS permits 10,000 parts, so sufficiently large streams fail after transferring roughly 78.125 GiB and then abort. The sync path can also add an empty part after an exact chunk boundary, making an otherwise valid 10,000-part upload attempt part 10,001. This is derived from the loops and [AWS’s multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html); I did not transfer an 80 GiB test object.

    **Fix:** Support configurable part sizes, enforce the part limit, and avoid an unnecessary empty final part.

29. **P2 — IMDS credential requests inherit proxies and redirects.**  
    Locations: [aws-creds/src/credentials.rs:704](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:704), IMDSv2 requests starting at line 733.

    Unlike the container credential fetcher, these requests neither disable environment proxies nor prohibit redirects. With a local HTTP proxy and fake responses, I confirmed that all IMDSv2 requests traversed the proxy, including requests carrying `x-aws-ec2-metadata-token`. This exposes metadata authentication material to the configured proxy and permits redirects outside the metadata endpoint.

    **Fix:** Reuse the container fetcher’s transport protections: disable proxies for link-local metadata, disable redirects, and validate statuses before consuming responses.

30. **P2 — An invalid IMDSv2 token response causes a panic.**  
    Location: [aws-creds/src/credentials.rs:746](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:746), repeated at line 754.

    The untrusted token response body is passed to attohttpc’s panicking `.header(...)` API. A fake metadata response containing `"bad\n"` reproduced a panic at `attohttpc`’s `expect("invalid header value")`. The public credential function returns `Result`, but this malformed response escapes that error boundary.

    **Fix:** Validate the token and use `try_header`, converting invalid values into `CredentialsError`.

31. **P2 — STS web-identity tokens leak through HTTP debug logging.**  
    Location: [aws-creds/src/credentials.rs:557](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:557), request at line 561.

    `from_sts` places the bearer `WebIdentityToken` in a GET query. Attohttpc 0.30.1 logs the complete request path and query at debug level, including for HTTPS requests. Enabling ordinary HTTP debug logging therefore records a replayable identity token. This was verified against the constructor and dependency logging code; no real STS token was used.

    **Fix:** Send STS parameters in an HTTPS POST form body. This is separate from the repaired `Credentials::Debug` exposure recorded in the earlier audit.

32. **P2 — The async-std rustls backend still contains a client-reachable certificate-validation DoS.**  
    Location: [s3/Cargo.toml:113](/home/at/code/rust/rust-s3/s3/Cargo.toml:113).

    The current normal-dependency graph is `surf → http-client → async-tls/rustls → webpki 0.21.4`. A pathological server certificate chain can cause exponential CPU work during validation, before authentication succeeds. I independently confirmed the dependency path and reran `cargo audit`. [RUSTSEC-2023-0052 explicitly affects TLS clients and is fixed in webpki 0.22.2.](https://rustsec.org/advisories/RUSTSEC-2023-0052.html) I did not execute the exploit.

    **Fix:** Move this backend to a maintained TLS stack; refreshing compatible lockfile versions alone cannot cross these dependency constraints.

    **Previously recorded:** The legacy async-std dependency issue at `AUDIT.md:21`; still open. This finding is narrower than treating every scanner match as reachable.

33. **P2 — Object Lock is serialized using the wrong header value.**  
    Location: [s3/src/bucket_ops.rs:196](/home/at/code/rust/rust-s3/s3/src/bucket_ops.rs:196).

    Setting `object_lock_enabled=true` emits `x-amz-bucket-object-lock-enabled: Enabled`. This header represents a boolean, not the `"Enabled"` value used by some XML configuration elements. Strict provider parsers reject the bucket-creation request. I verified the generated header and [AWS’s boolean request contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html); no live CreateBucket test was run.

    **Fix:** Emit `true`.

34. **P3 — Sync HEAD results always miss the normal Content-Type header.**  
    Location: [s3/src/utils/mod.rs:289](/home/at/code/rust/rust-s3/s3/src/utils/mod.rs:289).

    The sync conversion reads `"get-Type"` rather than `"Content-Type"`. Supplying a valid `content-type: text/plain` header reproduced `HeadObjectResult.content_type == None`. Applications using HEAD metadata to serve or copy objects receive incorrect metadata.

    **Fix:** Read `"Content-Type"`.

35. **P3 — `etag_for_path` disagrees with async streaming uploads at exact chunk boundaries.**  
    Location: [s3/src/utils/mod.rs:62](/home/at/code/rust/rust-s3/s3/src/utils/mod.rs:62).

    The helper hashes and counts the empty read after an exact multiple of 8 MiB. The async uploader skips that empty part. For an 8 MiB zero-filled file, the helper returned `f28ce6f900bc3567491417499dd6b302-2`; the async uploader’s single-part multipart layout corresponds to `9ed977000dc166f25a9b9ef26fb3c3fc-1`. Integrity comparisons therefore report a false mismatch.

    **Fix:** Share the uploader’s EOF/part-boundary rules. Also document that this predicts an ETag only for the applicable provider, encryption mode, and upload layout.

Validation performed:

- Default `cargo test --locked --workspace` passed, including integration tests and doctests.
- Eleven additional library configurations passed: Tokio without TLS, rustls, ring, and native TLS with blocking; sync without TLS, native TLS, and rustls; async-std Hyper, native TLS, and rustls, including representative `fail-on-err` disabled configurations.
- `aws-creds` tests without HTTP credential features passed.
- Reproductions used fake credentials, local servers, and separate target directories. The [reproduction harness](/tmp/rust-s3-review/gpt-6-harness/src/main.rs) is outside the repository.
- Passing builds do not cover the runtime feature panics above. No live AWS/provider verification or fuzzing was performed.

Opportunities for simplification:

- Build each request body once and derive its length and hash from those exact bytes. The separate command/body/hash implementations caused the `UploadPart` divergence.
- Use one URL construction path for ordinary requests and presigning, with structured query parameters and explicit key-preservation rules.
- Sign from one immutable credential snapshot.
- Replace manual tagging XML and the permissive minidom read path with the existing quick-xml serialization/deserialization stack.
- Implement bucket configuration helpers by cloning and changing fields, rather than repeatedly spelling out the complete `Bucket` struct.
- Retire the duplicate public `UploadPart` implementation and consider deprecating the old generic retry macro. Internal requests already use a more careful retry policy.
- Simplify `error_from_response_data`: it currently returns `Result<S3Error, S3Error>` while always returning `Err`.

Checks that held up:

- Copy and multipart completion reject HTTP 200 embedded errors and malformed/truncated XML; the earlier false-success repair remains present.
- Concurrent multipart results are sorted by part number before completion.
- High-level streaming uploads preserve the original failure while attempting cleanup, and drop outstanding part futures before starting abort.
- Internal retries restrict ambiguous replay to reads and immutable multipart parts. Body consumption and partially written downloads are outside replay.
- Tokio and async-std apply their current request deadlines through response-body consumption.
- Async-std’s body stream reads bounded chunks rather than eagerly buffering successful downloads.
- `Credentials` and nested STS credential `Debug` implementations redact keys and tokens. Refresh retains the original provider source rather than rediscovering another identity.
- The new range-stream API uses inclusive range bounds, exposes response headers, and shares the existing request/signing path.
- The observer’s once-only installation and body termination handling match its documented behavior. Inline callback panics and unobserved internal redirects are explicitly documented limitations.
- Both support crates are now local path dependencies of `s3`; the earlier workspace-versus-registry integration concern is resolved in the current manifests.
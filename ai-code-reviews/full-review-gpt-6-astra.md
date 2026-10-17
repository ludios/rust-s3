# GPT-6 repository review — 2026-10-16

Reviewed the working tree at `b2077f3`, including all three crates, the fork’s changes, and all three HTTP backends. No repository files were changed. No P0 findings.

The findings below are verified against the code. Reproductions used synthetic credentials, local servers, and programs under `/tmp`; no live storage-provider tests were run. Unless noted otherwise, these are additional findings beyond `AUDIT.md` and `PR_TRIAGE.md`.

Verification completed:

- Default `cargo test --locked --workspace`: passed.
- Library tests: Tokio rustls, Tokio ring, Tokio blocking, async-std native TLS, async-std rustls, async-std Hyper, sync native TLS, and sync rustls: passed.
- Credentials tests without HTTP and with plain HTTP; region tests with serde: passed.
- Separate reproductions exposed failures despite those passing suites. Material Tokio/rustls reproductions were repeated with the workspace’s locked dependency versions.
- Vendored OpenSSL and ignored provider tests were not run.

**1. P1 — Downloads can silently succeed with missing bytes**

Locations: [blocking.rs:166](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:166), [blocking.rs:177](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:177), [async_std_backend.rs:336](/home/at/code/rust/rust-s3/s3/src/request/async_std_backend.rs:336), [async_std_backend.rs:378](/home/at/code/rust/rust-s3/s3/src/request/async_std_backend.rs:378).

A server declares `Content-Length: 10`, sends `abc`, then closes. Both sync and async-std H1 returned successful buffered downloads containing three bytes; their writer APIs returned `Ok(200)`. Async-std’s stream ended normally after `abc`.

The underlying length-limited readers accept early EOF, and the library never checks the final byte count. Additionally, async-std H1 returned an empty successful download for a valid close-delimited response containing `abc`.

**Fix:** Enforce expected response lengths for body-bearing responses, returning `UnexpectedEof` on shortage. Repair or replace H1’s handling of close-delimited responses. Preserve HEAD/no-body exceptions.

**2. P1 — Dot segments change the object being read, overwritten, or deleted**

Location: [request_trait.rs:779](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:779).

Object paths are encoded and then passed through `Url::parse`, which normalizes dot segments. Reproduced: a DELETE for `dir/../victim` produces `/b/victim`. It can therefore delete an existing object different from the requested key. PUT has the corresponding overwrite risk.

S3 keys are not filesystem paths; AWS explicitly prohibits this normalization. [AWS signing specification](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html)

**Fix:** Preserve the literal key throughout URL construction and transmission. If the transport cannot represent these keys faithfully, reject them before issuing a request.

Earlier audit: dot-segment handling was listed as a coverage gap at `AUDIT.md:323`; the defect remains.

**3. P1 — Bulk deletion can delete the wrong key containing a newline**

Location: [serde_types.rs:456](/home/at/code/rust/rust-s3/s3/src/serde_types.rs:456).

`quick_xml::escape::escape` does not escape carriage returns. A requested key `a\rb` is emitted with a literal CR, which XML parsing normalizes to LF. Reproduced serialization and XML decoding changed it to `a\nb`.

If both objects exist, this can delete the wrong object while leaving the intended one intact. AWS specifically documents this DeleteObjects hazard. [Object-key XML requirements](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)

**Fix:** Escape CR as `&#13;` in XML text, including keys and version identifiers; test exact round trips for CR, CRLF, and LF.

**4. P1 — Copy source encoding can select a different source object**

Locations: [bucket.rs:1237](/home/at/code/rust/rust-s3/s3/src/bucket.rs:1237), [request_trait.rs:997](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:997).

`copy_object_internal` interpolates the source key directly into `x-amz-copy-source`. For a literal key `a%2Fb`, the header is `b/a%2Fb`; a provider decoding that header selects `a/b`, potentially copying the wrong bytes over the destination. Other reserved characters cause failures or change the source’s interpretation.

The header must be URL-encoded. [AWS CopyObject contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)

**Fix:** Encode the source key exactly once before constructing the header. Keep any deliberately supported version selector separate from the key.

**5. P1 — Automatic redirects forward sensitive headers and upload contents**

Locations: [tokio_backend.rs:51](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:51), [blocking.rs:33](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:33).

Both clients retain automatic redirect behavior. A local 307 redirect to another origin received the session token, SSE-C encryption key, and uploaded object bytes. The sync backend also forwarded the original Authorization header. Both uploads returned apparent success from the redirect target.

Reqwest strips some standard sensitive headers across origins, but not these S3-specific headers.

**Fix:** Disable automatic redirects for signed S3 requests. Any supported redirect should undergo explicit destination validation and fresh signing.

**6. P1 — Dependency feature unification silently changes downloaded object bytes**

Locations: [tokio_backend.rs:51](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:51), [blocking.rs:33](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:33).

Another dependency enabling reqwest’s `gzip` feature enables transparent decompression in this library’s client. Reproduced: a stored 46-byte gzip object was returned as its uncompressed text, with encoding and length headers removed.

Enabling attohttpc compression elsewhere similarly changed sync downloads, while retaining headers describing the compressed bytes.

**Fix:** Explicitly disable every reqwest decompressor using `no_gzip`, `no_brotli`, `no_deflate`, and `no_zstd`. The attohttpc path needs a reliable raw-body option; disabling compression negotiation alone does not disable its decoder. [Reqwest documents this feature-unification concern](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html).

**7. P1 — Small-buffer stream reads can overflow the stack and abort the process**

Locations: [request_trait.rs:381](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:381), [request_trait.rs:425](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:425).

Every partial chunk read wraps the previous stream in another boxed `once(...).chain(previous_stream)`. The wrappers accumulate, creating recursively polled and dropped chains.

Reproduced with a `ResponseDataStream` containing one 1 MiB chunk and 16-byte reads: stack overflow and process termination with SIGABRT. Both runtime adapters contain the same algorithm.

**Fix:** Keep the current chunk and offset in persistent adapter state instead of building nested streams.

**8. P1 — Combining the advertised rustls client and credential features can panic**

Locations: [s3/Cargo.toml:103](/home/at/code/rust/rust-s3/s3/Cargo.toml:103), [s3/Cargo.toml:107](/home/at/code/rust/rust-s3/s3/Cargo.toml:107), [aws-creds/Cargo.toml:35](/home/at/code/rust/rust-s3/aws-creds/Cargo.toml:35).

With default features disabled, `tokio-rustls-tls,http-credentials-rustls-tls` enables ring through reqwest and aws-lc through attohttpc. The credential transport calls rustls’s implicit-provider builder.

A local HTTPS credential request reproduced a panic: “Could not automatically determine the process-level CryptoProvider.” This also reproduced with locked rustls 0.23.45. Compilation succeeds.

**Fix:** Select a consistent provider across both clients, or explicitly configure the credential transport’s provider instead of relying on global inference.

**9. P1 — The isolated ring credential feature has no working HTTPS transport**

Locations: [aws-creds/Cargo.toml:36](/home/at/code/rust/rust-s3/aws-creds/Cargo.toml:36), [s3/Cargo.toml:110](/home/at/code/rust/rust-s3/s3/Cargo.toml:110).

The selected attohttpc 0.30.1 ring feature enables `__rustls-ring`, but that dependency’s TLS module selection only recognizes `__rustls`. Consequently, an isolated `tokio-rustls-tls-ring` build selects attohttpc’s no-TLS implementation.

Reproduced an HTTPS request returning `Error(TlsDisabled)`. STS and HTTPS container credentials cannot work in this configuration; native TLS enabled elsewhere can mask the defect.

**Fix:** Upgrade or patch attohttpc’s TLS module and root-store feature gates, and add an actual HTTPS credential test.

`PR_TRIAGE.md` discusses ring dependency wiring, but does not record this runtime failure.

**10. P1 — EC2 metadata requests can expose credentials through proxies or redirects**

Locations: [credentials.rs:699](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:699), [credentials.rs:733](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:733).

Unlike container credential requests, IMDS requests retain attohttpc’s environment proxy settings and redirect following.

With a local `HTTP_PROXY`, `from_instance_metadata_v2(true)` sent all three metadata requests through the proxy, including the IMDS token, and accepted the proxy’s fabricated credentials. On EC2 the normal provider path uses the same request construction.

**Fix:** Disable proxies and redirects for every IMDS request, as the container provider already does. Check each metadata response’s status before consuming its body.

**11. P1 — STS web-identity tokens appear in transport debug logs**

Location: [credentials.rs:551](/home/at/code/rust/rust-s3/aws-creds/src/credentials.rs:551).

`from_sts` puts `WebIdentityToken` in the GET query string. The selected attohttpc implementation logs the complete path and query at debug level when sending requests. Enabling ordinary transport debug logging therefore exposes the bearer token.

This is code-verified; no real token was sent. The earlier `Credentials::Debug` redaction does not address this separate path.

**Fix:** Send the STS parameters in a form-encoded POST body and ensure that body is not logged.

**12. P1 — Supported async-std configurations retain client-reachable vulnerable dependencies**

Locations: [s3/Cargo.toml:88](/home/at/code/rust/rust-s3/s3/Cargo.toml:88), [s3/Cargo.toml:113](/home/at/code/rust/rust-s3/s3/Cargo.toml:113).

Dependency-tree verification confirms:

- Async-std rustls reaches `webpki 0.21.4`, whose certificate-path construction permits exponential CPU consumption from a pathological certificate chain. [RUSTSEC-2023-0052](https://rustsec.org/advisories/RUSTSEC-2023-0052.html)
- Async-std Hyper reaches `hyper 0.13.10`, with an HTTP/1 chunk-size overflow affecting clients and potentially causing data loss. [RUSTSEC-2021-0079](https://rustsec.org/advisories/RUSTSEC-2021-0079.html)

These are production dependency paths, not merely development dependencies. Exploitation was not attempted.

**Fix:** Replace or update the legacy transport branches. Compatible lockfile refreshes cannot overcome their old major-version constraints.

Earlier audit: already recorded and still open. I am not treating every advisory match as a reachable exploit.

**13. P2 — Aggregate object listing silently stops before all objects are returned**

Location: [bucket.rs:3368](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3368).

`list()` stops whenever `next_continuation_token` is absent, ignoring `is_truncated`. For ListObjects v1 without a delimiter, AWS can omit `NextMarker`; callers must continue from the last returned key.

A valid truncated v1 fixture returned successfully as a one-page listing. Conversely, repeated continuation tokens can cause endless requests and growing memory because this loop has no progress check. [AWS ListObjects pagination](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)

**Fix:** Honor `IsTruncated`, implement the v1 last-key fallback, and reject missing or repeated cursors where continuation is required.

**14. P2 — `object_exists` treats authorization and server failures as existence**

Location: [bucket.rs:1364](/home/at/code/rust/rust-s3/s3/src/bucket.rs:1364).

Without `fail-on-err`, the implementation returns `status_code != 404`. A local 403 response reproduced `Ok(true)`; 500 and other non-404 errors follow the same branch. Callers can consequently skip required uploads or make incorrect existence decisions.

**Fix:** Return true for successful HEAD responses, false for 404, and an error for other statuses, independently of the raw-response feature policy.

**15. P2 — Signing can mix different generations of credentials**

Locations: [request_trait.rs:1023](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:1023), [request_trait.rs:949](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:949), [request_trait.rs:502](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:502).

Token, secret key, and access key are read through separate lock acquisitions. A concurrent refresh between those reads can combine an old token or signing secret with a new access key, producing intermittent authentication failures. Presigning has the same multi-read problem.

**Fix:** Snapshot the complete credentials once per signing attempt and use that snapshot throughout.

Earlier audit: explicitly recorded at `AUDIT.md:247`; still open. Verified by tracing lock and signing operations; no scheduling reproduction was run.

**16. P2 — Cancelling refresh callers defeats the refresh concurrency gate**

Location: [bucket.rs:405](/home/at/code/rust/rust-s3/s3/src/bucket.rs:405).

The caller owns the refresh mutex guard while awaiting `spawn_blocking`. Cancelling that future releases the guard, but the blocking HTTP request continues. Its result is never installed. Repeated short-lived callers can therefore launch overlapping refreshes and accumulate blocking work while credentials remain expired.

**Fix:** Give an independently owned refresh task responsibility for holding the gate and publishing the result; callers should await the shared operation.

Earlier audit: cancellation was explicitly left open at `AUDIT.md:230`; it remains open. This finding is based on the cancellation semantics and code path.

**17. P2 — A stalled upload source prevents already-failed parts from being processed**

Location: [bucket.rs:2248](/home/at/code/rust/rust-s3/s3/src/bucket.rs:2248).

The multipart scheduler awaits the next input chunk before polling remaining upload futures whenever a slot opens.

Reproduced with concurrency two: part 2 succeeded, part 1 returned HTTP 500, and the source stalled before part 3. The upload never reported the received failure or reached cleanup; an external three-second timeout expired.

**Fix:** Poll source reads and active uploads together, so a pending reader cannot prevent completion/error processing.

**18. P2 — Streaming APIs can allocate unbounded memory for error bodies**

Locations: [tokio_backend.rs:179](/home/at/code/rust/rust-s3/s3/src/request/tokio_backend.rs:179), [async_std_backend.rs:213](/home/at/code/rust/rust-s3/s3/src/request/async_std_backend.rs:213), [blocking.rs:52](/home/at/code/rust/rust-s3/s3/src/request/blocking.rs:52).

With `fail-on-err`, a failed streaming download buffers the entire error response before returning. A large or endless error body can exhaust memory even though the caller deliberately selected a streaming API. The request deadline is not a byte limit.

**Fix:** Limit retained error bodies to a modest configurable size and indicate truncation while preserving status. Drop the remaining body rather than draining it without a bound.

**19. P2 — Generated blocking stream methods return streams tied to a destroyed runtime**

Locations: [bucket.rs:458](/home/at/code/rust/rust-s3/s3/src/bucket.rs:458), [bucket.rs:1818](/home/at/code/rust/rust-s3/s3/src/bucket.rs:1818).

`block_on_proc` creates and drops a Tokio runtime per generated call. `get_object_stream_blocking` returns as soon as headers arrive, then destroys the runtime still needed by the body.

A server withholding its body until the method returned reproduced a panic when the returned stream was consumed: “A Tokio 1.x context was found, but it is being shutdown.”

**Fix:** Exclude streaming-return methods from these wrappers, or retain a runtime for the stream’s lifetime. The new range-stream method has the same exposure.

**20. P2 — Header canonicalization does not implement SigV4 normalization**

Locations: [signing.rs:122](/home/at/code/rust/rust-s3/s3/src/signing.rs:122), [request_trait.rs:967](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:967).

The canonicalizer only trims outer whitespace. Internal repeated spaces remain, and repeated values become separate canonical lines. Reproduced output:

`x-amz-meta-test:a   b\nx-amz-meta-test:c`

AWS requires whitespace compression and comma-separated values. Separately, request-header merging uses `insert` repeatedly, discarding earlier values. These paths cause signature failures or lost metadata. [AWS canonical-header requirements](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html)

**Fix:** Preserve repeated values and canonicalize them once, with the required whitespace and joining rules.

Earlier audit: listed as a coverage gap at `AUDIT.md:323`; still defective.

**21. P2 — Upload and version identifiers are interpolated without query encoding**

Locations: [command.rs:75](/home/at/code/rust/rust-s3/s3/src/command.rs:75), [request_trait.rs:788](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:788), [request_trait.rs:816](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:816).

Opaque identifiers containing `+`, `&`, or `#` change meaning during URL parsing. Reproduced upload ID `a+b&other=x` became `uploadId = "a b"` plus a separate `other=x` parameter.

This affects part uploads, completion, abort, and explicit attribute version IDs. An affected multipart upload may fail and then be impossible to abort through the same construction.

**Fix:** Append identifiers using `query_pairs_mut().append_pair`, consistently across commands.

Earlier audit: identifier encoding was a coverage gap at `AUDIT.md:323`.

**22. P2 — Presigned URL assembly fails in three supported configurations**

Locations: [request_trait.rs:714](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:714), [request_trait.rs:615](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:615), [request_trait.rs:580](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:580).

Reproduced:

- Bucket extra queries produce `?versionId=v1?X-Amz-Algorithm=...`; the algorithm becomes part of the version value.
- Sync `presign_put` drops `custom_queries` from the emitted URL, although canonical signing still includes them.
- Endpoint `https://example.com:443/storage/v1/s3` duplicates `/storage/v1/s3` in the emitted URL while signing the unduplicated path.

**Fix:** Build one URL through structured query operations and sign exactly that URL. Share the sync/async implementation and use the authority, rather than the endpoint-plus-path, when reconstructing URLs.

**23. P2 — Presigned POST omits ordinary temporary session tokens**

Location: [post_policy.rs:94](/home/at/code/rust/rust-s3/s3/src/post_policy.rs:94).

POST policies only inspect `security_token`. Environment/profile credentials using `AWS_SESSION_TOKEN`, and STS credentials created by this crate, populate `session_token` instead.

A locally generated policy with session-token credentials omitted `x-amz-security-token` entirely. Its browser upload cannot authenticate with those temporary credentials.

**Fix:** Resolve the effective token consistently with ordinary signed requests and include it in both policy conditions and form fields.

**24. P2 — POST checksum policies require a field absent from the returned form**

Locations: [post_policy.rs:202](/home/at/code/rust/rust-s3/s3/src/post_policy.rs:202), [post_policy.rs:136](/home/at/code/rust/rust-s3/s3/src/post_policy.rs:136).

Serializing `AmzChecksumAlgorithm(SHA256)` adds a condition for `x-amz-checksum-algorithm = SHA256`. Form generation iterates the original conditions, so it never adds that synthetic field.

Reproduced: the generated policy requires the algorithm, but `PresignedPost.fields` omits it. Submitting the supplied form fails policy validation.

**Fix:** Expand synthetic conditions before both serialization and form-field generation, or explicitly insert the matching algorithm field.

**25. P2 — `get_object_attributes` requests an incompatible response and selects the wrong default version**

Locations: [request_trait.rs:1105](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:1105), [serde_types.rs:50](/home/at/code/rust/rust-s3/s3/src/serde_types.rs:50), [request_trait.rs:818](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:818).

The request asks only for `ETag`, but deserialization requires checksum, object parts, storage class, and size. A valid ETag-only response reproduced `missing field Checksum`. Individual checksum algorithms are also incorrectly mandatory.

Additionally, `version_id: None` sends `versionId=null`, selecting the null version rather than the current version in a versioned bucket. [AWS GetObjectAttributes contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html)

**Fix:** Align requested attributes with optional response fields; omit the version query when no version was specified.

**26. P2 — Public `Command::UploadPart` is not a valid multipart request**

Locations: [request_trait.rs:837](/home/at/code/rust/rust-s3/s3/src/request/request_trait.rs:837), [command.rs:421](/home/at/code/rust/rust-s3/s3/src/command.rs:421).

This variant sends content but omits `uploadId` and `partNumber`, and signs the empty-payload hash. Reproduced its generated URL as `/b/key` without a query and its hash as SHA-256 of an empty body.

Authenticated providers reject it; it is not equivalent to the working high-level multipart helper.

**Fix:** Implement it through the existing `PutObject { multipart: Some(...) }` path, or deprecate the unusable variant.

Earlier audit: explicitly recorded at `AUDIT.md:236`; still open.

**27. P2 — Sync small-stream uploads lose their requested content type and require unnecessary abort permission**

Location: [bucket.rs:2375](/home/at/code/rust/rust-s3/s3/src/bucket.rs:2375).

The sync implementation initiates multipart before reading. For a small object it aborts, then calls `put_object`, losing the supplied content type.

A three-byte `text/plain` upload reproduced POST initiation, DELETE abort, then PUT with `application/octet-stream`. A caller allowed to put objects but not abort multipart uploads fails before the otherwise-valid small PUT.

**Fix:** Read the first chunk before initiation, and use `put_object_with_content_type` for the small-object path.

**28. P2 — `etag_for_path` adds a nonexistent empty part at exact chunk boundaries**

Location: [utils/mod.rs:61](/home/at/code/rust/rust-s3/s3/src/utils/mod.rs:61).

The helper hashes and counts the final zero-byte read before checking for EOF. For an exactly 8 MiB file it returns a two-part ETag, while the async uploader sends one multipart part.

Reproduced different digests ending in `-2` and `-1` for the same bytes. Applications using this helper for integrity checks or synchronization will report false mismatches.

**Fix:** Stop before hashing an empty terminal read, with an explicit empty-file case. Document the upload strategy required for any predicted ETag.

**29. P2 — `location()` requests an object named `?location`**

Location: [bucket.rs:2632](/home/at/code/rust/rust-s3/s3/src/bucket.rs:2632).

The method supplies `"?location"` as the object path. The URL builder encodes it as `%3Flocation`, while the command contributes no location query. Reproduced URL: `/b/%3Flocation`.

Normal providers therefore return an object response or object-not-found error instead of bucket location.

**Fix:** Use an empty path and add the `location` query through the command’s URL construction.

**30. P2 — `Bucket::exists()` breaks default virtual-host addressing**

Location: [bucket.rs:792](/home/at/code/rust/rust-s3/s3/src/bucket.rs:792).

The method clears the bucket name on a clone but retains virtual-host addressing. A default AWS bucket therefore constructs `https://.s3.amazonaws.com/`, which was reproduced locally.

The separate `list_buckets` constructor already avoids this by selecting path style.

**Fix:** Switch the dummy bucket to path style before listing, or reuse the correctly constructed listing path.

**31. P2 — Tag serialization inserts values directly into XML**

Location: [bucket.rs:3090](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3090).

Tag keys and values are interpolated without XML escaping. Reproduced value `A&B` as `<Value>A&B</Value>`, an invalid XML document. Markup-bearing input can also change the generated structure rather than remain tag text.

**Fix:** Serialize a typed tagging document or escape text correctly, including XML newline handling where preservation matters.

**32. P2 — Malformed tag responses are reported as successful empty tag sets**

Location: [bucket.rs:3253](/home/at/code/rust/rust-s3/s3/src/bucket.rs:3253).

The parser uses `if let Ok(...)` and silently ignores failure. A local HTTP 200 response ending halfway through a tag reproduced `Ok(([], 200))`.

Missing Key/Value elements also become fabricated placeholder strings. Consumers can mistake malformed responses for absent tags and make incorrect metadata or access-control decisions.

**Fix:** Propagate parse and missing-field errors. Use a typed response model rather than placeholder values and ignored parser failures.

**33. P2 — The Cape Town region maps to a nonexistent endpoint**

Location: [region.rs:323](/home/at/code/rust/rust-s3/aws-region/src/region.rs:323).

`AfSouth1` returns `s3-af-south-1.amazonaws.com`. DNS lookup failed for that hostname, while `s3.af-south-1.amazonaws.com` resolved. AWS lists the dotted endpoint. [AWS endpoint reference](https://docs.aws.amazon.com/general/latest/gr/s3.html)

**Fix:** Change this mapping to `s3.af-south-1.amazonaws.com`.

**34. P2 — Bucket creation sends an invalid Object Lock enablement value**

Location: [bucket_ops.rs:196](/home/at/code/rust/rust-s3/s3/src/bucket_ops.rs:196).

Setting `object_lock_enabled = true` generates `x-amz-bucket-object-lock-enabled: Enabled`. This request parameter is a boolean; AWS specifies `true`. The generated header was reproduced locally. [AWS CreateBucket contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)

**Fix:** Emit `"true"` and add a request-header regression.

**35. P2 — Exported `retry!` depends on the consuming crate’s features and dependencies**

Location: [utils/mod.rs:411](/home/at/code/rust/rust-s3/s3/src/utils/mod.rs:411).

A downstream invocation reproduced a compile error because `log::warn!` resolves against the consumer’s dependencies. Its `#[cfg(feature = ...)]` checks also evaluate against the consumer’s features, not rust-s3’s selected backend. With the logging dependency supplied but no matching consumer feature names, all backoff branches disappear.

**Fix:** Move logging and backend-specific waiting into library-owned helpers, referenced through `$crate`, or retire the macro in favor of the existing internal retry implementation.

**Opportunities for simplification**

- Replace duplicated sync/async presigning code with one URL-building and signing implementation.
- Consolidate multipart parts around one command representation; remove the divergent `UploadPart` implementation.
- Use one credential snapshot and one token-selection helper throughout ordinary signing, presigning, and POST policies.
- Replace the tagging string builder and minidom parsing path with a typed XML model; retain explicit CR escaping where necessary.
- Replace recursive stream chaining with a conventional buffered adapter.
- Consider an explicit conservative multipart concurrency default plus caller override, avoiding the machine-memory heuristic and its `sysinfo` dependency.
- A maintained transport abstraction could eliminate several legacy dependency, framing, TLS-feature, and decompression problems together.

**Checked and found to be sound, within the tested scope**

- Copy and multipart completion reject HTTP 200 responses containing embedded S3 errors; the earlier repair remains present.
- High-level multipart uploads sort completed parts, preserve reader failures, and attempt cleanup without replacing the primary error on the covered failure paths.
- Per-call multipart metadata, encryption, ownership, and conditional headers are routed by operation; unsupported whole-object checksum/framing headers are rejected.
- Multipart-upload listing carries both cursors and rejects repeated/cyclic pagination.
- Internal retries classify operation safety and avoid replaying ambiguous ordinary mutations or consumed download bodies. Disabling status retries without `fail-on-err` is explicitly documented.
- Tokio and async-std request deadlines extend through response-body consumption. An arbitrary stalled caller-provided reader or writer still needs its own operation-level bound.
- The observer tests passed for attempt identity, retries, partial reads, errors, and drops. Callback execution and cancellation limitations are documented.
- Range-stream requests preserve status and response headers for caller validation; callers are explicitly responsible for checking the returned range.
- Credential Debug redaction, refresh-source preservation, and container endpoint/token validation remain intact.
- Both support crates are now integrated through workspace path dependencies; the earlier warning that S3 still used the registry region crate is stale.
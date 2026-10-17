# rust-s3 full code review: Claude Opus 5.5, 2026-10-16

Baseline: branch `prime` at `b2077f3` (working tree clean apart from syncthing temp files).
Scope: the whole workspace (`s3`, `aws-creds`, `aws-region`), with production-relevant issues as the bar.

Method: I read the non-test source of every module, then checked each finding marked
**verified** with scratch tests outside the repository. Those tests ran against a local fake
HTTP server, using the tokio + rustls and sync + rustls configurations. Findings marked
**by reading** are from code inspection alone. Findings that `AUDIT.md` already records are
labelled as such.

## Summary

| # | Sev | Finding | Status |
|---|-----|---------|--------|
| 1 | P1 | `ResponseDataStream` `AsyncRead` nests one stream adapter per partial read: quadratic time, then a stack-overflow **abort** | verified |
| 2 | P1 | sync and async-std (H1): a body cut short by the peer is returned as a complete, successful download | verified |
| 3 | P1 | Object keys with `.`/`..` segments are normalized by `Url::parse`, so the request goes to another key, or in path style another bucket | verified |
| 4 | P1 | `list()` in ListObjects v1 mode silently returns only the first page when no delimiter is given | verified |
| 5 | P2 | Multipart part futures aren't polled while the next chunk is read; finished parts hit the request timeout and the upload is aborted | verified |
| 6 | P2 | Default 60 s `request_timeout` is a total deadline that includes the body, so large downloads and streams die midway | by reading (intentional per AUDIT) |
| 7 | P2 | `presign_post` omits `session_token`, so POST policies fail with temporary credentials | verified |
| 8 | P2 | `x-amz-copy-source` is not URL-encoded | verified |
| 9 | P2 | `Bucket::location()` requests an object literally named `?location` | verified |
| 10 | P2 | `Bucket::exists()` on a subdomain-style bucket builds the host `.<endpoint>` | verified |
| 11 | P2 | `get_object_range*` panics on `start > end` (new in this fork) | verified |
| 12 | P2 | Credentials refresh only once already expired: no early-refresh margin | by reading (AUDIT: open) |
| 13 | P2 | sync: `presign_put` drops `custom_queries` from the URL but signs them | verified |
| 14 | P2 | sync: `head_object` never fills `content_type` (`"get-Type"` typo) | verified |
| 15 | P3 | sync: small `put_object_stream_with_content_type` loses the content type and wastes an initiate+abort | verified |
| 16 | P3 | sync: object of exactly N×8 MiB uploads an empty final part | by reading |
| 17 | P3 | `etag_for_path` is wrong for files of exactly N×8 MiB | verified |
| 18 | P3 | SigV4 canonical header values don't collapse inner spaces | verified (local canonical string) |
| 19 | P3 | `uploadId`/`versionId` query values are not encoded; a `+` is sent raw but signed as a space | verified (wire + canonical) |
| 20 | P3 | Presigning with an anonymous bucket panics | verified |
| 21 | P3 | Signing reads credential fields one at a time, so a concurrent refresh can mix generations | by reading (AUDIT: open) |
| 22 | P3 | Mid-body read errors on buffered GET/list are never retried | by reading |
| 23 | P3 | async-std `response_data` headers: `header_names().zip(header_values())` misaligns repeated headers | by reading |
| 24 | P3 | Unknown region strings silently become the endpoint hostname | by reading |
| 25 | P3 | Various: `blocking` builds a runtime per call, sync has no connection reuse, double hashing, observer exposes tokens, … | by reading |

## Findings

### 1. P1: `AsyncRead for ResponseDataStream` builds an ever-deeper stream chain, then aborts on stack overflow

`s3/src/request/request_trait.rs:380-387` (tokio) and `:424-431` (async-std):

```rust
if amt < chunk.len() {
    let remainder = chunk.slice(amt..);
    let previous_stream = std::mem::replace(&mut self.bytes, Box::pin(stream::empty()));
    self.bytes = Box::pin(stream::once(async move { Ok(remainder) }).chain(previous_stream));
}
```

Every partially consumed chunk wraps the stream in another `Chain<Once, Box<dyn Stream>>`. The
`Chain` keeps its `second` forever, so the wrapping is never undone. Every later `poll_read`
recurses through every layer. The depth grows by one per short read: a 256 KiB reqwest chunk
read with an 8 KiB buffer adds ~31 layers. The cost is quadratic, and the stack overflows once
the depth reaches tens of thousands.

Reproduction (release build, `#[tokio::main]` default multi-thread runtime with 2 MiB worker
stacks, local server, `get_object_stream` then `stream.read(&mut [0; 8192])` in a loop):

```
mib=8   reads=1027   elapsed=4.9ms
mib=32  reads=4101   elapsed=7.8ms
mib=128 reads=16391  elapsed=62.7ms
mib=512 thread 'tokio-rt-worker' has overflowed its stack
        fatal runtime error: stack overflow, aborting
```

`tokio::io::copy(&mut stream, &mut sink)` also aborts at 512 MiB and at 1 GiB. A debug build
with a 2 MiB test thread aborts at 32 MiB. The `bytes().next()` stream API is unaffected
(1 GiB in 268 ms). The repo's own test calls `tokio::io::copy` over this impl "a common use
case" (`request_trait.rs:1250`). The abort is not a panic, so it cannot be caught and it kills
the whole process. Chunk sizes are chosen by the server and the network, not by the caller.

The impl came from upstream `d106d33` ("preserve response stream chunk remainders"). Not in AUDIT.

Fix: keep the remainder in the struct (`pending: Option<Bytes>`) and serve from it before
polling the stream. This fork's `headers` field already changed the struct literal, so adding
one more (private) field now costs nothing extra. Alternatively, wrap once in a `Prepend`
stream type that owns `Option<Bytes>` plus the inner stream, and refill its head instead of
re-wrapping. Add a test with ≥100k short reads.

### 2. P1: sync and async-std (H1) report a truncated body as a successful download

Neither backend checks the bytes it received against `Content-Length`, and neither HTTP client
does it for them. attohttpc reads a `Content-Length` body through a plain `Take` (attohttpc
0.30.1 `parsing/body_reader.rs:12,87`), which treats an early EOF as the end of the body.
Surf's H1 client behaves the same way. hyper (tokio backend) does raise an error.

Verified: the server announces `content-length: 100`, sends 50 bytes, then closes.

```
sync       get_object            -> Ok((200, 50 bytes))
sync       get_object_to_writer  -> Ok(200), wrote 50 bytes
async-std  get_object            -> Ok(50 bytes)
async-std  get_object_to_writer  -> Ok(200), wrote 50
async-std  get_object_stream     -> read 50, end Ok(())
tokio      get_object            -> Err("reqwest: error decoding response body")
tokio      get_object_to_writer  -> Err(...), wrote 50
```

A dropped connection, a proxy or load-balancer reset sent as FIN, or a server crash mid-response
therefore yields a silently short object, with status 200, on plain HTTP (e.g. MinIO inside a
cluster). Over TLS it depends on whether the TLS stack reports an unclean close; I only tested
plain HTTP. attohttpc's own timeout watchdog is reported correctly as `TimedOut` (unix).

Fix: in `blocking.rs` (`response_data`, `response_data_to_writer`) and `async_std_backend.rs`
(`response_data`, `response_data_to_writer`, `body_stream`), count the bytes and fail with an
`io::ErrorKind::UnexpectedEof` error when a non-HEAD response with a `Content-Length` (and no
`Content-Encoding`) ends short. Make it a do-not-retry error once data has reached a writer or
stream. Add the 100/50 test above to each backend.

### 3. P1: Keys containing `.` or `..` segments silently address a different object or bucket

`request_trait.rs:772-779` pushes `uri_encode(path, false)` into the URL string, and `:844`
runs `Url::parse`. `.` is unreserved and so not encoded, and WHATWG URL parsing removes dot
segments (including `%2e`/`%2E` forms). Signing then uses the normalized URL, so the server
accepts the request as valid.

Verified on a path-style bucket:

```
put_object("dir/../../other-bucket/secret")  ->  PUT /other-bucket/secret HTTP/1.1
presign_get("dir/../../other-bucket/secret") ->  http://127.0.0.1:9000/other-bucket/secret?X-Amz-...
```

So a key like `a/./b` writes to `a/b`. A key with `..` escapes its prefix. In path style it can
even escape the bucket, with a valid signature, using the caller's credentials. That turns a
data-integrity bug into an authorization problem for any application that builds keys from
user input and relies on a prefix for isolation. Subdomain style is affected too (`/../x` →
`/x`).

AUDIT lists "dot-segment object keys" only under "further coverage needed".

Fix: reqwest only takes `url::Url`, so a literal `..` segment can't be sent at all. The robust
option is to reject keys that contain a `.` or `..` path segment, returning an `S3Error`
variant before any request or presign is built. As a cheap invariant, assert that
`url.path()` still ends with the encoded key.

### 4. P1: `list()` truncates silently in ListObjects v1 mode

`bucket.rs:3358-3373` loops while `next_continuation_token` is `Some`. For v1 that field is
the serde alias of `NextMarker` (`serde_types.rs:245`). S3 only returns `NextMarker` when a
delimiter was sent. Without one, the client is supposed to continue from the last `Key`.
`is_truncated` is never consulted.

Verified: one v1 page with `IsTruncated=true`, two keys and no `NextMarker`. `with_listobjects_v1().list("", None)`
returned `pages=1 keys=2 last_page_truncated=true` and made no second request.

Anything that syncs, deletes or reconciles from `list()` on a v1-only provider sees at most
1000 keys and no error. Not in AUDIT.

Fix: in v1 mode, when `is_truncated` and no `NextMarker` is present, continue from
`contents.last().key` (or the larger of that and the last common prefix). In both modes, fail
when `is_truncated` is set but the cursor didn't advance. That also gives v2 the cycle guard
that `list_multiparts_uploads` already has.

### 5. P2: Streamed multipart uploads starve in-flight parts and abort on spurious timeouts

`bucket.rs:2247-2297`: the inner `while` reads the next chunk (`read_chunk_async(reader).await`)
while the `FuturesUnordered` of part uploads goes unpolled. reqwest's `Pending::poll` checks
its total deadline *before* polling the response (reqwest 0.12.28 `async_impl/client.rs:3045`).
So a part whose response already arrived still fails with `TimedOut` if the reader took longer
than the remaining timeout. The first parts also aren't sent until the concurrency window has
filled.

Verified: 8 MiB chunks arriving 1.5 s apart, `request_timeout = 1 s`, `set_retries(0)`, two
concurrent chunks. The server answered every part immediately, yet the call returned
`Err(Reqwest(... partNumber=2 ..., source: TimedOut))`, and the requests were initiate, part 1,
part 2, then `DELETE ?uploadId` (abort).

With the defaults (60 s, one retry), this triggers once one 8 MiB chunk takes about a minute to
produce (a source slower than ~140 KB/s: pipes, compression, a slow upstream). The 8 MiB gets
re-sent, and if the retry is starved too, the whole upload is aborted.

Fix: drive uploads while reading, e.g. `select` between the next read and
`active_uploads.next()`, or spawn parts on a `JoinSet` (abort-on-drop keeps the current
"drop outstanding parts before abort" property).

### 6. P2: The default 60 s timeout covers the whole body

`bucket.rs:337` sets `DEFAULT_REQUEST_TIMEOUT = Some(60 s)`, and `tokio_backend.rs:219-223`
applies it as reqwest's total timeout, "from connect until the body has finished". async-std
does the same by design, as does sync (attohttpc `timeout`). A default `Bucket` therefore fails
`get_object_to_writer` and `get_object_stream` on any object that takes over a minute to
transfer, e.g. 1 GiB at 100 Mbit/s, after part of it has already gone to the writer.

AUDIT records this as the intended repair ("The default 60-second timeout is now enforced").
It is still the most likely way a default configuration breaks large transfers.

Suggest: keep a total deadline for buffered calls, and use an idle/read timeout (reqwest
`read_timeout`) for streaming bodies. At minimum, document this on `get_object_stream`,
`*_to_writer` and `put_object_stream`.

### 7. P2: `presign_post` drops the session token

`post_policy.rs:94-99` adds an `x-amz-security-token` condition only from `security_token()`.
STS web-identity credentials, `AWS_SESSION_TOKEN` from the environment, and
`Credentials::new(.., session_token, ..)` all put the token in `session_token`, so the policy
and its form fields carry no token and S3 rejects the upload.

Verified with `session_token = Some(..)`: the fields were `["Policy", "bucket", "x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-signature"]`.
For the same bucket, `presign_get` did include `X-Amz-Security-Token`.

Related: the three code paths disagree on precedence. Headers prefer
`session_token` (`request_trait.rs:1019-1029`), query presigning prefers `security_token`
(`:709-713`), and POST reads only `security_token`. A single `Credentials::token()` helper
would fix all three.

### 8. P2: `CopyObject` source is sent unencoded

`request_trait.rs:996-997` and `bucket.rs:1234-1239`: `x-amz-copy-source` is `bucket/key` taken
verbatim, but S3 requires this value to be URL-encoded. Verified wire value:
`test-bucket/dir/a b+c%41?versionId=x`. S3 decodes it, so `%41` becomes `A`, `?` starts a
`versionId`, and `+` is likely read as a space. The result is a copy of the wrong object, or
`NoSuchKey`. A non-ASCII key fails locally with `ReqwestHeaderToStr`.

Fix: `uri_encode(&format!("{bucket}/{key}"), false)`.

### 9. P2: `Bucket::location()` asks for an object named `?location`

`bucket.rs:2632` passes `"?location"` as the *path*. `url()` percent-encodes it, and
`Command::GetBucketLocation` appends no query (`request_trait.rs:829`). Verified request line:
`GET /test-bucket/%3Flocation HTTP/1.1`. Against S3 that is a `GetObject` of the key
`?location`: a 404, or with `fail-on-err` disabled, `Region::Custom { region: "Error encountered : ..." }`.
No test covers it.

Fix: pass `""` and add `Command::GetBucketLocation => url_str.push_str("?location")`.

### 10. P2: `Bucket::exists()` only works in path style

`bucket.rs:793-796` clones `self`, blanks the name, and lists buckets without forcing path
style the way `list_buckets` does (`:743`). Subdomain style (the default) produces
`format!("{}.{}", "", host)`. Verified URL: `http://.localhost:9/`. Against AWS this would be
`https://.s3.amazonaws.com/`. The repo's own `exists` tests all use `.with_path_style()`.
Separately, `exists()` needs `s3:ListAllMyBuckets` and returns `false` for buckets the caller
can use but doesn't own. A `HeadBucket` would be the usual check.

Fix: `dummy_bucket.with_path_style()` at minimum.

### 11. P2: Range methods panic on `start > end`

`command.rs:192` asserts in `Command::get_object_range`, which backs the public
`get_object_range`, `get_object_range_to_writer` and `get_object_range_stream`. None of their
docs mention a panic. Verified: `get_object_range("k", 10, Some(5))` panics with
`range start 10 is past its end 5`. Callers that forward user-supplied ranges (proxies,
range-serving handlers) crash a task, or the process under `panic = "abort"`. Returning
`Err(S3Error::...)` keeps the invariant without the crash.

Note that none of the range methods check for a 206 response. If the server or an
intermediary ignores `Range`, `get_object_range_to_writer` writes the whole object into the
writer, and `get_object_range` buffers the whole object. Only `get_object_range_stream` lets a
caller check first.

### 12. P2: No early-refresh margin for expiring credentials

`bucket.rs:395-410` and `aws-creds/src/credentials.rs:477-478` refresh only when
`expiration <= now`. A request signed at T−1 s that arrives after expiry, or any request from a
host whose clock runs behind, gets `ExpiredToken`. With IMDS/ECS/STS credentials this repeats at
every rotation. SDKs refresh around 5 minutes early, and IMDS has the next credentials ready by
then. AUDIT notes "proactive expiry margins remain open".

Fix: refresh when `expiration - margin <= now` (e.g. 5 min, clamped for short-lived tokens).

### 13. P2 (sync): `presign_put` drops custom queries but signs them

The `sync_impl` of `presigned()` (`request_trait.rs:611-615`) pattern-matches `PresignPut` with
`..` and passes `None` for the queries. `presigned_canonical_request` (`:676-698`) still signs
them. Verified: `presign_put("k", 600, None, Some({"x-id": "PutObject"}))` returns a URL with no
`x-id`, so the parameter is lost, and the signature covers a query the URL doesn't carry, so S3
answers `SignatureDoesNotMatch`. The async copy is correct. The two near-identical 50-line
copies are how they drifted (see Simplifications).

### 14. P2 (sync): `head_object().content_type` is always `None`

`utils/mod.rs:289` reads `headers.get("get-Type")`. Verified: the server sent
`content-type: text/html`, but `content_type` came back `None`. Collapsing the three
`HeadObjectResult` `From` impls into one over `http::HeaderMap` removes this whole class of bug.

### 15. P3 (sync): small streamed uploads lose their content type

`bucket.rs:2358-2376`: sync `put_object_stream_with_content_type` always initiates a multipart
upload. When the first chunk is short, it aborts that upload and falls back to
`self.put_object(..)`, which uses `application/octet-stream`. Verified wire sequence for a
13-byte `text/html` body:
`POST ?uploads (text/html)`, then `DELETE ?uploadId=u1`, then `PUT (application/octet-stream)`.
That is two extra round trips plus the wrong `Content-Type`. The async version reads the first
chunk before initiating; sync should do the same, and call `put_object_with_content_type`.

### 16. P3 (sync): exact multiples of 8 MiB end with an empty part

Same function, `:2367-2386`: after the last full chunk, `read_chunk` returns an empty `Vec`,
which is `< CHUNK_SIZE` with `part_number > 0`, so a 0-byte part gets uploaded. The async path
skips it (`chunk_len == 0`). AWS accepts an empty last part, but it's an extra request and the
ETag differs from the async path. Some S3-compatible stores are stricter.

### 17. P3: `etag_for_path` is wrong for files of exactly N × 8 MiB

`utils/mod.rs:60-68` counts the trailing empty read as a part. Verified for one 8 MiB file:
`etag_for_path` gives `8a4d…-2`, while the async upload of that file is a single part with ETag
`a2ec…-1`. Callers comparing ETags to detect changes will re-upload or flag corruption.

### 18. P3: Canonical header values keep runs of spaces

`signing.rs:122-133` trims each value but doesn't collapse internal whitespace, which SigV4's
"Trimall" requires. Verified: `x-amz-meta-note: a  b` canonicalizes as `x-amz-meta-note:a  b`,
so user metadata or `Content-Disposition` containing double spaces gets `SignatureDoesNotMatch`.
AUDIT lists header whitespace as coverage needed.

### 19. P3: Upload and version IDs are not encoded in the query

`command.rs:67-72` and `request_trait.rs:787-792,814-819` interpolate `upload_id` and
`version_id` raw. Verified: for upload ID `abc+def/ghi=`, the request line is
`?partNumber=1&uploadId=abc+def/ghi=`, but the canonical query signs `uploadId=abc%20def%2Fghi%3D`.
If a provider issues IDs with `+`, `&`, `#` or `%`, every part, complete and abort fails. AWS
IDs don't include such characters as far as I know. This is cheap hardening:
`query_pairs_mut().append_pair`.

### 20. P3: Presigning with anonymous credentials panics

`request_trait.rs:495-506` `expect`s a secret key. Verified: `Bucket::new_public(..).presign_get(..)`
panics with `Secret key must be provided to sign headers, found None`. Likewise,
`authorization()` panics (`:949`) if a secret is present without an access key. Both should be
`S3Error::Credentials(..)`.

### 21. P3: Signing reads credential fields separately

`headers()` / `authorization()` / `signing_key()` / `presigned_url_no_sig()` each take the
credentials lock (and clone the whole struct) separately for the access key, secret key and
tokens. A concurrent refresh between those reads produces a signature from mixed generations.
This is already in AUDIT (open). The fix is one snapshot per attempt, passed down; it also
removes four `Credentials` clones per request.

### 22. P3: Mid-body failures on buffered reads are never retried

`tokio_backend.rs:286-325`: `retry_request` covers only up to the response headers. `collect()`
runs outside it, so a connection reset halfway through `get_object` or a list page goes
straight to the caller. Nothing has been handed out yet at that point, so retrying the whole
GET is safe. This is a common S3 transient failure. Same shape in the async-std and sync
backends.

### 23. P3 (async-std): `response_data().headers()` can be mislabeled

`async_std_backend.rs:311-315` zips `header_names()` with `header_values()`. The second
flattens every value of every header, so one repeated header shifts all later name/value pairs.
The recent `header_map()` fix covers `response_header` and the stream, but not this path.

### 24. P3: Unknown region names become hostnames

`aws-region/src/region.rs:303-306`: `"us-gov-west-1".parse::<Region>()` (or any AWS region added
after this list was written) yields `Custom { endpoint: "us-gov-west-1" }`. Requests then go to
`https://bucket.us-gov-west-1/` and fail at DNS, with no hint why. Consider
`s3.<region>.amazonaws.com` for names that look like AWS regions, or an error.

### 25. P3: Smaller production notes

- `blocking` feature: `block_on_proc` creates a fresh multi-thread tokio `Runtime` per `*_blocking`
  call (one worker per CPU, 32 here). Connections don't survive between calls, and calling it
  from inside a runtime panics.
- sync backend: a new `attohttpc::Session` per request (`blocking.rs:33,88`) means no keep-alive,
  so each request pays a TCP+TLS handshake. The async-std H1 path disables keep-alive on purpose.
- Every attempt hashes the body with SHA-256 twice (`headers()` at `request_trait.rs:960`, then
  `canonical_request()` at `:932`), with MD5 on top, and copies it (`request_body()` →
  `Vec::from`). `PutObjectRequest::new` copies it once more. For 8 MiB parts that is wasted
  CPU and memory at upload concurrency.
- `RequestObserver::on_request` hands over the signed `reqwest::Request`, including
  `authorization` and `x-amz-security-token`. The docs should say not to log headers verbatim.
- `Bucket`'s derived `Debug` prints `extra_headers`, which can hold an SSE-C key.
- `_tags_xml` (`bucket.rs:3085-3104`) doesn't XML-escape tag keys/values (`&` → MalformedXML).
- `delete_objects` deserializes a 200 `<Error>` body as an empty `DeleteObjectsResult` (no
  deleted, no errors); `validate_success_xml_response(.., "DeleteResult")` would catch it.
- Retry backoff is `n²` seconds with no jitter. `set_retries(255)` adds up to ~64 days of sleeps, and
  synchronized clients retry in lockstep.
- Fixed `CHUNK_SIZE` caps streamed uploads at 10,000 × 8 MiB ≈ 78 GiB. Larger objects fail after
  uploading 78 GiB.
- async-std: `ResponseDataStream` implements `async_std::io::Read`, so with `ReadExt` in scope
  `stream.bytes()` resolves to `ReadExt::bytes(self)`, a by-value byte iterator, instead of
  the inherent `bytes(&mut self)`. The field `stream.bytes` still works.
- Without `fail-on-err`, `*_to_writer` streams S3's XML error document into the caller's writer.
  This is documented semantics, but worth a doc warning on those methods.

## Simplification opportunities

- **`tokio_backend::execute`** converts `http::HeaderMap` into `reqwest::header::HeaderMap`
  name-by-name. These are the same type in reqwest 0.12, and the `filter(is_ok)` silently drops
  anything that fails, after it was signed. Pass the map straight through.
- **One `From<&http::HeaderMap> for HeadObjectResult`** instead of three ~100-line copies
  (`utils/mod.rs:111-367`). That removes the sync typo (#14) and the inconsistent
  `unwrap_or_default` parsing (`Content-Length: abc` → `Some(0)` on tokio, `None` on async-std).
- **`Bucket` builder methods** (`with_path_style`, `with_extra_headers`, `with_extra_query`, two
  `with_request_timeout`s, `with_listobjects_v1`, `set_dangerous_config`, `set_proxy`) each list
  every field under `cfg`s. `let mut b = self.clone(); b.x = ..; b` cuts ~150 lines, and adding
  a field can no longer be forgotten in one of them.
- **`presigned()` / `presigned_url_no_sig()`** exist as async and sync copies; one
  `#[maybe_async]` body would have prevented #13.
- **`Command::UploadPart`** is unused, has no URL query and signs the empty-body hash (AUDIT P2).
  Delete it.
- **`Command::content_type`/`sha256`/`content_length`**: 30-arm matches that are mostly
  `"text/plain"`/`EMPTY_PAYLOAD_SHA`/`0` could use a `_ =>` arm, and each body could be
  serialized once (the CompleteMultipartUpload XML is currently built four times per attempt).
- The **async-std and sync `response_status`** duplicate request construction that tokio already
  folded into `execute()`.
- `Bucket::create` / `create_with_path_style` are copies; the unused `sync: bool` field in all
  three request structs can go.

## Checked and found fine

- SigV4 query canonicalization: pairs are percent-encoded before sorting, and the `FRAGMENT`
  set covers everything outside the unreserved set.
- `read_chunk*` returns short only at EOF, so the async multipart path can't end early. Parts
  are re-sorted before completion, and any part failure aborts the upload with the original
  error kept.
- Retry policy: non-idempotent operations (plain PUT, DELETE, initiate, complete, copy) are not
  replayed; only reads and re-sendable parts are.
- CopyObject and CompleteMultipartUpload reject 200-with-`<Error>` bodies.
- `Credentials`/STS `Debug` redact secrets. Container-credential URIs are host-restricted, with
  redirects and proxies off for plain HTTP.
- The `bytes()` stream API of `ResponseDataStream` (1 GiB without issue), and the `observer`
  tee, which by reading ends its body observer exactly once on every exit path. Of the
  async-std configurations I ran only the truncation check (`async-std-rustls-tls`, with
  `RUSTFLAGS='--cfg rustix_use_libc'`).
- Sync attohttpc's own `Content-Type: application/octet-stream` on bodyless requests is unsigned
  and doesn't override a caller's signed `Content-Type`.

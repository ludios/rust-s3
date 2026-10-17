# Codex (GPT-5) full rust-s3 repository review — 2026-10-16

## Executive summary

I found **29 production-relevant issues**:

- **P0:** 0
- **P1:** 10
- **P2:** 18
- **P3:** 1

The most serious issues can redirect operations to the wrong S3 key, delete the wrong object, expose credentials and object contents through redirects or proxies, overflow the stack while streaming, truncate object listings, or silently select credentials from the wrong identity.

This was a read-only review of the complete working tree, including the fork-specific commits. I did not inspect `ai-code-reviews/` or `.syncthing.*.tmp` files and did not modify the repository.

## P1 — High

### 1. Dot-only key components are normalized, redirecting requests to another key or bucket

**Location:** `s3/src/request/request_trait.rs:772-844`, `s3/src/signing.rs:88-103`

**Failure scenario:** An application writes key `a/../victim`. URL parsing normalizes this to `victim`, so the request can overwrite or delete a different object. With path-style addressing, `../other-bucket/victim` can even escape the intended bucket path.

**Evidence:** The code percent-encodes the key while leaving `.` unchanged, concatenates it into a URL string, and reparses it through `url::Url`, whose path processing removes dot segments. A local fixture produced:

```text
a/../victim          -> /bucket/victim
a/./b                -> /bucket/a/b
../other-bucket/key  -> /other-bucket/key
```

S3 explicitly requires that canonical request paths not be normalized because doing so changes the object key. [AWS SigV4 documentation](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html)

**Suggested fix:** Encode dot-only segments as `%2E` and `%2E%2E` before handing the path to `Url`, or construct the request URI through an API that preserves the already-encoded path verbatim. Add wire-level tests for `.`, `..`, repeated slashes, and path-style bucket escape attempts.

`AUDIT.md` mentioned dot keys as a coverage gap, but did not record the defect.

---

### 2. Bulk deletion does not preserve carriage returns in object keys

**Location:** `s3/src/serde_types.rs:445-464`

**Failure scenario:** Deleting `a\rb` serializes a literal carriage return into XML. XML parsing normalizes it to a line feed, so the provider can interpret the requested key as `a\nb`. If both objects exist, the wrong object can be permanently deleted.

**Evidence:** The generated `DeleteObjects` XML contained a literal `\r`. XML escaping of `<`, `>`, and `&` does not prevent XML end-of-line normalization. AWS’s own DeleteObjects example uses `&#13;` specifically to preserve a carriage return. [DeleteObjects API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html), [S3 object-key guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)

**Suggested fix:** Use an S3/XML-specific serializer that emits `&#13;` and `&#10;` for carriage return and line feed and rejects XML-illegal code points. Test round trips for CR, LF, tabs, ampersands, and non-ASCII keys.

---

### 3. `x-amz-copy-source` is not URL-encoded and can select the wrong source object

**Location:** `s3/src/bucket.rs:1234-1251`, `s3/src/request/request_trait.rs:996-998`

**Failure scenario:** Copying the literal key `a%2Fb` sends `x-amz-copy-source: bucket/a%2Fb`. S3 decodes `%2F`, selecting `a/b` instead. The destination may be silently overwritten with data from the wrong source object.

**Evidence:** A local request fixture observed the raw header `bucket/a%2Fb`. AWS requires `x-amz-copy-source` to be URL-encoded. Other characters, such as spaces and non-ASCII bytes, can instead cause header construction or signature failures. [CopyObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)

**Suggested fix:** Encode the source bucket and key exactly once, preserving path-separator slashes but encoding literal `%` as `%25`. Append and encode `versionId` through a query builder rather than string interpolation.

---

### 4. Tokio and sync backends follow redirects for signed uploads

**Location:** `s3/src/request/tokio_backend.rs:47-79,191-224`, `s3/src/request/blocking.rs:29-50,85-109`

**Failure scenario:** A compromised or misconfigured S3-compatible endpoint returns a 307 redirect to another origin. The client resends the PUT body and sensitive S3 headers to that origin, then reports the attacker’s 200 response as upload success.

**Evidence:** Neither backend disables its HTTP client’s default redirect behavior. In a local two-server fixture:

- Tokio resent the object body and `x-amz-security-token` cross-origin.
- Sync resent the body, session token, and full `Authorization` header.
- Both returned `Ok(200)`.

SSE-C keys and other `x-amz-*` secrets are likewise at risk. The Surf configuration did not exhibit automatic redirects.

**Suggested fix:** Configure reqwest with `redirect(Policy::none())` and attohttpc sessions with `follow_redirects(false)`. Surface all 3xx responses to the caller; signed requests should not be transparently redirected.

---

### 5. EC2 instance-metadata requests honor `HTTP_PROXY`

**Location:** `aws-creds/src/credentials.rs:277-290,699-755`

**Failure scenario:** An EC2 workload has `HTTP_PROXY` configured without a suitable `NO_PROXY` rule. A proxy can answer the link-local IMDS token and credential requests with attacker-chosen credentials. The library accepts and uses those credentials for S3 operations.

**Evidence:** A local proxy fixture intercepted the IMDSv2 token PUT, role-name GET, and credential GET, and `from_instance_metadata_v2(true)` accepted the fake credential JSON. Attohttpc uses environment proxy settings by default. The container-credential path at `aws-creds/src/credentials.rs:427-446` already disables proxying and redirects, but IMDS does not.

**Suggested fix:** Centralize metadata HTTP construction and use empty proxy settings plus `follow_redirects(false)` for every IMDSv1/v2 request. Validate the status of each response before parsing it.

---

### 6. Public `Debug` implementations expose object data and customer-provided encryption keys

**Location:** `s3/src/bucket.rs:369-378`, `s3/src/put_object_request.rs:53-60,228-235`, `s3/src/command.rs:82-152`

**Failure scenario:** An application logs `Bucket`, `Command`, or `PutObjectRequest` during debugging or error handling. Logs can contain the complete buffered object, `x-amz-server-side-encryption-customer-key`, tokens in custom headers/query parameters, tags, or bucket policies.

**Evidence:** These types derive `Debug` over raw payloads and arbitrary header/query maps. Headers inserted by the public APIs are not marked sensitive, so `HeaderValue` debugging does not reliably redact them.

**Suggested fix:** Implement manual `Debug`:

- Report payload lengths instead of bytes.
- Redact all arbitrary header/query values, or at minimum known credentials, tokens, presigned parameters, cookies, and SSE-C headers.
- Mark sensitive `HeaderValue`s as sensitive when inserted.

`AUDIT.md` records the earlier credential and nested bucket-credential redaction work, but these remaining fields are still exposed.

---

### 7. Small-buffer reads from `ResponseDataStream` can overflow the stack

**Location:** `s3/src/request/request_trait.rs:359-444`

**Failure scenario:** A consumer reads a streaming response one byte at a time. Each partial read wraps the remaining stream in another `once(...).chain(...)`. Eventually EOF polling recurses through tens of thousands of nested stream combinators and aborts the process with a stack overflow.

**Evidence:** A local 64 KiB, one-byte-read fixture caused stack overflow in both Tokio and async-std implementations. The chain depth grows with the number of partial reads, not with the number of network chunks.

**Suggested fix:** Store the current `Bytes` chunk and an offset directly in `ResponseDataStream`. Consume it before polling the underlying stream again. Add tests that read a large single chunk through one-byte and small irregular buffers through EOF.

---

### 8. Object listing can silently truncate V1 results or loop forever

**Location:** `s3/src/bucket.rs:3280-3373`, `s3/src/serde_types.rs:239-246`, `s3/src/request/request_trait.rs:850-891`

**Failure scenario:**

- A V1 listing is truncated but has no `NextMarker`, which is valid when no delimiter is supplied. The code stops after the first page and silently omits objects.
- A broken provider repeats a V2 continuation token. The code loops forever while retaining every page in memory.
- Keys containing XML control characters can make an unencoded listing response unparsable.

**Evidence:** Pagination stops based on a missing continuation value rather than `IsTruncated`. A local truncated V1 response without `NextMarker` produced one page only. There is no repeated-token detection or overall page limit. Requests also omit `encoding-type=url`. AWS specifies that clients should use the last returned key when a truncated V1 result omits `NextMarker`. [ListObjects API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)

**Suggested fix:**

- Stop only when `IsTruncated` is false.
- For V1, fall back to the last returned key when `NextMarker` is absent.
- Require progress and detect repeated cursors.
- Request `encoding-type=url` and decode key fields.
- Consider exposing a lazy page stream instead of accumulating all pages.

---

### 9. The default credential chain can silently fall through to a different identity

**Location:** `aws-creds/src/credentials.rs:621-641`

**Failure scenario:** Environment variables, a web-identity configuration, or a container credential endpoint are present but malformed or temporarily unavailable. Rather than fail closed, the chain continues to a profile, node role, or IMDS identity belonging to another account. Uploads or deletes can then occur under the wrong principal.

**Evidence:** Providers are joined using `or_else`, collapsing “not configured” and “configured but broken” into the same fallback behavior. Direct credentials also accept an access key without requiring a secret key. For example, a partial environment credential can silently select a profile instead.

**Suggested fix:** Give provider resolution at least three outcomes: absent, successfully configured, and configured-but-invalid. Continue only for absent providers. Reject partial access-key pairs and preserve the provider’s original error.

`AUDIT.md` states credential resolution was intentionally left unchanged; this remains open.

---

### 10. Optional async-std transport graphs contain exploitable legacy dependencies

**Location:** `s3/Cargo.toml:71,88-89,112-113`

**Failure scenario:** A service enabling the affected async-std configurations processes hostile network responses. Legacy TLS, HTTP/1, HTTP/2, and Hyper implementations contain known infinite-loop, CPU-exhaustion, request-smuggling, resource-exhaustion, or response-parsing vulnerabilities.

**Evidence:** `cargo audit --file Cargo.lock` reported 15 vulnerabilities. Important reachable examples include:

- `surf 2.3.2` → `rustls 0.18.1`, `webpki 0.21.4`, and `ring 0.16.20`, including [RUSTSEC-2024-0336](https://rustsec.org/advisories/RUSTSEC-2024-0336.html) and [RUSTSEC-2023-0052](https://rustsec.org/advisories/RUSTSEC-2023-0052.html).
- `with-async-std-hyper` → `hyper 0.13.10` and `h2 0.2.7`, including the critical transfer-encoding issue [RUSTSEC-2021-0079](https://rustsec.org/advisories/RUSTSEC-2021-0079.html) and request smuggling [RUSTSEC-2021-0078](https://rustsec.org/advisories/RUSTSEC-2021-0078.html).

The ignored lockfile also currently resolves `rustls 0.23.45`, behind the fixes in 0.23.46, although downstream consumers without this lockfile may resolve a newer patch automatically.

**Suggested fix:** Replace or remove the Surf/http-client and Hyper 0.13 configurations; updating the lockfile alone cannot modernize those pinned legacy graphs. Refresh the current rustls resolution and add security-audit CI for every supported feature family.

This is the still-open P1 dependency-graph finding in `AUDIT.md`.

## P2 — Medium

### 11. Presigned URL construction is invalid in several supported configurations

**Location:** `s3/src/request/request_trait.rs:552-652,702-758,846-848`, `s3/src/signing.rs:228-315`

**Failure scenario:**

1. Sync `PresignPut` signs custom query parameters but omits them from the returned URL.
2. Presigning a bucket carrying existing query parameters produces `?versionId=v1?X-Amz-Algorithm=...`.
3. Custom endpoints with a path and explicit default port can duplicate the endpoint path.

All three produce unusable signatures or target the wrong URL.

**Evidence:** Local fixtures showed:

```text
Tokio PresignPut custom tagging: present
Sync PresignPut custom tagging:  absent

...?versionId=v1?X-Amz-Algorithm=...

http://localhost:80/storage
  -> /storage/storage/bucket/key
```

**Suggested fix:** Build presigned URLs from one structured `Url`, append every query item through `query_pairs_mut`, and share the URL-building code across sync and async implementations. Parse endpoint authority and base path once rather than reconstructing them from `host()`.

`AUDIT.md` mentioned related presigning cases only as coverage gaps.

---

### 12. Presigned POST omits required session-token and checksum fields

**Location:** `s3/src/post_policy.rs:61-100,119-159,195-210`

**Failure scenario:** A presigned POST generated from ordinary STS/session credentials lacks `x-amz-security-token`, so AWS rejects the upload. A policy using a checksum condition returns a form without `x-amz-checksum-algorithm`, despite signing a policy that requires it.

**Evidence:** The code checks `security_token`, while credentials loaded from common sources populate `session_token`. The checksum-algorithm condition is synthesized only while serializing the policy and is not added to the returned form field set. Both omissions were reproduced locally. AWS requires session tokens to be included in the form and policy for temporary credentials. [SigV4 POST policy documentation](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-HTTPPOSTConstructPolicy.html)

**Suggested fix:** Use one credential snapshot and select `session_token.or(security_token)`. Materialize synthetic checksum conditions before both policy serialization and form-field generation.

The temporary-credential case was listed as an untested area in `AUDIT.md`.

---

### 13. `GetObjectAttributes` requests and response types contradict each other

**Location:** `s3/src/request/request_trait.rs:814-819,1096-1108`, `s3/src/serde_types.rs:49-105`, `s3/src/bucket.rs:1297-1313`

**Failure scenario:** Calling the method without a version sends `versionId=null`, selecting the null version rather than the current version. The request asks only for ETag, but deserialization requires checksum, object-parts, storage-class, and object-size fields, so a valid ETag-only response fails to parse.

**Evidence:** Local output contained `?attributes&versionId=null`. Parsing a response containing only the requested ETag returned `missing field Checksum`. The API allows callers to select the requested attributes. [GetObjectAttributes API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html)

**Suggested fix:** Omit `versionId` when absent and encode it through the URL query API. Either request all fields represented by the return type or make optional fields optional and expose attribute selection to callers.

---

### 14. Multipart upload IDs and version IDs are interpolated without query encoding

**Location:** `s3/src/command.rs:66-71`, `s3/src/request/request_trait.rs:787-819`

**Failure scenario:** An S3-compatible provider returns an opaque upload ID containing `+`, `&`, `#`, or `=`. Subsequent part, completion, or abort calls split or alter the identifier, causing permanent failure and potentially leaving billed orphaned parts. Version IDs have the same problem.

**Evidence:** With upload ID `opaque+a&b#c`, the built URL parsed `uploadId` as `opaque a` and added a separate `b` parameter.

**Suggested fix:** Append all upload and version IDs through `Url::query_pairs_mut().append_pair(...)`; never interpolate opaque IDs into query strings.

`AUDIT.md` listed reserved upload/version IDs as a coverage gap, not a resolved defect.

---

### 15. SigV4 canonical headers mishandle whitespace and repeated fields

**Location:** `s3/src/signing.rs:121-143`

**Failure scenario:** A custom signed header contains sequential spaces or appears more than once. The library constructs a canonical request different from the provider’s, resulting in authentication failure.

**Evidence:** Values are trimmed only at their ends; internal spaces and tabs are not collapsed. Repeated values are emitted as separate canonical header lines while the header name occurs once in `SignedHeaders`. Local fixtures reproduced both forms. SigV4 canonicalization requires normalized whitespace and grouped values. [AWS canonical-request description](https://docs.aws.amazon.com/java/api/latest/software/amazon/awssdk/http/auth/aws/internal/signer/V4CanonicalRequest.html)

**Suggested fix:** Group all values by lowercase header name, trim and collapse spaces/tabs, join repeated values with commas, and generate one sorted canonical line and one unique signed-header entry per name.

This was an `AUDIT.md` coverage gap and is still open.

---

### 16. `Bucket::location` requests an object named `?location`

**Location:** `s3/src/bucket.rs:2631-2654`, `s3/src/request/request_trait.rs:829`

**Failure scenario:** Instead of issuing `GET /bucket?location`, the method requests `/bucket/%3Flocation`. It can therefore return an object response, a 404, or fabricate a custom empty region rather than report the bucket’s location.

**Evidence:** A local request fixture captured `/bucket/%3Flocation`. The API requires the `location` subresource, and AWS also defines special null/`EU` mappings. [GetBucketLocation API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html)

**Suggested fix:** Use an empty object path plus a structured `location` query parameter. Propagate status and parse errors and map null to `us-east-1` and `EU` to `eu-west-1`. AWS recommends `HeadBucket` for region discovery.

---

### 17. `object_exists` treats every non-404 response as existence

**Location:** `s3/src/bucket.rs:1360-1365`

**Failure scenario:** A HEAD request receives 403, 429, 500, or 503. With `fail-on-err` disabled, `object_exists` returns `Ok(true)`, so applications can skip uploads, assume stale data is present, or make incorrect recovery decisions.

**Evidence:** The implementation returns false only for 404. A local 503 response produced `Ok(true)`.

**Suggested fix:** Return true only for successful 2xx status, false only for 404, and an error for every other status regardless of the global `fail-on-err` feature.

---

### 18. Tagging serialization is invalid, while malformed tagging responses are accepted

**Location:** `s3/src/bucket.rs:3085-3103,3227-3276`

**Failure scenario:** A tag value such as `a&b` is sent as malformed XML and rejected by the provider. Conversely, a malformed successful response can be converted into an empty or placeholder tag set and reported as success.

**Evidence:** A local request contained raw `a&b`. Parsing malformed `<Tagging>` returned `Ok(([], 200))`; missing `Key` or `Value` elements can be replaced with default values.

**Suggested fix:** Use typed `quick_xml` serialization with proper escaping. Make response deserialization strict about the expected structure and propagate malformed XML or missing mandatory fields.

---

### 19. CreateBucket object-lock header uses the wrong value

**Location:** `s3/src/bucket_ops.rs:185-198`

**Failure scenario:** Creating a bucket with object lock enabled sends `x-amz-bucket-object-lock-enabled: Enabled`. AWS expects boolean `true`, so bucket creation fails.

**Evidence:** The literal is `"Enabled"`. `"Enabled"` belongs in the later Object Lock configuration XML, not the CreateBucket header. [CreateBucket API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)

**Suggested fix:** Send `x-amz-bucket-object-lock-enabled: true` and add a request-level test.

---

### 20. Backend-specific header handling loses or corrupts response metadata

**Location:** `s3/src/request/async_std_backend.rs:306-315`, `s3/src/utils/mod.rs:266-290`

**Failure scenario:**

- In async-std, a repeated response header causes subsequent header values to be associated with the wrong header names.
- In sync mode, `HeadObjectResult.content_type` is always missing.

**Evidence:** The async-std code zips an iterator of distinct header names with a flattened iterator of all values; cardinalities differ when a field repeats. The sync metadata parser looks up `"get-Type"` rather than `"Content-Type"`. A local sync fixture with `Content-Type` returned `None`.

**Suggested fix:** Iterate response header entries as name/value pairs without zipping independent iterators. Correct the sync lookup and preferably centralize response-header conversion across backends.

---

### 21. Sync streamed uploads mishandle small bodies and exact part boundaries

**Location:** `s3/src/bucket.rs:2351-2415`, `s3/src/utils/mod.rs:55-75`

**Failure scenario:**

- A small streamed upload initiates and aborts multipart upload, then falls back to `put_object` while discarding the requested content type.
- A body exactly divisible by 8 MiB can create or hash an extra empty final part.
- The public ETag helper therefore returns a value inconsistent with the upload strategy.

**Evidence:** A local sync upload requested `text/plain`, but the fallback PUT used `application/octet-stream`. For an exact 8 MiB input, the ETag helper reported a two-part ETag because it counted an empty chunk.

**Suggested fix:** Pre-read before initiating multipart upload, as the async path does, and preserve the caller’s content type and other headers. Stop before uploading or hashing an empty chunk. Add tests for `0`, `1`, `part_size-1`, `part_size`, and `2*part_size`.

---

### 22. Runtime features are not additive and fail under normal Cargo feature unification

**Location:** `s3/Cargo.toml:84-121` and backend-gated imports throughout `s3/src/`

**Failure scenario:** One dependency enables the default Tokio backend while another enables sync or async-std. Cargo unifies features, and the application fails to compile due to duplicate types and conflicting implementations. The advertised `blocking` feature alone is also insufficient.

**Evidence:**

- `cargo check --features sync` failed with duplicate `RwLock`/`RequestImpl` definitions and conflicting implementations.
- Combining Tokio and async-std without defaults failed similarly.
- `--no-default-features --features blocking` failed due to missing `futures_util`, lock aliases, and backend types.

**Suggested fix:** Move backend implementation behind one internal facade or separate backend crates. Until then, add explicit `compile_error!` diagnostics for unsupported combinations, document a precise feature matrix, and test both valid isolated configurations and forbidden combinations in CI.

`AUDIT.md` acknowledged feature mutual exclusivity as a test gap; it remains a real downstream integration hazard.

---

### 23. Credentials and request time are neither atomic nor refreshed per retry

**Location:** `s3/src/request/request_trait.rs:495-505,937-954,1019-1028,1117-1119`, `s3/src/bucket.rs:395-410,3635-3669`, `aws-creds/src/credentials.rs:477-486`, `s3/src/request/mod.rs:130-177`

**Failure scenario:**

- Concurrent refresh can mix an access key from one generation with a secret or token from another, producing intermittent invalid signatures.
- Credentials valid at request creation but expiring during transfer are used without a safety margin.
- Retries hours later retain the original signing time and may be rejected for clock skew.
- Large retry counts create effectively unbounded overall operation times.

**Evidence:** Credential fields are fetched independently. Refresh occurs only after actual expiry. The request datetime is created once outside the retry loop. Quadratic backoff with 30 attempts exceeds 2.6 hours; higher accepted counts can take days, while individual-request timeouts do not impose a total deadline.

**Suggested fix:** Snapshot credentials atomically for each attempt, refresh with a proactive margin, and generate a new signing timestamp and signature per attempt. Cap retry count/backoff, add jitter and `Retry-After` support, and enforce an overall deadline.

`AUDIT.md` already records the independent credential reads and proactive-expiry issue; both remain open.

---

### 24. Multipart abort is not retried and low-level upload failures can be masked

**Location:** `s3/src/command.rs:197-224`, `s3/src/bucket.rs:2087-2098,2511-2521,2547-2556`

**Failure scenario:** A transient 503 during abort leaves uploaded parts orphaned and billable. In low-level multipart methods, a failed abort can replace the original upload error, hiding the cause callers need to diagnose.

**Evidence:** `AbortMultipartUpload` is omitted from the retry-safe command set. Some cleanup paths explicitly ignore the abort result; others return the abort error instead of the original part failure.

**Suggested fix:** Treat abort as retry-safe, accept `NoSuchUpload` as successful cleanup, and preserve the original upload error while attaching abort failure as secondary context. AWS notes that multipart cleanup can require verification when concurrent parts are still in progress. [AbortMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)

The earlier audit repaired the principal streaming cleanup path, but these retry and low-level error-preservation gaps remain.

---

### 25. Error response bodies are buffered without a size bound

**Location:** `s3/src/request/tokio_backend.rs:157-184`, `s3/src/request/async_std_backend.rs:210-216,287-298`, `s3/src/request/blocking.rs:51-56,114-118`

**Failure scenario:** A malicious or faulty endpoint sends a very large or endless non-2xx body. The client buffers it all before returning the HTTP error, causing excessive memory use or a prolonged hang. Tokio’s concatenate step also temporarily duplicates the buffered data.

**Evidence:** All three backends convert the full error body into bytes or a string with no cap. If request timeouts are disabled, an endpoint can keep streaming indefinitely.

**Suggested fix:** Retain only a bounded diagnostic prefix, such as 64 KiB or 1 MiB, record whether it was truncated, and drop or boundedly drain the remaining body.

---

### 26. Streaming multipart uploads cannot approach S3’s supported object limit

**Location:** `s3/src/bucket.rs:335,2209-2279`, `s3/src/utils/mod.rs:25-44`, `s3/src/put_object_request.rs:314-325`

**Failure scenario:** The fixed 8 MiB part size reaches S3’s 10,000-part limit at about 78.125 GiB. Larger streaming uploads fail only after tens of gigabytes have already transferred and then require cleanup, despite S3 supporting objects up to 5 TiB. On 32-bit systems, the `usize` uploaded-byte count can wrap or panic above 4 GiB.

An arbitrarily large concurrency setting can also authorize extreme buffering.

**Evidence:** The loop has no preflight part-count check or adaptive part sizing. Totals use `usize`, and concurrency is not given a practical upper bound. S3 limits multipart uploads to 10,000 parts and permits object sizes up to 5 TiB. [S3 multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html)

**Suggested fix:** Use `u64` for byte totals, expose/configure part size, select an adaptive size when the total length is known, and fail before network I/O if the operation would exceed 10,000 parts. Bound concurrency to a documented safe range.

---

### 27. AWS GovCloud region strings silently become invalid custom endpoints

**Location:** `aws-region/src/region.rs:35-165,242-306`

**Failure scenario:** Parsing `us-gov-west-1` succeeds as a custom region whose endpoint is literally `us-gov-west-1`, rather than `s3.us-gov-west-1.amazonaws.com`. Requests fail DNS resolution or target an unintended host. Typographical errors are treated the same way.

**Evidence:** There are no GovCloud variants. `FromStr` maps every unrecognized string to `Region::Custom` using that same string as the endpoint. AWS publishes distinct GovCloud S3 endpoints. [AWS S3 service endpoints](https://docs.aws.amazon.com/general/latest/gr/s3.html)

**Suggested fix:** Add explicit `us-gov-east-1` and `us-gov-west-1` variants and endpoints. Longer term, make unknown `FromStr` values errors and require an explicit constructor for custom endpoints; that part may need a compatibility transition.

---

### 28. Public low-level `Command::UploadPart` is not actually an UploadPart request

**Location:** `s3/src/command.rs:142-146,421`, `s3/src/request/request_trait.rs:508-515,837`

**Failure scenario:** A caller constructs the public `Command::UploadPart` and executes it through a public request implementation. The URL lacks `partNumber` and `uploadId`, while signing uses an empty body hash despite sending data. AWS rejects the signature; an anonymous S3-compatible endpoint could instead treat it as an ordinary object PUT.

**Evidence:** The command carries a body, upload ID, and part number, but general URL construction never appends the multipart query parameters, and the command’s SHA-256 path selects the empty-body hash.

**Suggested fix:** Remove or make this variant private, deprecate the public low-level path, or route it through the same correctly implemented multipart request builder used by the high-level APIs.

This is the still-open public low-level UploadPart finding in `AUDIT.md`.

## P3 — Low

### 29. Several `Result`-returning APIs can panic on caller-controlled input

**Location:** `s3/src/command.rs:188-195`, `s3/src/bucket.rs:1635-1644,1864-1872,3695-3697`, `s3/src/request/request_trait.rs:495-502,945-949`

**Failure scenario:** A service passes an invalid range, malformed custom header, or partial credentials originating from configuration. Rather than return an error, the library asserts, unwraps, or calls `expect`, potentially crashing the process.

**Evidence:** Range construction asserts ordering, `add_header` unwraps header parsing, and signing expects credential components that public credential construction does not require to be complete.

**Suggested fix:** Validate at public API boundaries and return typed errors. Add a fallible `try_add_header` API, require complete access-key pairs, and replace signing `expect`s with propagated credential errors. Presign expiry should also reject zero before sending a predictably invalid request.

## Simplification opportunities

- Consolidate URL/path/query construction into one backend-independent builder. This would address most dot-segment, opaque-ID, presigning, and subresource bugs at one boundary.
- Represent credentials as an immutable, internally complete snapshot instead of independently locked optional fields.
- Keep one buffered-chunk field in `ResponseDataStream`; remove dynamic stream reconstruction.
- Replace hand-built XML for tagging and bulk deletion with typed serializers and strict typed deserializers.
- Centralize response status, header, and bounded-error-body handling so the three backends cannot drift.
- Use one multipart state machine for sync and async paths, parameterized by transport, instead of duplicating boundary and cleanup logic.
- Separate runtime backends into distinct crates or mutually exclusive adapter modules. The current feature graph forces pervasive conditional compilation and makes Cargo feature unification unsafe.
- Remove or privatize the partially implemented low-level command variants rather than maintaining two request-construction APIs.

## Checks that did not reveal an additional issue

- `cargo test --locked --workspace` passed with the required OpenSSL pkg-config path.
- Isolated compile/test configurations passed for:
  - Tokio native-tls.
  - Tokio rustls, including ring.
  - async-std native H1.
  - async-std rustls H1.
  - async-std Hyper.
  - sync native-tls.
  - sync rustls.
  - `aws-creds` without defaults and with the split HTTP credential features.
- The async-std H1 checks required the documented `RUSTFLAGS='--cfg rustix_use_libc'`.
- I did not run ignored tests requiring real credentials or providers.
- Embedded-error handling for successful HTTP responses to multipart completion and copy operations remains present.
- Ordinary mutation requests are not broadly retried; the retry allowlist is conservative aside from the missing multipart-abort classification.
- The repaired principal streaming multipart paths attempt cleanup after failure.
- Multipart-upload listing pagination includes the expected cursor fields.
- The container-credential HTTP path validates endpoints, disables redirects and HTTP proxying where appropriate, and marks its authorization header sensitive.
- Credential objects’ own manual `Debug` implementation redacts secrets; finding 6 concerns outer request/bucket types that still expose other sensitive data.
- Tokio request bodies are collected exactly, avoiding silent truncation from length mismatches.
- The new response-header access on `ResponseDataStream` preserves the original header map in Tokio. The severe streaming issue is instead its partial-read buffering design.
- The request-observer lifecycle did not show an additional correctness defect: terminal notification paths are guarded against repeated completion.
- Canonical query parameters are sorted after encoding.
- Default per-request timeouts exist; finding 23 concerns total retry duration and stale signing state, not their complete absence.
- Surf did not appear to follow redirects without explicit redirect middleware, so finding 4 is limited to Tokio and sync.

# rust-s3 full code reviews, consolidated: 2026-10-16

This index combines four independent full-repository reviews of `prime` at `b2077f3`:

| Tag | Review |
|-----|--------|
| **C** | [Claude Opus 5.5](full-review-claude-opus-5-5.md) |
| **A** | [gpt-6-astra](full-review-gpt-6-astra.md) (xhigh) |
| **S6** | [gpt-6.1-sol](full-review-gpt-6.1-sol.md) (xhigh) |
| **S5** | [gpt-5.6-sol](full-review-gpt-5.6-sol.md) (xhigh) |

Duplicate findings are merged. Severity is my call where the reviewers disagree. "Repro"
means at least one reviewer reproduced the finding against a local server or fixture. "Read"
means it rests on code reading only. "AUDIT" means `AUDIT.md` already lists it as open or as a
coverage gap. Line numbers are in the individual reviews.

## P1

| Finding | Found by | Basis |
|---------|----------|-------|
| `.`/`..` key segments are normalized by `Url::parse`; the signed request hits another key, or in path style another bucket | C A S6 S5 | repro (AUDIT gap) |
| `AsyncRead for ResponseDataStream` nests one `once().chain()` per partial read: quadratic, then a stack-overflow **abort** (C: 512 MiB through `tokio::io::copy` in release) | C A S6 S5 | repro |
| sync and async-std H1 return a body cut short by the peer as a complete `Ok` download (buffered, writer, stream); tokio errors correctly | C A | repro |
| `x-amz-copy-source` not URL-encoded: wrong source object (`a%2Fb` → `a/b`), `?` → versionId, non-ASCII fails | C A S6 S5 | repro |
| Tokio and sync follow redirects: a 307 re-sends the body, `x-amz-security-token` and SSE-C headers (sync also `Authorization`) to another origin, which then reports success | A S6 S5 | repro |
| reqwest/attohttpc auto-decompression is switched on by any other crate enabling `reqwest/gzip` or `attohttpc/compress`; stored gzip objects come back decoded | A S6 | repro |
| ListObjects v1 `list()` stops after the first truncated page without `NextMarker`; v2 has no repeated-token guard | C A S6 S5 | repro |
| `DeleteObjects` XML keeps a literal CR, which XML parsing turns into LF, so `a\rb` can delete `a\nb` | A S6 S5 | repro |
| IMDS v1/v2 requests honour `HTTP_PROXY` and redirects, so a proxy sees the IMDS token and can supply credentials (the container path already disables both) | A S6 S5 | repro |
| `tokio-rustls-tls` + `http-credentials-rustls-tls` compiles in both ring and aws-lc; attohttpc's `ClientConfig::builder()` then panics on STS HTTPS ("Could not automatically determine the process-level CryptoProvider") | A S6 | repro; C confirmed by reading attohttpc |
| `tokio-rustls-tls-ring` / `aws-creds/rustls-tls-ring`: attohttpc 0.30.1's `tls-rustls-webpki-roots-ring` never enables `__rustls`, so the no-TLS implementation is selected and STS HTTPS returns `TlsDisabled` | A | repro; C confirmed by reading attohttpc |
| async-std transport graphs contain RUSTSEC advisories (webpki 0.21, rustls 0.18, hyper 0.13, h2 0.2) | A S6 S5 | AUDIT (open) |

## P2

| Finding | Found by | Basis |
|---------|----------|-------|
| Multipart scheduler doesn't poll part futures while reading the next chunk: finished parts hit reqwest's deadline and the upload aborts (C), and a stalled source hides an already-failed part (A) | C A | repro |
| `presign_post` uses only `security_token`, so session-token credentials produce an unusable form | C A S6 S5 | repro (AUDIT gap) |
| POST checksum policy requires `x-amz-checksum-algorithm`, which the returned form omits | A S6 S5 | repro |
| Presign with `extra_query` yields `?versionId=v1?X-Amz-Algorithm=…` | A S6 S5 | repro |
| Presign of an endpoint with a path and explicit `:80`/`:443` duplicates the path | A S6 S5 | repro |
| sync `presign_put` drops `custom_queries` from the URL but signs them | C A S6 S5 | repro |
| `location()` requests the object `?location` | C A S6 S5 | repro |
| `exists()` on a subdomain-style bucket builds `https://.s3.amazonaws.com/` | C A S6 S5 | repro |
| `object_exists()` without `fail-on-err` returns `true` for 403/500/503 | A S6 S5 | repro |
| `get_object_attributes`: an ETag-only request, but every field is required by the deserializer (`missing field Checksum`); `None` sends `versionId=null` | A S6 S5 | repro |
| `uploadId`/`versionId` interpolated raw: `+`, `&`, `#` change meaning | C A S6 S5 | repro (AUDIT gap) |
| SigV4 canonical headers neither collapse inner spaces nor join repeated values | C A S6 S5 | repro (AUDIT gap) |
| Signing reads access key, secret and token under separate locks, so a refresh can mix generations | C A S6 S5 | AUDIT (open) |
| Refresh only after expiry (no margin); retries reuse the first attempt's signing time | C S5 | AUDIT (open) |
| A cancelled refresh caller drops the single-flight gate while its blocking fetch continues | A | AUDIT (open) |
| STS `WebIdentityToken` travels in a GET query, which attohttpc logs at debug level | A S6 | read |
| IMDSv2 token containing `\n` panics in attohttpc `.header()` | S6 | repro |
| Default credential chain treats "configured but broken" like "absent" and falls through to another identity | S5 | read |
| Error bodies are buffered without a size cap, even for streaming APIs | A S6 S5 | read |
| `*_blocking` stream methods return a stream bound to a runtime that has already been dropped, which panics on read; each call also builds a new multi-thread runtime | A C | repro (A) |
| Bare `with-async-std` (no transport) builds, then panics on the first request | S6 | repro |
| Runtime features aren't additive; unification of e.g. `sync` + tokio fails to compile | S5 | repro (AUDIT gap) |
| sync timeout doesn't bound TCP connect (a 20 ms timeout still blocked past 300 ms) | S6 | repro |
| Default 60 s `request_timeout` is a total deadline including the body, so large transfers fail midway | C | read (A, S6 list body-inclusive deadlines as intended) |
| Tag XML is not escaped (`&`, and injection of extra tags), and malformed tag responses become `Ok(([], 200))` | C A S6 S5 | repro |
| `x-amz-bucket-object-lock-enabled: Enabled` should be `true` | A S6 S5 | read |
| `AfSouth1` endpoint `s3-af-south-1.amazonaws.com` doesn't resolve; the dotted form does | A | DNS check |
| Unknown region strings (GovCloud, typos) silently become endpoint hostnames | C S5 | read |
| Fixed 8 MiB parts cap streamed uploads at ~78 GiB, discovered only after uploading that much | C S6 S5 | read |
| `Command::UploadPart` has no multipart query and signs the empty hash; with empty content it's a plain overwrite | C A S6 S5 | AUDIT (open) |
| Range constructor `assert!`, signing `expect`s (anonymous presign panics), `add_header` `unwrap` | C S5 | repro (C) |
| sync `head_object().content_type` is always `None` (`"get-Type"`) | C A S6 S5 | repro |
| sync small streamed upload: initiate plus abort, then the PUT loses the content type | C A S6 S5 | repro |
| `etag_for_path` counts an empty part at exact multiples of 8 MiB | C A S6 S5 | repro |
| sync uploads an empty final part at exact multiples of 8 MiB | C S6 S5 | read |
| Exported `retry!` resolves `log` and `cfg(feature)` in the *caller's* crate | A | repro |
| Abort isn't retried; low-level `put_multipart_chunk` returns the abort error instead of the part error | S5 | read |
| `Debug` on `Bucket`/`Command`/`PutObjectRequest` prints payloads and SSE-C keys | C S5 | read |

## P3

| Finding | Found by | Basis |
|---------|----------|-------|
| Mid-body failures on buffered GET/list are not retried | C | read |
| async-std `response_data` headers come from `header_names().zip(header_values())`, which misaligns on repeated headers | C S5 | read |
| async-std H1 returns an empty body for a valid close-delimited response | A | repro |
| async-std: with `ReadExt` in scope, `stream.bytes()` resolves to the by-value byte iterator | C | compile error |
| Duplicate request work: SHA-256 of the body twice per attempt plus MD5, body copied per attempt; sync has no keep-alive | C | read |
| `RequestObserver::on_request` exposes `authorization`/`x-amz-security-token`; the docs should warn | C | read |

## Disagreements and notes

- **60 s total deadline**: only C treats it as a production problem; A and S6 list body-inclusive
  deadlines among things that hold up. Both readings are in the code: it is intentional per
  AUDIT, and it fails large downloads on the default configuration.
- **Severity of copy-source encoding**: C rated it P2 (fails or misdirects on special keys).
  A, S6 and S5 rated it P1 because the wrong object can be copied successfully. P1 is right.
- **Feature-split commits on this fork** (`50a4186`, `e2b5773`): the two rustls credential
  findings above are the only ones that land squarely on fork-specific code. The others are
  upstream behaviour or older.

## Simplifications that all four reviews converge on

- One URL/query builder for requests and presigning (dot segments, IDs, `extra_query`, ports,
  `location`).
- One credential snapshot per signing attempt, with one token-selection helper.
- A pending-`Bytes` field in `ResponseDataStream` instead of re-wrapping the stream.
- Typed quick-xml serialization for tagging and DeleteObjects; strict deserialization.
- `self.clone()` plus field updates for the `Bucket` builder methods; one
  `From<&http::HeaderMap> for HeadObjectResult`; one async/sync presign body.
- Remove `Command::UploadPart`; consider retiring the exported `retry!` macro.

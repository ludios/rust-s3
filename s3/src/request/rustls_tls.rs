// Model-output: Claude Opus 5.5

//! The rustls configuration of the tokio backend's client: the program's process-wide rustls
//! provider if it installed one and ring otherwise, the Mozilla root certificates bundled by
//! `webpki-roots`, and the certificate checks the `Bucket`'s options leave on.
//!
//! The client gets a finished configuration, so no process-wide provider is needed or installed,
//! and no system certificate store is read.

use std::sync::{Arc, LazyLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{WebPkiServerVerifier, verify_server_cert_signed_by_trust_anchor};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

static RING: LazyLock<Arc<CryptoProvider>> =
    LazyLock::new(|| Arc::new(rustls::crypto::ring::default_provider()));

static ROOTS: LazyLock<Arc<RootCertStore>> = LazyLock::new(|| {
    Arc::new(RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
});

/// Every scheme a server may sign its handshake with, all of which `accept_invalid_certs`
/// accepts unchecked.
const ALL_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PKCS1_SHA1,
    SignatureScheme::ECDSA_SHA1_Legacy,
    SignatureScheme::RSA_PKCS1_SHA256,
    SignatureScheme::ECDSA_NISTP256_SHA256,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::ECDSA_NISTP384_SHA384,
    SignatureScheme::RSA_PKCS1_SHA512,
    SignatureScheme::ECDSA_NISTP521_SHA512,
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::ED25519,
    SignatureScheme::ED448,
];

/// Checks a server's certificate chain, validity and name as `WebPkiServerVerifier` does, minus
/// what the options accept anyway.
#[derive(Debug)]
struct Verifier {
    provider: Arc<CryptoProvider>,
    /// The certificates a server's chain has to end in.
    roots: Arc<RootCertStore>,
    /// The full check against `roots`.
    webpki: Arc<WebPkiServerVerifier>,
    /// Accept any certificate and any handshake signature.
    accept_invalid_certs: bool,
    /// Check the chain and validity but not the name.
    accept_invalid_hostnames: bool,
}

impl Verifier {
    fn new(
        provider: Arc<CryptoProvider>,
        roots: Arc<RootCertStore>,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Verifier {
        let webpki = WebPkiServerVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .expect("the root store is not empty");
        Verifier {
            provider,
            roots,
            webpki,
            accept_invalid_certs,
            accept_invalid_hostnames,
        }
    }
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if self.accept_invalid_certs {
            return Ok(ServerCertVerified::assertion());
        }
        if self.accept_invalid_hostnames {
            verify_server_cert_signed_by_trust_anchor(
                &ParsedCertificate::try_from(end_entity)?,
                &self.roots,
                intermediates,
                now,
                self.provider.signature_verification_algorithms.all,
            )?;
            return Ok(ServerCertVerified::assertion());
        }
        self.webpki
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        if self.accept_invalid_certs {
            return Ok(HandshakeSignatureValid::assertion());
        }
        self.webpki.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        if self.accept_invalid_certs {
            return Ok(HandshakeSignatureValid::assertion());
        }
        self.webpki.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        if self.accept_invalid_certs {
            return ALL_SCHEMES.to_vec();
        }
        self.webpki.supported_verify_schemes()
    }
}

/// Returns the TLS configuration for one client. Cheap: ring and the roots are made once.
///
/// `accept_invalid_certs`: accept any server certificate.
/// `accept_invalid_hostnames`: accept a certificate that passes every check except the one that
/// it names the server.
pub(crate) fn client_config(
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
) -> ClientConfig {
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| RING.clone());
    let verifier = Verifier::new(
        provider.clone(),
        ROOTS.clone(),
        accept_invalid_certs,
        accept_invalid_hostnames,
    );
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the provider supports TLS 1.2 or 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    // HTTP/1.1, even when another crate in the build enables reqwest's HTTP/2.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::CertificateError;
    use rustls::pki_types::pem::PemObject;
    use std::time::Duration;

    /// A test CA, valid from 2026-10-17 to 2126.
    const CA: &str = "-----BEGIN CERTIFICATE-----
MIIBmjCCAUGgAwIBAgIUKaugxcARFNsOsbv+hVnnzf0bBZowCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPcnVzdC1zMyB0ZXN0IENBMCAXDTI2MTAxNzAwMDQxOFoYDzIx
MjYwOTIzMDAwNDE4WjAaMRgwFgYDVQQDDA9ydXN0LXMzIHRlc3QgQ0EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAAQlE3e9lk1LVpniWwF2vDd+5qPn7DYw+jnVG/ab
XNU0XrhTBjZKqKnyUnt7WDmwsO8mL02TYuWT8CVfW58oagXzo2MwYTAdBgNVHQ4E
FgQUIdShzSKRkuI4X/eNau3nTIjkV5UwHwYDVR0jBBgwFoAUIdShzSKRkuI4X/eN
au3nTIjkV5UwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAQYwCgYIKoZI
zj0EAwIDRwAwRAIgD3V8q+PiBUd1xGGF7hWWzGHJzZBCwHZSGXmjrW9cV/MCICv1
Vzwem+EgfFTjdfjnR5LquDXQcqaRwzzezlW9gVcX
-----END CERTIFICATE-----";

    /// A server certificate for `s3.test` that `CA` signed, valid from 2026-10-17 to 2126.
    const LEAF: &str = "-----BEGIN CERTIFICATE-----
MIIBvDCCAWGgAwIBAgIUHWpLVRkOu8Es0z2XWEX5WeZhFc8wCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPcnVzdC1zMyB0ZXN0IENBMCAXDTI2MTAxNzAwMDQxOFoYDzIx
MjYwOTIzMDAwNDE4WjASMRAwDgYDVQQDDAdzMy50ZXN0MFkwEwYHKoZIzj0CAQYI
KoZIzj0DAQcDQgAEZW3e8IZJuiwsYy0N6ntamCaTG9sXBHs9as6HdXkeglOwKKbQ
TieJblyiGlsvMqZQTUBF1wXah0J6MzikGEQHNaOBijCBhzAMBgNVHRMBAf8EAjAA
MA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDATASBgNVHREECzAJ
ggdzMy50ZXN0MB0GA1UdDgQWBBQ+5FiwBEdP8vbF8IyWY3cMm7MvIjAfBgNVHSME
GDAWgBQh1KHNIpGS4jhf941q7edMiORXlTAKBggqhkjOPQQDAgNJADBGAiEA38bG
AM8jjWLAKulBhfCu9aonWiPDrNbU2HV9pWw+7Q0CIQDeeYpyD6zSCa/oc8NWeK0W
N+O2+3hKDpIpvsKrrZHqBg==
-----END CERTIFICATE-----";

    fn verifier(
        trust_ca: bool,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Verifier {
        let roots = if trust_ca {
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(CA.as_bytes()).unwrap())
                .unwrap();
            Arc::new(roots)
        } else {
            ROOTS.clone()
        };
        Verifier::new(
            RING.clone(),
            roots,
            accept_invalid_certs,
            accept_invalid_hostnames,
        )
    }

    /// Verifies `LEAF` for the server `name` on 2030-01-01, with `CA` as the only root when
    /// `trust_ca`, or else with the bundled roots, which do not include it.
    fn verify(
        trust_ca: bool,
        name: &str,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Result<ServerCertVerified, Error> {
        let leaf = CertificateDer::from_pem_slice(LEAF.as_bytes()).unwrap();
        let name = ServerName::try_from(name.to_string()).unwrap();
        let now = UnixTime::since_unix_epoch(Duration::from_secs(1_893_456_000));
        verifier(trust_ca, accept_invalid_certs, accept_invalid_hostnames).verify_server_cert(
            &leaf,
            &[],
            &name,
            &[],
            now,
        )
    }

    fn is_name_error(result: Result<ServerCertVerified, Error>) -> bool {
        matches!(
            result,
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            ))
        )
    }

    fn is_unknown_issuer(result: Result<ServerCertVerified, Error>) -> bool {
        matches!(
            result,
            Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
        )
    }

    #[test]
    fn checks_chain_and_name_by_default() {
        assert!(verify(true, "s3.test", false, false).is_ok());
        assert!(is_name_error(verify(true, "other.test", false, false)));
        assert!(is_unknown_issuer(verify(false, "s3.test", false, false)));
    }

    #[test]
    fn accept_invalid_hostnames_still_checks_the_chain() {
        assert!(verify(true, "other.test", false, true).is_ok());
        assert!(is_unknown_issuer(verify(false, "s3.test", false, true)));
        assert!(is_unknown_issuer(verify(false, "other.test", false, true)));
    }

    #[test]
    fn accept_invalid_certs_accepts_any_certificate_and_signature_scheme() {
        assert!(verify(false, "other.test", true, false).is_ok());
        // ring cannot verify P-521 signatures, so only this mode can offer the scheme.
        let schemes = verifier(false, true, false).supported_verify_schemes();
        assert!(schemes.contains(&SignatureScheme::ECDSA_NISTP521_SHA512));
    }

    #[test]
    fn bundled_roots_are_loaded() {
        assert!(ROOTS.len() > 100, "{}", ROOTS.len());
    }
}

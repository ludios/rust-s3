// Model-output: Claude Opus 5.5

//! The rustls configuration of the tokio backend's client: ring, the Mozilla root certificates
//! bundled by `webpki-roots`, and the certificate checks the `Bucket`'s options leave on.
//!
//! The client gets a finished configuration, so its crypto provider and roots are its own: no
//! process-wide rustls provider is needed or installed, and no system certificate store is read.

use std::sync::{Arc, LazyLock};

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
};

static PROVIDER: LazyLock<Arc<CryptoProvider>> =
    LazyLock::new(|| Arc::new(rustls::crypto::ring::default_provider()));

static ROOTS: LazyLock<Arc<RootCertStore>> = LazyLock::new(|| {
    Arc::new(RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
});

/// Returns the TLS configuration for one client. Cheap: the provider and roots are shared.
///
/// `accept_invalid_certs`: accept any server certificate.
/// `accept_invalid_hostnames`: accept a certificate that passes every check except the one that
/// it names the server.
pub(crate) fn client_config(
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
) -> ClientConfig {
    let verifier = Verifier::new(
        ROOTS.clone(),
        accept_invalid_certs,
        accept_invalid_hostnames,
    );
    let mut config = ClientConfig::builder_with_provider(PROVIDER.clone())
        .with_safe_default_protocol_versions()
        .expect("ring supports TLS 1.2 and 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    // The client speaks only HTTP/1.1.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

/// Checks a server's certificate chain, validity and name as `WebPkiServerVerifier` does, except
/// for what the options accept anyway. Handshake signatures are always checked against the
/// certificate's key.
#[derive(Debug)]
struct Verifier {
    webpki: Arc<WebPkiServerVerifier>,
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
}

impl Verifier {
    /// `roots`: the certificates a server's chain has to end in.
    fn new(
        roots: Arc<RootCertStore>,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Verifier {
        let webpki = WebPkiServerVerifier::builder_with_provider(roots, PROVIDER.clone())
            .build()
            .expect("the root store is not empty");
        Verifier {
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
        let result = self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        );
        // `WebPkiServerVerifier` checks the name last, so a name error means every other
        // check passed.
        match result {
            Err(_) if self.accept_invalid_certs => Ok(ServerCertVerified::assertion()),
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) if self.accept_invalid_hostnames => Ok(ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.webpki.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.webpki.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::pem::PemObject;

    /// A test CA, valid until 2126.
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

    /// A server certificate for `s3.test` that `CA` signed, valid until 2126.
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

    /// Verifies `LEAF` for the server `name`, with `CA` as the only root when `trust_ca`, or
    /// else with the bundled roots, which do not include it.
    fn verify(
        trust_ca: bool,
        name: &str,
        accept_invalid_certs: bool,
        accept_invalid_hostnames: bool,
    ) -> Result<ServerCertVerified, Error> {
        let roots = if trust_ca {
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(CA.as_bytes()).unwrap())
                .unwrap();
            Arc::new(roots)
        } else {
            ROOTS.clone()
        };
        let verifier = Verifier::new(roots, accept_invalid_certs, accept_invalid_hostnames);
        let leaf = CertificateDer::from_pem_slice(LEAF.as_bytes()).unwrap();
        let name = ServerName::try_from(name.to_string()).unwrap();
        verifier.verify_server_cert(&leaf, &[], &name, &[], UnixTime::now())
    }

    fn is_name_error(result: Result<ServerCertVerified, Error>) -> bool {
        matches!(
            result,
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            ))
        )
    }

    #[test]
    fn checks_chain_and_name_by_default() {
        assert!(verify(true, "s3.test", false, false).is_ok());
        assert!(is_name_error(verify(true, "other.test", false, false)));
        assert!(matches!(
            verify(false, "s3.test", false, false),
            Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
        ));
    }

    #[test]
    fn accept_invalid_hostnames_still_checks_the_chain() {
        assert!(verify(true, "other.test", false, true).is_ok());
        assert!(matches!(
            verify(false, "s3.test", false, true),
            Err(Error::InvalidCertificate(CertificateError::UnknownIssuer))
        ));
    }

    #[test]
    fn accept_invalid_certs_accepts_any_certificate() {
        assert!(verify(false, "other.test", true, false).is_ok());
    }

    #[test]
    fn bundled_roots_are_loaded() {
        assert!(ROOTS.len() > 100, "{}", ROOTS.len());
    }
}

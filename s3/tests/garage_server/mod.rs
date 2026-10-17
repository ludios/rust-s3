// Model-output: Claude Opus 5.5

//! An ephemeral garage server: a real S3-compatible service on localhost, for end-to-end tests
//! of `Bucket`. Each test starts its own, so tests run in parallel and share nothing; the server
//! is killed and its data removed on drop. `garage` must be on PATH.

#![allow(dead_code)] // Each test binary uses its own subset.

use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// The region garage signs for, configured as its `s3_region`
const REGION: &str = "garage";

/// A running `garage server` with a single-node layout applied, so that its S3 API serves
/// requests.
pub struct GarageServer {
    /// Holds meta/, data/, config.toml and server.log; removed on drop
    dir: PathBuf,
    /// The `garage server` process, killed on drop
    child: Child,
    /// Port of the S3 API on 127.0.0.1
    s3_port: u16,
}

impl GarageServer {
    /// Start a garage server on free localhost ports and wait until it accepts admin commands.
    /// Panics, with the server log when there is one, if `garage` is missing or does not
    /// become ready within 30 s.
    pub fn start() -> GarageServer {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("garage-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let [rpc_port, s3_port] = free_ports();
        let config = format!(
            r#"
metadata_dir = "{dir}/meta"
data_dir = "{dir}/data"
db_engine = "sqlite"
replication_factor = 1

rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "4425f45c38f8e8d38cf5100fca6d84d19f0954d76b22ca1a4dd05a945d9d4b7e"

[s3_api]
s3_region = "{REGION}"
api_bind_addr = "127.0.0.1:{s3_port}"
"#,
            dir = dir.display(),
        );
        std::fs::write(dir.join("config.toml"), config).unwrap();
        let log = std::fs::File::create(dir.join("server.log")).unwrap();
        let child = Command::new("garage")
            .arg("-c")
            .arg(dir.join("config.toml"))
            .arg("server")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap_or_else(|error| {
                panic!(
                    "could not run `garage server` ({error}); these tests need garage on PATH, \
                     see https://garagehq.deuxfleurs.fr/download/"
                )
            });
        let mut garage = GarageServer {
            dir,
            child,
            s3_port,
        };

        let deadline = Instant::now() + Duration::from_secs(30);
        while !garage.garage(&["status"]).status.success() {
            let exited = garage.child.try_wait().unwrap();
            if exited.is_some() || Instant::now() > deadline {
                panic!(
                    "garage server did not become ready ({exited:?}); log:\n{}",
                    std::fs::read_to_string(garage.dir.join("server.log")).unwrap_or_default()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let node_id_at_addr = garage.garage_ok(&["node", "id", "-q"]);
        let node_id = node_id_at_addr.split('@').next().unwrap().trim();
        garage.garage_ok(&["layout", "assign", "-z", "z1", "-c", "100M", node_id]);
        garage.garage_ok(&["layout", "apply", "--version", "1"]);
        garage
    }

    /// Run the garage CLI against this server.
    fn garage(&self, args: &[&str]) -> Output {
        Command::new("garage")
            .arg("-c")
            .arg(self.dir.join("config.toml"))
            .args(args)
            .output()
            .expect("failed to run garage")
    }

    /// Run the garage CLI against this server and return its stdout. Panics with its output if
    /// it exits nonzero.
    pub fn garage_ok(&self, args: &[&str]) -> String {
        let output = self.garage(args);
        assert!(
            output.status.success(),
            "garage {args:?} exited with {:?}\nstdout: {}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8(output.stdout).unwrap()
    }

    /// Create the bucket `name` with a key allowed to read, write and own it, and return a
    /// path-style `Bucket` for it signed with that key.
    pub fn bucket(&self, name: &str) -> Box<Bucket> {
        /// The trimmed rest of the first line of `text` that starts with `label`
        fn field_after<'a>(text: &'a str, label: &str) -> &'a str {
            text.lines()
                .find_map(|line| line.strip_prefix(label))
                .unwrap_or_else(|| panic!("no line starting with {label:?} in {text:?}"))
                .trim()
        }

        let key_name = format!("{name}-key");
        self.garage_ok(&["bucket", "create", name]);
        let key_info = self.garage_ok(&["key", "create", &key_name]);
        self.garage_ok(&[
            "bucket", "allow", "--read", "--write", "--owner", name, "--key", &key_name,
        ]);

        let credentials = Credentials::new(
            Some(field_after(&key_info, "Key ID:")),
            Some(field_after(&key_info, "Secret key:")),
            None,
            None,
            None,
        )
        .unwrap();
        let region = Region::Custom {
            region: REGION.to_owned(),
            endpoint: format!("http://127.0.0.1:{}", self.s3_port),
        };
        Bucket::new(name, region, credentials)
            .unwrap()
            .with_path_style()
    }
}

impl Drop for GarageServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `N` distinct localhost ports that were free a moment ago.
fn free_ports<const N: usize>() -> [u16; N] {
    let listeners: Vec<TcpListener> = (0..N)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    std::array::from_fn(|i| listeners[i].local_addr().unwrap().port())
}

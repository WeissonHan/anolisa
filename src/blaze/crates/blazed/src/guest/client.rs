// SPDX-License-Identifier: Apache-2.0
//! JSON-line readiness client layered over Firecracker's vsock proxy.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use blaze_core::guest_protocol::{
    DEFAULT_GUEST_PORT, DEFAULT_MAX_RESPONSE_BYTES, GuestOp, GuestRequest, GuestResponse,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{GuestError, Result};

const READY_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);

/// Client for checking one Firecracker guest agent.
#[derive(Debug, Clone)]
pub struct GuestClient {
    vsock_path: PathBuf,
    port: u32,
    io_timeout: Duration,
    max_response_bytes: usize,
}

impl GuestClient {
    /// Create a readiness client with production protocol defaults.
    pub fn new(vsock_path: PathBuf, io_timeout: Duration) -> Self {
        Self {
            vsock_path,
            port: DEFAULT_GUEST_PORT,
            io_timeout,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }

    /// Override the response limit for focused framing tests.
    #[cfg(test)]
    fn with_response_limit(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Check whether the guest agent is responsive.
    pub async fn ping(&self) -> Result<()> {
        let request = GuestRequest::new(Uuid::new_v4().to_string(), GuestOp::Ping);
        self.send_recv(&request).await?;
        Ok(())
    }

    /// Poll readiness with bounded exponential backoff.
    pub async fn wait_ready(
        &self,
        deadline: Duration,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let started = Instant::now();
        let mut backoff = Duration::from_millis(10);
        let mut last_error = None;
        while started.elapsed() < deadline {
            let remaining = deadline.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            let attempt_timeout = READY_ATTEMPT_TIMEOUT.min(remaining);
            tokio::select! {
                _ = cancellation.cancelled() => return Err(GuestError::Cancelled),
                result = tokio::time::timeout(attempt_timeout, self.ping()) => {
                    match result {
                        Ok(Ok(())) => return Ok(()),
                        Ok(Err(error)) => last_error = Some(error),
                        Err(_) => {
                            last_error = Some(GuestError::Timeout(format!(
                                "readiness ping exceeded {attempt_timeout:?}"
                            )));
                        }
                    }
                }
            }
            let remaining = deadline.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Err(GuestError::Cancelled),
                _ = tokio::time::sleep(backoff.min(remaining)) => {}
            }
            backoff = (backoff * 2).min(Duration::from_millis(250));
        }
        Err(GuestError::Timeout(format!(
            "guest at {} was not ready within {:?}: {}",
            self.vsock_path.display(),
            deadline,
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "no attempt completed".to_string())
        )))
    }

    async fn send_recv(&self, request: &GuestRequest) -> Result<GuestResponse> {
        tokio::time::timeout(self.io_timeout, self.send_recv_inner(request))
            .await
            .map_err(|_| {
                GuestError::Timeout(format!(
                    "{:?} request to {} exceeded {:?}",
                    request.op,
                    self.vsock_path.display(),
                    self.io_timeout
                ))
            })?
    }

    async fn send_recv_inner(&self, request: &GuestRequest) -> Result<GuestResponse> {
        let mut stream = UnixStream::connect(&self.vsock_path).await?;
        stream
            .write_all(format!("CONNECT {}\n", self.port).as_bytes())
            .await?;
        let handshake = read_line(&mut stream, 128).await?;
        let handshake = std::str::from_utf8(&handshake).map_err(|error| {
            GuestError::Protocol(format!("CONNECT response is not UTF-8: {error}"))
        })?;
        let peer_cid = handshake
            .strip_prefix("OK ")
            .and_then(|value| value.parse::<u32>().ok());
        if peer_cid.is_none() {
            return Err(GuestError::Protocol(format!(
                "unexpected CONNECT {} response: expected \"OK <numeric-peer-cid>\", received {handshake:?}",
                self.port,
            )));
        }

        let mut encoded = serde_json::to_vec(request)?;
        encoded.push(b'\n');
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        let line = read_line(&mut stream, self.max_response_bytes).await?;
        let response: GuestResponse = serde_json::from_slice(&line)?;
        if response.id != request.id {
            return Err(GuestError::Protocol(format!(
                "response id mismatch: sent {}, received {}",
                request.id, response.id
            )));
        }
        if !response.ok {
            return Err(GuestError::Rejected(response.err.unwrap_or_else(|| {
                "guest rejected request without an error".to_string()
            })));
        }
        Ok(response)
    }
}

async fn read_line<R>(stream: &mut R, limit: usize) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let bounded = limit.saturating_add(1);
    let mut reader = BufReader::new(stream).take(bounded as u64);
    let mut output = Vec::with_capacity(limit.min(8192));
    let count = reader.read_until(b'\n', &mut output).await?;
    if output.last() == Some(&b'\n') {
        output.pop();
        if output.len() <= limit {
            return Ok(output);
        }
    }
    if output.len() > limit {
        return Err(GuestError::PayloadTooLarge {
            actual: output.len(),
            limit,
        });
    }
    debug_assert_eq!(count, output.len());
    Err(GuestError::Protocol(
        "connection closed before newline delimiter".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use tokio::net::UnixListener;

    use super::*;

    async fn spawn_server(
        socket: PathBuf,
        response: Arc<dyn Fn(serde_json::Value) -> serde_json::Value + Send + Sync>,
    ) {
        let listener = UnixListener::bind(socket).expect("bind");
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let response = response.clone();
                tokio::spawn(async move {
                    let connect = read_line(&mut stream, 128).await.expect("connect");
                    assert_eq!(connect, b"CONNECT 5000");
                    stream.write_all(b"OK 1073742006\n").await.expect("ok");
                    let request = read_line(&mut stream, 4096).await.expect("request");
                    let request: serde_json::Value =
                        serde_json::from_slice(&request).expect("json");
                    let response = response(request);
                    let mut bytes = serde_json::to_vec(&response).expect("encode");
                    bytes.push(b'\n');
                    stream.write_all(&bytes).await.expect("write");
                });
            }
        });
    }

    async fn accept_request(listener: &UnixListener) -> (UnixStream, serde_json::Value) {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let connect = read_line(&mut stream, 128).await.expect("connect");
        assert_eq!(connect, b"CONNECT 5000");
        stream.write_all(b"OK 1073742006\n").await.expect("ok");
        let request = read_line(&mut stream, 4096).await.expect("request");
        let request = serde_json::from_slice(&request).expect("json");
        (stream, request)
    }

    #[tokio::test]
    async fn ping_uses_the_readiness_contract() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        spawn_server(
            socket.clone(),
            Arc::new(|request| {
                assert_eq!(request["op"], "ping");
                json!({"id": request["id"], "ok": true})
            }),
        )
        .await;

        GuestClient::new(socket, Duration::from_secs(1))
            .ping()
            .await
            .expect("ping");
    }

    #[tokio::test]
    async fn mismatched_response_id_is_rejected() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        spawn_server(
            socket.clone(),
            Arc::new(|_| json!({"id": "wrong", "ok": true})),
        )
        .await;
        let error = GuestClient::new(socket, Duration::from_secs(1))
            .ping()
            .await
            .expect_err("mismatch");
        assert!(matches!(error, GuestError::Protocol(_)));
    }

    #[tokio::test]
    async fn malformed_json_does_not_poison_the_next_call() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        let listener = UnixListener::bind(&socket).expect("bind");
        let server = tokio::spawn(async move {
            let (mut first, _) = accept_request(&listener).await;
            first.write_all(b"{not-json}\n").await.expect("malformed");

            let (mut second, request) = accept_request(&listener).await;
            let mut response =
                serde_json::to_vec(&json!({"id": request["id"], "ok": true})).expect("response");
            response.push(b'\n');
            second.write_all(&response).await.expect("valid");
        });
        let client = GuestClient::new(socket, Duration::from_secs(1));
        assert!(matches!(client.ping().await, Err(GuestError::Json(_))));
        client.ping().await.expect("subsequent request");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn missing_socket_is_reported_as_connection_failure() {
        let temp = tempfile::tempdir().expect("temp");
        let error = GuestClient::new(temp.path().join("missing.uds"), Duration::from_millis(100))
            .ping()
            .await
            .expect_err("connection failure");
        assert!(matches!(error, GuestError::Io(_)));
    }

    #[tokio::test]
    async fn connect_response_requires_a_numeric_peer_cid() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        let listener = UnixListener::bind(&socket).expect("bind");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let connect = read_line(&mut stream, 128).await.expect("connect");
            assert_eq!(connect, b"CONNECT 5000");
            stream
                .write_all(b"OK not-a-cid\n")
                .await
                .expect("invalid peer cid");
        });
        let error = GuestClient::new(socket, Duration::from_secs(1))
            .ping()
            .await
            .expect_err("invalid peer cid");
        assert!(matches!(error, GuestError::Protocol(_)));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn guest_rejection_is_returned_without_panicking() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        spawn_server(
            socket.clone(),
            Arc::new(|request| json!({"id": request["id"], "ok": false, "err": "not ready"})),
        )
        .await;
        let error = GuestClient::new(socket, Duration::from_secs(1))
            .ping()
            .await
            .expect_err("rejected");
        assert!(matches!(error, GuestError::Rejected(message) if message == "not ready"));
    }

    #[tokio::test]
    async fn line_reader_accepts_the_limit_and_rejects_one_more_byte() {
        let (mut exact_reader, mut exact_writer) = tokio::io::duplex(64);
        tokio::spawn(async move {
            exact_writer
                .write_all(b"1234\n")
                .await
                .expect("write exact");
        });
        assert_eq!(
            read_line(&mut exact_reader, 4).await.expect("exact limit"),
            b"1234"
        );

        let (mut oversized_reader, mut oversized_writer) = tokio::io::duplex(64);
        tokio::spawn(async move {
            oversized_writer
                .write_all(b"12345\n")
                .await
                .expect("write oversized");
        });
        assert!(matches!(
            read_line(&mut oversized_reader, 4)
                .await
                .expect_err("one byte over"),
            GuestError::PayloadTooLarge {
                actual: 5,
                limit: 4
            }
        ));
    }

    #[tokio::test]
    async fn bounded_response_rejects_an_oversized_line() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        let listener = UnixListener::bind(&socket).expect("bind");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let connect = read_line(&mut stream, 128).await.expect("connect");
            assert_eq!(connect, b"CONNECT 5000");
            stream.write_all(b"OK 5000\n").await.expect("ok");
            let _ = read_line(&mut stream, 4096).await.expect("request");
            stream
                .write_all(b"12345\n")
                .await
                .expect("oversized response");
        });

        let error = GuestClient::new(socket, Duration::from_secs(1))
            .with_response_limit(4)
            .ping()
            .await
            .expect_err("oversized line");
        assert!(matches!(error, GuestError::PayloadTooLarge { .. }));
    }

    #[tokio::test]
    async fn wait_ready_honors_cancellation() {
        let temp = tempfile::tempdir().expect("temp");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = GuestClient::new(temp.path().join("missing.uds"), Duration::from_millis(10))
            .wait_ready(Duration::from_secs(1), &cancellation)
            .await
            .expect_err("cancelled");
        assert!(matches!(error, GuestError::Cancelled));
    }

    #[tokio::test]
    async fn wait_ready_stops_at_its_deadline() {
        let temp = tempfile::tempdir().expect("temp");
        let started = Instant::now();
        let error = GuestClient::new(temp.path().join("missing.uds"), Duration::from_secs(1))
            .wait_ready(Duration::from_millis(60), &CancellationToken::new())
            .await
            .expect_err("deadline");
        assert!(matches!(error, GuestError::Timeout(_)));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn wait_ready_retries_after_one_stalled_connection() {
        let temp = tempfile::tempdir().expect("temp");
        let socket = temp.path().join("vsock.uds");
        let listener = UnixListener::bind(&socket).expect("bind");
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = attempts.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let attempt = server_attempts.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    if attempt == 0 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        return;
                    }
                    let connect = read_line(&mut stream, 128).await.expect("connect");
                    assert_eq!(connect, b"CONNECT 5000");
                    stream.write_all(b"OK 5000\n").await.expect("ok");
                    let request = read_line(&mut stream, 4096).await.expect("request");
                    let request: serde_json::Value =
                        serde_json::from_slice(&request).expect("json");
                    let response = json!({"id": request["id"], "ok": true});
                    let mut bytes = serde_json::to_vec(&response).expect("encode");
                    bytes.push(b'\n');
                    stream.write_all(&bytes).await.expect("write");
                });
            }
        });

        GuestClient::new(socket, Duration::from_secs(5))
            .wait_ready(Duration::from_secs(1), &CancellationToken::new())
            .await
            .expect("second readiness attempt");
        assert!(attempts.load(Ordering::Relaxed) >= 2);
    }
}

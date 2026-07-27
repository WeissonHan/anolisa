// SPDX-License-Identifier: Apache-2.0
//! Bounded daemon endpoint configuration and HTTP transport.

use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{ACCEPT, CONNECTION, CONTENT_TYPE, HOST, HeaderMap, USER_AGENT};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UnixStream};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::cli::EndpointSelection;

/// Maximum time allowed to establish one daemon connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum time allowed for one complete daemon request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum response bytes collected before protocol decoding.
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// Validated daemon endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// HTTP over a Unix domain socket.
    Unix(PathBuf),
    /// HTTP over an explicit TCP origin.
    Http(Uri),
}

/// Immutable safety bounds for a daemon client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Validated transport endpoint.
    pub endpoint: Endpoint,
    /// Connection establishment deadline.
    pub connect_timeout: Duration,
    /// Complete request deadline.
    pub request_timeout: Duration,
    /// Collected response body limit.
    pub max_response_bytes: usize,
}

impl ClientConfig {
    /// Validate a selected endpoint and apply the frozen safety bounds.
    ///
    /// # Errors
    ///
    /// Returns [`ClientConfigError`] when the socket or HTTP origin violates
    /// the endpoint contract.
    pub fn from_selection(selection: EndpointSelection) -> Result<Self, ClientConfigError> {
        let endpoint = match selection {
            EndpointSelection::Unix(path) => Endpoint::Unix(validate_socket(path)?),
            EndpointSelection::Http(value) => Endpoint::Http(validate_http_origin(&value)?),
        };
        Ok(Self {
            endpoint,
            connect_timeout: CONNECT_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            max_response_bytes: MAX_RESPONSE_BYTES,
        })
    }
}

/// One bounded HTTP response before protocol decoding.
#[derive(Debug, Clone)]
pub struct RawResponse {
    /// Daemon HTTP status.
    pub status: StatusCode,
    /// Daemon HTTP headers.
    pub headers: HeaderMap,
    /// Collected response bytes.
    pub body: Vec<u8>,
}

#[async_trait]
trait Connector: std::fmt::Debug + Send + Sync {
    async fn connect_unix(&self, path: &Path) -> std::io::Result<UnixStream>;

    async fn connect_tcp(&self, host: &str, port: u16) -> std::io::Result<TcpStream>;
}

#[derive(Debug)]
struct SystemConnector;

#[async_trait]
impl Connector for SystemConnector {
    async fn connect_unix(&self, path: &Path) -> std::io::Result<UnixStream> {
        UnixStream::connect(path).await
    }

    async fn connect_tcp(&self, host: &str, port: u16) -> std::io::Result<TcpStream> {
        TcpStream::connect((host, port)).await
    }
}

/// Bounded HTTP client for one validated daemon endpoint.
#[derive(Debug, Clone)]
pub struct BlazeClient {
    config: ClientConfig,
    connector: Arc<dyn Connector>,
}

impl BlazeClient {
    /// Create a client with validated endpoint and safety bounds.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            connector: Arc::new(SystemConnector),
        }
    }

    #[cfg(test)]
    fn with_connector(config: ClientConfig, connector: impl Connector + 'static) -> Self {
        Self {
            config,
            connector: Arc::new(connector),
        }
    }

    /// Send one request without automatic retries.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] for invalid routes, connection failures,
    /// timeouts, HTTP protocol failures, or oversized responses.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError> {
        let path = validate_request_path(path)?;
        timeout(
            self.config.request_timeout,
            self.request_once(method, path, body),
        )
        .await
        .map_err(|_| ClientError::RequestTimeout)?
    }

    async fn request_once(
        &self,
        method: Method,
        path: PathAndQuery,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError> {
        match &self.config.endpoint {
            Endpoint::Unix(socket) => {
                let stream = timeout(
                    self.config.connect_timeout,
                    self.connector.connect_unix(socket),
                )
                .await
                .map_err(|_| ClientError::ConnectTimeout)?
                .map_err(|source| ClientError::Connect { source })?;
                let uri = request_uri("localhost", path)?;
                self.send(stream, method, uri, body).await
            }
            Endpoint::Http(origin) => {
                let authority = origin
                    .authority()
                    .ok_or(ClientError::RequestBuildInvariant)?;
                let port = authority.port_u16().unwrap_or(80);
                let stream = timeout(
                    self.config.connect_timeout,
                    self.connector.connect_tcp(authority.host(), port),
                )
                .await
                .map_err(|_| ClientError::ConnectTimeout)?
                .map_err(|source| ClientError::Connect { source })?;
                let uri = request_uri(authority.as_str(), path)?;
                self.send(stream, method, uri, body).await
            }
        }
    }

    async fn send<T>(
        &self,
        stream: T,
        method: Method,
        uri: Uri,
        body: Vec<u8>,
    ) -> Result<RawResponse, ClientError>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let authority = uri
            .authority()
            .ok_or(ClientError::RequestBuildInvariant)?
            .as_str()
            .to_owned();
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(ACCEPT, "application/json")
            .header(CONNECTION, "close")
            .header(HOST, authority)
            .header(USER_AGENT, concat!("blazectl/", env!("CARGO_PKG_VERSION")));
        if !body.is_empty() {
            builder = builder.header(CONTENT_TYPE, "application/json");
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|source| ClientError::RequestBuild { source })?;

        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|source| ClientError::Handshake { source })?;
        let mut connections = JoinSet::new();
        connections.spawn(connection);
        let response = sender
            .send_request(request)
            .await
            .map_err(|source| ClientError::Request { source })?;
        let result = collect_response(response, self.config.max_response_bytes).await;
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        result
    }
}

/// Endpoint validation failures that never reflect input values.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigError {
    /// The UDS path is not an absolute, non-empty, NUL-free path.
    #[error("daemon socket path must be absolute and NUL-free")]
    InvalidSocket,
    /// The URL is not a valid absolute HTTP origin.
    #[error("daemon URL must be an absolute HTTP origin")]
    InvalidUrl,
    /// The URL scheme is not the approved plain HTTP transport.
    #[error("daemon URL scheme must be http")]
    UnsupportedScheme,
    /// User information must never be accepted or reflected.
    #[error("daemon URL must not contain userinfo")]
    UserInfo,
    /// Query and fragment components are outside the endpoint contract.
    #[error("daemon URL must not contain query or fragment components")]
    QueryOrFragment,
    /// A base path would make route construction ambiguous.
    #[error("daemon URL must not contain a base path")]
    BasePath,
}

/// Request transport failures that never reflect endpoint input.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Only canonical daemon API paths are accepted.
    #[error("request path must be a canonical /v1 route without query or fragment")]
    InvalidPath,
    /// A validated endpoint lost an invariant during request construction.
    #[error("validated endpoint could not build an HTTP request")]
    RequestBuildInvariant,
    /// The connection deadline elapsed.
    #[error("daemon connection timed out")]
    ConnectTimeout,
    /// The endpoint could not be reached.
    #[error("could not connect to the daemon")]
    Connect {
        /// Underlying I/O failure retained for diagnostics.
        #[source]
        source: std::io::Error,
    },
    /// HTTP client/server negotiation failed.
    #[error("daemon HTTP handshake failed")]
    Handshake {
        /// Underlying HTTP failure retained for diagnostics.
        #[source]
        source: hyper::Error,
    },
    /// A request could not be constructed.
    #[error("daemon HTTP request could not be constructed")]
    RequestBuild {
        /// Underlying builder failure retained for diagnostics.
        #[source]
        source: hyper::http::Error,
    },
    /// Sending or receiving the request failed.
    #[error("daemon HTTP request failed")]
    Request {
        /// Underlying HTTP failure retained for diagnostics.
        #[source]
        source: hyper::Error,
    },
    /// The complete request deadline elapsed.
    #[error("daemon request timed out")]
    RequestTimeout,
    /// A response exceeded the configured collection bound.
    #[error("daemon response exceeded the configured size limit")]
    ResponseTooLarge,
}

fn validate_socket(path: PathBuf) -> Result<PathBuf, ClientConfigError> {
    if !path.is_absolute()
        || path.as_os_str().is_empty()
        || path.as_os_str().as_bytes().contains(&0)
    {
        return Err(ClientConfigError::InvalidSocket);
    }
    Ok(path)
}

fn validate_http_origin(value: &str) -> Result<Uri, ClientConfigError> {
    if value.contains('#') {
        return Err(ClientConfigError::QueryOrFragment);
    }
    if authority_text(value).is_some_and(|authority| authority.contains('@')) {
        return Err(ClientConfigError::UserInfo);
    }
    let uri: Uri = value.parse().map_err(|_| ClientConfigError::InvalidUrl)?;
    if uri.scheme_str() != Some("http") {
        return Err(ClientConfigError::UnsupportedScheme);
    }
    let authority = uri.authority().ok_or(ClientConfigError::InvalidUrl)?;
    if authority.host().is_empty() {
        return Err(ClientConfigError::InvalidUrl);
    }
    if uri.query().is_some() {
        return Err(ClientConfigError::QueryOrFragment);
    }
    if uri.path() != "/" {
        return Err(ClientConfigError::BasePath);
    }
    Ok(uri)
}

fn authority_text(value: &str) -> Option<&str> {
    let (_, remainder) = value.split_once("://")?;
    let end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    Some(&remainder[..end])
}

fn validate_request_path(path: &str) -> Result<PathAndQuery, ClientError> {
    if !path.starts_with("/v1/") || path.contains('?') || path.contains('#') {
        return Err(ClientError::InvalidPath);
    }
    path.parse().map_err(|_| ClientError::InvalidPath)
}

fn request_uri(authority: &str, path: PathAndQuery) -> Result<Uri, ClientError> {
    Uri::builder()
        .scheme("http")
        .authority(authority)
        .path_and_query(path)
        .build()
        .map_err(|_| ClientError::RequestBuildInvariant)
}

async fn collect_response(
    response: hyper::Response<Incoming>,
    limit: usize,
) -> Result<RawResponse, ClientError> {
    let (parts, mut body) = response.into_parts();
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|source| ClientError::Request { source })?;
        if let Ok(data) = frame.into_data() {
            let next = collected.len().saturating_add(data.len());
            if next > limit {
                return Err(ClientError::ResponseTooLarge);
            }
            collected.extend_from_slice(&data);
        }
    }
    Ok(RawResponse {
        status: parts.status,
        headers: parts.headers,
        body: collected,
    })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::pending;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::{Response, StatusCode};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, UnixListener};
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct PendingConnector {
        unix_calls: Arc<AtomicUsize>,
        tcp_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Connector for PendingConnector {
        async fn connect_unix(&self, _path: &std::path::Path) -> std::io::Result<UnixStream> {
            self.unix_calls.fetch_add(1, Ordering::SeqCst);
            pending().await
        }

        async fn connect_tcp(&self, _host: &str, _port: u16) -> std::io::Result<TcpStream> {
            self.tcp_calls.fetch_add(1, Ordering::SeqCst);
            pending().await
        }
    }

    #[test]
    fn socket_requires_an_absolute_path() {
        let config =
            ClientConfig::from_selection(EndpointSelection::Unix(PathBuf::from("/tmp/api.sock")))
                .expect("absolute socket");
        assert_eq!(
            config.endpoint,
            Endpoint::Unix(PathBuf::from("/tmp/api.sock"))
        );
        assert_eq!(
            ClientConfig::from_selection(EndpointSelection::Unix(PathBuf::from("api.sock"))),
            Err(ClientConfigError::InvalidSocket)
        );
    }

    #[test]
    fn http_origin_accepts_only_the_frozen_shape() {
        let config = ClientConfig::from_selection(EndpointSelection::Http(
            "http://127.0.0.1:14159".to_string(),
        ))
        .expect("HTTP origin");
        assert_eq!(
            config.endpoint,
            Endpoint::Http("http://127.0.0.1:14159".parse().expect("expected URI"))
        );

        let cases = [
            (
                "https://127.0.0.1:14159",
                ClientConfigError::UnsupportedScheme,
            ),
            ("http://alpha@127.0.0.1:14159", ClientConfigError::UserInfo),
            (
                "http://127.0.0.1:14159?mode=x",
                ClientConfigError::QueryOrFragment,
            ),
            (
                "http://127.0.0.1:14159#x",
                ClientConfigError::QueryOrFragment,
            ),
            ("http://127.0.0.1:14159/v1", ClientConfigError::BasePath),
            ("not-an-origin", ClientConfigError::UnsupportedScheme),
        ];
        for (value, expected) in cases {
            let actual = ClientConfig::from_selection(EndpointSelection::Http(value.to_string()));
            assert_eq!(actual, Err(expected), "{value}");
        }
    }

    #[test]
    fn validation_errors_do_not_reflect_endpoint_input() {
        let input = "http://alpha@127.0.0.1:14159";
        let error = ClientConfig::from_selection(EndpointSelection::Http(input.to_string()))
            .expect_err("userinfo");
        assert!(!error.to_string().contains(input));
    }

    #[test]
    fn safety_bounds_match_the_approved_contract() {
        let config =
            ClientConfig::from_selection(EndpointSelection::Unix(PathBuf::from("/tmp/api.sock")))
                .expect("config");
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_response_bytes, 32 * 1024 * 1024);
    }

    #[test]
    fn request_paths_are_canonical_and_query_free() {
        assert_eq!(
            validate_request_path("/v1/sandboxes")
                .expect("canonical path")
                .as_str(),
            "/v1/sandboxes"
        );
        for path in [
            "/health",
            "/v2/sandboxes",
            "/v1/sandboxes?all=true",
            "/v1/sandboxes#x",
        ] {
            assert!(matches!(
                validate_request_path(path),
                Err(ClientError::InvalidPath)
            ));
        }
    }

    #[tokio::test]
    async fn tcp_transport_preserves_wire_contract() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|request: Request<Incoming>| async move {
                        assert_eq!(request.method(), Method::POST);
                        assert_eq!(request.uri().path(), "/v1/sandboxes");
                        assert_eq!(request.headers()[ACCEPT], "application/json");
                        assert_eq!(request.headers()[CONTENT_TYPE], "application/json");
                        assert_eq!(
                            request.headers()[HOST].to_str().expect("host"),
                            address.to_string()
                        );
                        let body = request
                            .into_body()
                            .collect()
                            .await
                            .expect("request body")
                            .to_bytes();
                        assert_eq!(body.as_ref(), br#"{"template":"base"}"#.as_slice());
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::CREATED)
                                .header(CONTENT_TYPE, "application/json")
                                .body(Full::new(Bytes::from_static(br#"{"status":"running"}"#)))
                                .expect("response"),
                        )
                    }),
                )
                .await
                .expect("serve");
        });
        let client = BlazeClient::new(test_config(Endpoint::Http(
            format!("http://{address}").parse().expect("origin"),
        )));
        let response = client
            .request(
                Method::POST,
                "/v1/sandboxes",
                br#"{"template":"base"}"#.to_vec(),
            )
            .await
            .expect("request");
        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.body.as_slice(), br#"{"status":"running"}"#);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn uds_transport_accepts_empty_204_response() {
        let socket = std::env::temp_dir().join(format!("blazectl-{}.sock", Uuid::new_v4()));
        let listener = UnixListener::bind(&socket).expect("bind");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|request: Request<Incoming>| async move {
                        assert_eq!(request.method(), Method::DELETE);
                        assert_eq!(
                            request.uri().path(),
                            "/v1/sandboxes/00000000-0000-4000-8000-000000000001"
                        );
                        assert_eq!(request.headers()[HOST].to_str().expect("host"), "localhost");
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::NO_CONTENT)
                                .body(Full::new(Bytes::new()))
                                .expect("response"),
                        )
                    }),
                )
                .await
                .expect("serve");
        });
        let client = BlazeClient::new(test_config(Endpoint::Unix(socket.clone())));
        let response = client
            .request(
                Method::DELETE,
                "/v1/sandboxes/00000000-0000-4000-8000-000000000001",
                Vec::new(),
            )
            .await
            .expect("request");
        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert!(response.body.is_empty());
        server.await.expect("server task");
        std::fs::remove_file(socket).expect("remove socket");
    }

    #[tokio::test]
    async fn response_body_cap_is_enforced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"12345"))))
                    }),
                )
                .await;
        });
        let mut config = test_config(Endpoint::Http(
            format!("http://{address}").parse().expect("origin"),
        ));
        config.max_response_bytes = 4;
        let error = BlazeClient::new(config)
            .request(Method::GET, "/v1/health", Vec::new())
            .await
            .expect_err("oversized response");
        assert!(matches!(error, ClientError::ResponseTooLarge));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn complete_request_timeout_is_enforced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_: Request<Incoming>| async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                    }),
                )
                .await;
        });
        let mut config = test_config(Endpoint::Http(
            format!("http://{address}").parse().expect("origin"),
        ));
        config.request_timeout = Duration::from_millis(10);
        let error = BlazeClient::new(config)
            .request(Method::GET, "/v1/health", Vec::new())
            .await
            .expect_err("request timeout");
        assert!(matches!(error, ClientError::RequestTimeout));
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn uds_connect_timeout_is_hermetic_and_non_reflecting() {
        assert_connect_timeout(
            Endpoint::Unix(PathBuf::from("/tmp/blazectl-connect-timeout.sock")),
            1,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn tcp_connect_timeout_is_hermetic_and_non_reflecting() {
        assert_connect_timeout(
            Endpoint::Http("http://127.0.0.1:14159".parse().expect("origin")),
            0,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn connection_error_does_not_reflect_socket_path() {
        let socket = std::env::temp_dir().join(format!("blazectl-missing-{}.sock", Uuid::new_v4()));
        let error = BlazeClient::new(test_config(Endpoint::Unix(socket.clone())))
            .request(Method::GET, "/v1/health", Vec::new())
            .await
            .expect_err("missing socket");
        assert!(matches!(error, ClientError::Connect { .. }));
        assert_eq!(error.to_string(), "could not connect to the daemon");
        assert!(!error.to_string().contains(&socket.display().to_string()));
    }

    #[tokio::test]
    async fn malformed_http_response_is_a_protocol_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            stream.write_all(b"not-http\r\n").await.expect("write");
            stream.shutdown().await.expect("shutdown");
        });
        let error = BlazeClient::new(test_config(Endpoint::Http(
            format!("http://{address}").parse().expect("origin"),
        )))
        .request(Method::GET, "/v1/health", Vec::new())
        .await
        .expect_err("malformed response");
        assert!(matches!(
            error,
            ClientError::Handshake { .. } | ClientError::Request { .. }
        ));
        server.await.expect("server task");
    }

    fn test_config(endpoint: Endpoint) -> ClientConfig {
        ClientConfig {
            endpoint,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            max_response_bytes: 1024,
        }
    }

    async fn assert_connect_timeout(
        endpoint: Endpoint,
        expected_unix_calls: usize,
        expected_tcp_calls: usize,
    ) {
        let unix_calls = Arc::new(AtomicUsize::new(0));
        let tcp_calls = Arc::new(AtomicUsize::new(0));
        let connector = PendingConnector {
            unix_calls: Arc::clone(&unix_calls),
            tcp_calls: Arc::clone(&tcp_calls),
        };
        let mut config = test_config(endpoint);
        config.connect_timeout = Duration::from_millis(1);
        let error = BlazeClient::with_connector(config, connector)
            .request(Method::GET, "/v1/health", Vec::new())
            .await
            .expect_err("connect timeout");

        assert!(matches!(error, ClientError::ConnectTimeout));
        assert_eq!(error.to_string(), "daemon connection timed out");
        assert_eq!(unix_calls.load(Ordering::SeqCst), expected_unix_calls);
        assert_eq!(tcp_calls.load(Ordering::SeqCst), expected_tcp_calls);
    }
}

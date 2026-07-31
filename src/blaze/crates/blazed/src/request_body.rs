// SPDX-License-Identifier: Apache-2.0
//! Bounded HTTP request-body collection for every daemon API route.

use http_body_util::BodyExt;
use hyper::Request;
use hyper::body::{Body, Bytes};
use hyper::header::CONTENT_LENGTH;

use crate::error::{BlazeDaemonError, Result};

/// Collect a request body without buffering more than `limit` bytes.
pub(crate) async fn collect<B>(req: Request<B>, limit: usize) -> Result<Vec<u8>>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    if let Some(declared) = declared_body_length(&req)?
        && declared > limit as u64
    {
        return Err(BlazeDaemonError::PayloadTooLarge {
            actual: declared,
            limit,
        });
    }

    let mut body = req.into_body();
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| BlazeDaemonError::RequestBody(error.to_string()))?;
        if let Ok(data) = frame.into_data() {
            let actual = collected.len().checked_add(data.len()).ok_or(
                BlazeDaemonError::PayloadTooLarge {
                    actual: u64::MAX,
                    limit,
                },
            )?;
            if actual > limit {
                return Err(BlazeDaemonError::PayloadTooLarge {
                    actual: actual as u64,
                    limit,
                });
            }
            collected.extend_from_slice(&data);
        }
    }
    Ok(collected)
}

fn declared_body_length<B>(req: &Request<B>) -> Result<Option<u64>> {
    let mut declared = None;
    for value in req.headers().get_all(CONTENT_LENGTH) {
        let value = value
            .to_str()
            .map_err(|_| BlazeDaemonError::BadRequest("invalid Content-Length".into()))?;
        for item in value.split(',') {
            let length = item
                .trim()
                .parse::<u64>()
                .map_err(|_| BlazeDaemonError::BadRequest("invalid Content-Length".into()))?;
            match declared {
                Some(previous) if previous != length => {
                    return Err(BlazeDaemonError::BadRequest(
                        "conflicting Content-Length values".into(),
                    ));
                }
                None => declared = Some(length),
                _ => {}
            }
        }
    }
    Ok(declared)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use hyper::body::Frame;
    use hyper::header::{CONTENT_LENGTH, TRANSFER_ENCODING};

    use super::*;

    #[derive(Debug)]
    struct TestBodyError;

    impl fmt::Display for TestBodyError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("test body failed")
        }
    }

    impl std::error::Error for TestBodyError {}

    struct TestBody {
        frames: VecDeque<std::result::Result<Frame<Bytes>, TestBodyError>>,
        polls: Arc<AtomicUsize>,
        panic_when_exhausted: bool,
    }

    impl TestBody {
        fn new(
            frames: impl IntoIterator<Item = std::result::Result<Frame<Bytes>, TestBodyError>>,
            polls: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                polls,
                panic_when_exhausted: false,
            }
        }

        fn panic_when_exhausted(mut self) -> Self {
            self.panic_when_exhausted = true;
            self
        }
    }

    impl Body for TestBody {
        type Data = Bytes;
        type Error = TestBodyError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, Ordering::AcqRel);
            match this.frames.pop_front() {
                Some(frame) => Poll::Ready(Some(frame)),
                None if this.panic_when_exhausted => {
                    panic!("collector polled after the limit had already been exceeded")
                }
                None => Poll::Ready(None),
            }
        }
    }

    #[tokio::test]
    async fn accepts_body_at_declared_limit() {
        let polls = Arc::new(AtomicUsize::new(0));
        let body = TestBody::new(
            [
                Ok(Frame::data(Bytes::from_static(b"ab"))),
                Ok(Frame::data(Bytes::from_static(b"cd"))),
            ],
            polls.clone(),
        );
        let request = Request::builder()
            .header(CONTENT_LENGTH, "4")
            .body(body)
            .expect("request");

        assert_eq!(collect(request, 4).await.expect("body"), b"abcd");
        assert_eq!(polls.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn rejects_large_content_length_before_polling_body() {
        let polls = Arc::new(AtomicUsize::new(0));
        let body = TestBody::new(
            [Ok(Frame::data(Bytes::from_static(b"body")))],
            polls.clone(),
        )
        .panic_when_exhausted();
        let request = Request::builder()
            .header(CONTENT_LENGTH, "5")
            .body(body)
            .expect("request");

        let error = collect(request, 4).await.expect_err("oversized body");
        assert!(matches!(
            error,
            BlazeDaemonError::PayloadTooLarge {
                actual: 5,
                limit: 4
            }
        ));
        assert_eq!(error.status_code(), 413);
        assert_eq!(polls.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn stops_collecting_chunked_body_at_limit() {
        let polls = Arc::new(AtomicUsize::new(0));
        let body = TestBody::new(
            [
                Ok(Frame::data(Bytes::from_static(b"abcd"))),
                Ok(Frame::data(Bytes::from_static(b"e"))),
            ],
            polls.clone(),
        )
        .panic_when_exhausted();
        let request = Request::builder()
            .header(TRANSFER_ENCODING, "chunked")
            .body(body)
            .expect("request");

        let error = collect(request, 4).await.expect_err("oversized body");
        assert!(matches!(
            error,
            BlazeDaemonError::PayloadTooLarge {
                actual: 5,
                limit: 4
            }
        ));
        assert_eq!(polls.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn stops_collecting_undelimited_body_at_limit() {
        let polls = Arc::new(AtomicUsize::new(0));
        let body = TestBody::new(
            [
                Ok(Frame::data(Bytes::from_static(b"ab"))),
                Ok(Frame::data(Bytes::from_static(b"cde"))),
            ],
            polls.clone(),
        )
        .panic_when_exhausted();

        let error = collect(Request::new(body), 4)
            .await
            .expect_err("oversized body");
        assert!(matches!(
            error,
            BlazeDaemonError::PayloadTooLarge {
                actual: 5,
                limit: 4
            }
        ));
        assert_eq!(polls.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn maps_body_read_failures_to_bad_request() {
        let body = TestBody::new([Err(TestBodyError)], Arc::new(AtomicUsize::new(0)));
        let error = collect(Request::new(body), 4)
            .await
            .expect_err("body read must fail");

        assert!(matches!(error, BlazeDaemonError::RequestBody(_)));
        assert_eq!(error.status_code(), 400);
    }
}

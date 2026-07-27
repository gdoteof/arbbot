//! The signed-request seam.
//!
//! A [`Transport`] takes an already-signed request and puts it on the wire.
//! Splitting it out is what lets the venue quirks be tested without a network:
//! the gateway composes path/query/body/headers (where every quirk lives) and
//! the transport only moves bytes.
//!
//! Three impls:
//!   [`NotWired`]      — the default. Every send is [`VenueError::NotWired`],
//!                       so a gateway built with `new()` cannot reach a venue.
//!   [`HttpTransport`] — real reqwest/rustls, blocking (one-shot tools).
//!   `MockTransport`   — in tests; canned (status, body) per request.

use crate::error::VenueError;
use serde_json::Value;

/// A raw venue response. Status is kept verbatim — several Kalshi quirks are
/// status-coded (404-on-cancel means "already gone", i.e. success), so the
/// transport must never collapse a status into an error on the caller's behalf.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

pub trait Transport {
    /// `path` is the full API path (including the `/trade-api/v2` prefix) and
    /// is what the signature covers. `query` is appended to the URL but is
    /// NEVER part of the signed message — signing it is Kalshi quirk K2 and
    /// produces a 401 that looks like a bad key.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        headers: &[(&'static str, String)],
        body: Option<&Value>,
    ) -> Result<Response, VenueError>;
}

/// The inert transport: the seam with nothing behind it.
pub struct NotWired;

impl Transport for NotWired {
    fn send(
        &self,
        _method: &str,
        _path: &str,
        _query: Option<&str>,
        _headers: &[(&'static str, String)],
        _body: Option<&Value>,
    ) -> Result<Response, VenueError> {
        Err(VenueError::NotWired)
    }
}

/// Real HTTP. Blocking on purpose: the first venue-write is a one-shot tool,
/// and a blocking client keeps [`crate::gateway::VenueGateway`] sync. It must
/// not be constructed inside a tokio runtime.
pub struct HttpTransport {
    base: String,
    client: reqwest::blocking::Client,
}

impl HttpTransport {
    /// `base` is the origin + API prefix, e.g.
    /// `https://api.elections.kalshi.com/trade-api/v2`. Paths passed to
    /// `send` are signed WITH that prefix but appended to `base` without it —
    /// see [`Self::url`].
    pub fn new(base: impl Into<String>, timeout_secs: u64) -> Result<Self, VenueError> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| VenueError::Transport(e.to_string()))?;
        Ok(Self { base: base.into(), client })
    }

    /// The signed path carries the API prefix (`/trade-api/v2/portfolio/...`)
    /// but `base` already ends in it, so strip the overlap rather than
    /// double it. Keeping ONE path string means the signature and the URL can
    /// never drift apart.
    fn url(&self, path: &str, query: Option<&str>) -> String {
        let suffix = match self.base.rfind("/trade-api/v2") {
            Some(_) => path.strip_prefix("/trade-api/v2").unwrap_or(path),
            None => path,
        };
        match query {
            Some(q) if !q.is_empty() => format!("{}{}?{}", self.base, suffix, q),
            _ => format!("{}{}", self.base, suffix),
        }
    }
}

impl Transport for HttpTransport {
    fn send(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        headers: &[(&'static str, String)],
        body: Option<&Value>,
    ) -> Result<Response, VenueError> {
        let url = self.url(path, query);
        let m = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| VenueError::Transport(format!("bad method {method}: {e}")))?;
        let mut req = self.client.request(m, &url);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().map_err(|e| VenueError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let body = resp.text().map_err(|e| VenueError::Transport(e.to_string()))?;
        Ok(Response { status, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_does_not_double_the_api_prefix() {
        let t = HttpTransport::new("https://api.elections.kalshi.com/trade-api/v2", 5).unwrap();
        assert_eq!(
            t.url("/trade-api/v2/portfolio/orders", None),
            "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders"
        );
    }

    #[test]
    fn query_is_appended_to_the_url() {
        let t = HttpTransport::new("https://api.elections.kalshi.com/trade-api/v2", 5).unwrap();
        assert_eq!(
            t.url("/trade-api/v2/portfolio/orders", Some("cursor=abc")),
            "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders?cursor=abc"
        );
    }
}

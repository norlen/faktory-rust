#![warn(rust_2018_idioms)]
#![deny(missing_docs)]
//! faktory-rust provides a `Consumer` and `Producer` to push and fetch jobs from a Faktory server.

use thiserror::Error;

mod consumer;
mod producer;
mod protocol;

pub use consumer::Consumer;
pub use producer::Producer;
pub use protocol::Job;

/// 
#[derive(Debug, Error)]
pub enum Error {
    /// If no address is supplied, so the client cannot connect to the server.
    #[error("No faktory address supplied or found in FAKTORY_URL")]
    NoAddress,

    /// If no state is supplied that can get passed onto the handlers.
    #[error("Currently a valid state is required")]
    NoState,

    /// If the client expected a response but did not get any.
    #[error("Expected response but got none")]
    NoResponse,

    /// If a certain response was expected, but it received another one.
    #[error("Unxpected response: expected {} but got {}", .0, .1)]
    UnexpectedResponse(String, String),

    /// If a job is fetched which does not have a registered handler.
    #[error("No handler for {}", .0)]
    NoHandler(String),

    /// Other errors from the underlying client.
    #[error("Protocol error: {}", .0)]
    ProtocolError(#[from] protocol::Error),
}

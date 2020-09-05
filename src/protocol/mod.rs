use std::{io, str::Utf8Error};
use thiserror::Error;

mod client;
pub(crate) mod commands;
mod connection;
mod frame;
mod responses;
mod job;

pub(crate) use client::{Client, State, ConnectInfo};
pub use job::Job;

/// Supported protocol version.
pub(self) const PROTOCOL_VERSION: usize = 2;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Incomplete")]
    Incomplete,

    #[error("{}", .0)]
    UtfError(#[from] Utf8Error),

    #[error("Invalid frame: {}", .0)]
    InvalidFrameByte(u8),

    #[error("Invalid frame format")]
    InvalidFrameFormat,

    #[error("Connection reset by peer")]
    ConnectionReset,

    #[error("Expected response but got none")]
    NoResponse,

    #[error("Expected {} but got {}", .0, .1)]
    UnexpectedResponse(String, String),

    #[error("{}", .0)]
    Io(#[from] io::Error),

    #[error("Could not parse data in message: {}", .0)]
    CouldNotParse(#[from] serde_json::Error),

    #[error("Unexpected protocol version: server has {} but client expects {}", .0, .1)]
    UnexpectedProtocol(usize, usize),

    #[error("Server error: {}", .0)]
    ServerError(String),
}

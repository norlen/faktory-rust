use super::{
    commands::Command,
    frame::Frame,
    Error,
};
use bytes::{Buf, BytesMut};
use serde::Serialize;
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// CRCL is sent in every frame, so a small constant for that.
const CRLF: &[u8] = b"\r\n";

/// Send and receive `Frame`s from a remote location.
pub struct Connection {
    stream: BufWriter<TcpStream>,

    /// Buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub(crate) async fn read_frame(&mut self) -> Result<Option<Frame>, Error> {
        loop {
            // Attempt to parse a frame from the buffered data.
            if let Some(frame) = self.parse_frame()? {
                // println!("[CONN] read_frame {:?}", frame);
                return Ok(Some(frame));
            }

            // If there is not enough data to parse a frame we attempt to read more.
            // If the end of the stream is reached read returns zero bytes read.
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // The remote closed the connection, for a clean shutdown there should
                // be no data left in the buffer.
                if self.buffer.is_empty() {
                    // println!("[CONN] read_frame OK(NONE)");
                    return Ok(None);
                } else {
                    return Err(Error::ConnectionReset);
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, Error> {
        use super::Error::Incomplete;

        let mut buf = Cursor::new(&self.buffer[..]);

        // Check if enough data has been buffered to parse an entire frame.
        match Frame::check(&mut buf) {
            // If we have enough data to parse an entire frame we get an `Ok`.
            Ok(_) => {
                // Check will have advanced the cursor to the end of the frame.
                // We get this position and reset it.
                let len = buf.position() as usize;
                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                // Discard data after parsing a frame.
                self.buffer.advance(len);

                Ok(Some(frame))
            }

            // If there is not enough data to parse an entire frame we get back `Incomplete`.
            // This is expected so we just return `None` here.
            Err(Incomplete) => Ok(None),

            // These are other errors encountered that are not expected.
            Err(e) => Err(e),
        }
    }

    /// Write a single command without additional data to the underlying stream.
    pub(crate) async fn write_simple<T>(&mut self) -> io::Result<()>
    where
        T: Command,
    {
        self.stream.write_all(T::NAME.as_bytes()).await?;
        self.stream.write_all(CRLF).await?;
        self.stream.flush().await
    }

    /// Write a single command with additional string data to the underlying stream.
    pub(crate) async fn write_str<T>(&mut self, data: &str) -> io::Result<()>
    where
        T: Command,
    {
        self.stream.write_all(T::NAME.as_bytes()).await?;
        self.stream.write_all(b" ").await?;
        self.stream.write_all(data.as_bytes()).await?;
        self.stream.write_all(CRLF).await?;
        self.stream.flush().await
    }

    /// Write a single command with the data serialized as json to the underlying stream.
    pub(crate) async fn write_json<T>(&mut self, command: &T) -> io::Result<()>
    where
        T: Command + Serialize,
    {
        let ser = serde_json::to_string(command)?;
        self.stream.write_all(T::NAME.as_bytes()).await?;
        self.stream.write_all(b" ").await?;
        self.stream.write_all(ser.as_bytes()).await?;
        self.stream.write_all(CRLF).await?;
        self.stream.flush().await
    }
}

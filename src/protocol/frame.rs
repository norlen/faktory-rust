use super::Error;
use bytes::{Buf, Bytes};
use std::io::Cursor;

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    pub(super) fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => get_line(src).map(|_| ()),
            b'-' => get_line(src).map(|_| ()),
            b':' => get_decimal(src).map(|_| ()),
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'.
                    skip(src, 4)
                } else {
                    // Read bulk string.
                    let len = get_decimal(src)? as usize;

                    // Skip those bytes + \r\n.
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)? as usize;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            actual => Err(Error::InvalidFrameByte(actual)),
        }
    }

    pub(super) fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?;
                let string = std::str::from_utf8(line)?;
                Ok(Frame::Simple(string.to_owned()))
            }
            b'-' => {
                let line = get_line(src)?;
                let string = std::str::from_utf8(line)?;
                Ok(Frame::Error(string.to_owned()))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(Error::InvalidFrameFormat);
                    }
                    Ok(Frame::Null)
                } else {
                    // Read bulk string.
                    let len = get_decimal(src)? as usize;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.bytes()[..len]);

                    // Skip those bytes and \r\n.
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?;
                let mut out = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    let frame = Frame::parse(src)?;
                    out.push(frame);
                }

                Ok(Frame::Array(out))
            }
            _ => todo!(),
        }
    }
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        Err(Error::Incomplete)
    } else {
        src.advance(n);
        Ok(())
    }
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    let line = get_line(src)?;
    atoi::<u64>(line).ok_or(Error::InvalidFrameFormat)
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        Err(Error::Incomplete)
    } else {
        Ok(src.bytes()[0])
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        Err(Error::Incomplete)
    } else {
        Ok(src.get_u8())
    }
}

fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // Line found, set position to be after \n.
            src.set_position(i as u64 + 2);

            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}

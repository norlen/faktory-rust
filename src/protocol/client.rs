use super::{
    commands::{Ack, Beat, End, Fail, Fetch, Flush, Hello, Info, Push},
    connection::Connection,
    frame::Frame,
    responses::Hi,
    Error, Job, PROTOCOL_VERSION,
};
use serde::Deserialize;

/// The different states a client can be in. Some are only possible to get into
/// by actions of the server from e.g. the `Beat` response.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum State {
    /// After a successful `Hello` the client is in the `Identified` state.
    /// where it can issue commands at will.
    Identified,

    /// After a `Beat` the client can be put in a `Quiet` state where it should
    /// not fetch any more jobs. While in this state the client must not terminate
    /// and must continue issue regular `Beat`s.
    Quiet,

    /// After a `Beat` the client can be put in a `Terminating` state where it must not
    /// fetch any more jobs. It should also issue a `Fail` for any current jobs within 30
    /// seconds. It must as well enter the `End` state after at most 30 seconds.
    Terminating,

    /// In this state the connection is being terminated. The client sends an `End` message
    /// to enter this state, where the server sends an `Ok` that must be handled.
    ///
    /// The server is not allowed to put a consumer in this state unilaterally without first
    /// changing its state to either `Quiet` or `Terminating`. But it can do it for producers.
    End,
}

impl State {
    /// Tries to convert from a `StateChange` to `State`, returns None if no matching
    /// state could be parsed.
    fn from_state_change(s: &StateChange) -> Option<Self> {
        match s.state.as_str() {
            "quiet" => Some(State::Quiet),
            "terminating" => Some(State::Terminating),
            _ => None,
        }
    }
}

/// State changes sent back by the server are represented as this.
#[derive(Debug, Deserialize)]
struct StateChange {
    state: String,
}

/// Additional information that is needed if we want to connect as a consumer.
pub(crate) struct ConnectInfo<'a> {
    pub(crate) hostname: &'a str,
    pub(crate) worker_id: &'a str,
    pub(crate) pid: usize,
}

/// Client provides the higher level representation of the connection to the server.
/// It sends commands back and forth, without having to worry about the actual frames sent.
pub(crate) struct Client {
    pub(crate) state: State,
    conn: Connection,
}

impl Client {
    /// Connect to the server and initialize the connection so the server is ready
    /// to accept requests from the client. Optionally takes a `ConnectInfo` if the client
    /// wants to be a consumer some additional fields are required. If server is password
    /// protected the password has to be supplied.
    pub(crate) async fn connect(addr: &str, info: Option<ConnectInfo<'_>>, password: Option<&str>) -> Result<Self, Error> {
        let socket = tokio::net::TcpStream::connect(addr).await?;
        let conn = Connection::new(socket);

        let mut client = Self {
            state: State::Identified,
            conn,
        };

        let password = if let Some(password) = password {
            password
        } else {
            ""
        };
        client.exchange_hello(info, password).await?;

        Ok(client)
    }

    /// End the connection to the server.
    pub(crate) async fn end(&mut self) -> Result<(), Error> {
        self.conn.write_simple::<End>().await?;
        self.read_ok().await?;
        self.state = State::End;
        Ok(())
    }

    /// Push a job to the server.
    pub(crate) async fn push(&mut self, job: &Job) -> Result<(), Error> {
        let push = Push::new(job);
        self.conn.write_json(&push).await?;
        self.read_ok().await
    }

    /// Clears all jobs on the server.
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        self.conn.write_simple::<Flush>().await?;
        self.read_ok().await
    }

    /// Get stats about the server. Returns a string that can be deserialized if needed.
    pub(crate) async fn info(&mut self) -> Result<String, Error> {
        self.conn.write_simple::<Info>().await?;
        let res = self.conn.read_frame().await?.ok_or(Error::NoResponse)?;
        match &res {
            Frame::Bulk(s) => {
                let info = std::str::from_utf8(s)?;
                Ok(info.to_owned())
            }
            Frame::Error(e) => Err(Error::ServerError(e.to_string())),
            _ => Err(Error::UnexpectedResponse(
                "INFO".to_owned(),
                format!("{:?}", res),
            )),
        }
    }

    /// Fetch a single from the server. Can take up to 2 seconds to fetch the job if none are available,
    /// and if it still cant find one None is returned.
    pub(crate) async fn fetch(&mut self, queues: &str) -> Result<Option<Job>, Error> {
        // Send FETCH.
        self.conn.write_str::<Fetch<'_>>(queues).await?;

        // We can either receive Null or a work unit.
        let res = self.conn.read_frame().await?.ok_or(Error::NoResponse)?;

        let job = match &res {
            // No work unit available.
            Frame::Null => None,

            // Got one.
            Frame::Bulk(s) => {
                // Try and deserialize the response as a work unit.
                let s = std::str::from_utf8(s)?;
                let job: Job = serde_json::from_str(s)?;
                Some(job)
            }

            // Might get errors.
            Frame::Error(e) => return Err(Error::ServerError(e.clone())),

            // Other unexpected responses.
            _ => {
                return Err(Error::UnexpectedResponse(
                    "Null or work unit".to_owned(),
                    format!("{:?}", res),
                ))
            }
        };
        Ok(job)
    }

    /// On successful job completion an `Ack` has to be sent with the worker_id.
    pub(crate) async fn ack(&mut self, job_id: &str) -> Result<(), Error> {
        let ack = Ack::new(job_id);
        self.conn.write_json(&ack).await?;
        self.read_ok().await
    }

    /// On a failed job `Fail` should be sent so the job can be rescheduled.
    pub(crate) async fn fail(&mut self, fail: &Fail<'_>) -> Result<(), Error> {
        self.conn.write_json(fail).await?;
        self.read_ok().await
    }

    /// Sends a heartbeat to the server.
    pub(crate) async fn beat(&mut self, worker_id: &str) -> Result<(), Error> {
        let beat = Beat { worker_id };
        self.conn.write_json(&beat).await?;

        // After a beat we expect either an OK or a change in state.
        let res = self.conn.read_frame().await?.ok_or(Error::NoResponse)?;

        match &res {
            // We can receive an OK and that's it we are done.
            Frame::Simple(s) if s == "OK" => Ok(()),

            // We can receive a state change as well. So we try to deserialize that.
            Frame::Simple(s) => {
                let state_change: StateChange = serde_json::from_str(s.as_str()).map_err(|_e| {
                    Error::UnexpectedResponse("Ok or state change".to_owned(), format!("{:?}", res))
                })?;
                self.state_change(state_change)?;

                Ok(())
            }

            // Handle error responses.
            Frame::Error(e) => Err(Error::ServerError(e.clone())),

            // And unexpected responses.
            _ => Err(Error::UnexpectedResponse(
                "Ok or state change".to_owned(),
                format!("{:?}", res),
            )),
        }
    }

    async fn exchange_hello(&mut self, info: Option<ConnectInfo<'_>>, password: &str) -> Result<(), Error> {
        // Get the Hi message.
        let frame = self.conn.read_frame().await?.ok_or(Error::NoResponse)?;
        let err = || Error::UnexpectedResponse("HI".to_owned(), format!("{:?}", frame));
        let hi = match &frame {
            Frame::Simple(s) => {
                let hi = Hi::from_str(s)?.ok_or_else(err)?;

                // Check that the expected protocol matches.
                if hi.version != PROTOCOL_VERSION {
                    return Err(Error::UnexpectedProtocol(hi.version, PROTOCOL_VERSION));
                }
                hi
            }
            Frame::Error(e) => return Err(Error::ServerError(e.clone())),
            _ => return Err(err()),
        };

        let mut hello = Hello::new();
        if let Some(info) = info {
            hello.hostname = Some(info.hostname);
            hello.worker_id = Some(info.worker_id);
            hello.pid = Some(info.pid);
        }

        if let Some(nonce) = &hi.nonce {
            let iterations = match hi.iterations {
                Some(iters) => iters,
                None => 1,
            };
            hello.set_password(password, nonce, iterations);
        }

        // Send Hello.
        self.conn.write_json(&hello).await?;

        // From this an OK is expected.
        self.read_ok().await?;

        Ok(())
    }

    /// Reads a frame and try to parse it as an OK. Returns errors sent back from the server as `Error::ServerError`.
    async fn read_ok(&mut self) -> Result<(), Error> {
        let frame = self.conn.read_frame().await?.ok_or(Error::NoResponse)?;
        match &frame {
            Frame::Simple(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(Error::ServerError(e.clone())),
            _ => Err(Error::UnexpectedResponse(
                "OK".to_owned(),
                format!("{:?}", frame),
            )),
        }
    }

    /// Updates the internal state of the connection based on `StateChange`. Can fail if the
    /// new state is not a valid state.
    fn state_change(&mut self, state_change: StateChange) -> Result<(), Error> {
        let new_state = State::from_state_change(&state_change);
        let new_state = new_state.ok_or(Error::UnexpectedResponse(
            "state object".to_owned(),
            state_change.state,
        ))?;
        self.state = new_state;
        Ok(())
    }
}

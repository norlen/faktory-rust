use super::{Job, PROTOCOL_VERSION};
use serde::Serialize;
use sha2::{Sha256, Digest};

/// Trait for all commands, mostly used to prevent small errors
/// from occuring when having to type the command names, and to
/// keep them all in one place.
pub(crate) trait Command {
    const NAME: &'static str;
}

/// Response sent to the server's `Hi` message.
#[derive(Debug, Serialize)]
pub(crate) struct Hello<'a> {
    /// The protocol version the client expects.
    /// The only required field. On a producer the `Hello` can be as simple as this.
    #[serde(rename = "v")]
    pub(crate) version: usize,

    /// The worker id is a unique string for each worker.
    #[serde(rename = "wid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) worker_id: Option<&'a str>,

    /// Hostname for this worker, useful for debugging in the web ui.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hostname: Option<&'a str>,

    /// Pid for this worker, useful for debugging in the web ui.
    #[serde(rename = "pid")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pid: Option<usize>,

    /// Application specific labels, shown in the web ui.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) labels: Option<Vec<&'a str>>,

    /// Hash used to authenticate each connection.
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl<'a> Hello<'a> {
    /// Creates a new empty `Hello` with the expected protocol version. The rest of 
    /// the fields have to be set manually, with the exception of the password hash,
    /// which uses `set_password`.
    pub(crate) fn new() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            worker_id: None,
            hostname: None,
            pid: None,
            labels: None,
            password_hash: None,
        }
    }

    /// Calculates the hash and sets the password for `Hello`.
    pub(crate) fn set_password(&mut self, password: &str, nonce: &str, iterations: usize) {
        self.password_hash = Some(Hello::get_password(password, nonce, iterations));
    }

    fn get_password(password: &str, nonce: &str, iterations: usize) -> String {
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        hasher.update(nonce.as_bytes());
        let mut hash = hasher.finalize();

        // We've already hashed it once now, so skip one iteration.
        for _ in 1..iterations {
            hash = Sha256::digest(hash.as_slice());
        }
        format!("{:x}", hash)
    }
}

impl<'a> Command for Hello<'a> {
    const NAME: &'static str = "HELLO";
}

/// Clears Faktory's internal database.
#[derive(Debug)]
pub(crate) struct Flush {}

impl Command for Flush {
    const NAME: &'static str = "FLUSH";
}

/// Get stats about the server.
#[derive(Debug)]
pub(crate) struct Info {}

impl Command for Info {
    const NAME: &'static str = "INFO";
}

/// Signal to the server that the worker wants to terminate the connection.
/// Should never be sent if any jobs are reservered, these have to be `FAIL`ed first.
/// The server responds with an `OK`, then it does not expect any more messages from
/// the worker.
#[derive(Debug)]
pub(crate) struct End {}

impl Command for End {
    const NAME: &'static str = "END";
}

/// Enqueues a job at the server. Server response with `OK` or `Error`.
#[derive(Debug, Serialize)]
pub(crate) struct Push<'a>(&'a Job);

impl<'a> Push<'a> {
    pub(crate) fn new(job: &'a Job) -> Self {
        Self(job)
    }
}

impl<'a> Command for Push<'a> {
    const NAME: &'static str = "PUSH";
}

/// Workers requests jobs using the fetch command, if no queues are passed it
/// will get jobs from the default queue. If no jobs are found the server will
/// block the call for up to two seconds on the first queue provided. If a job
/// is returning it must be either `Ack`ed or `Fail`ed for the `job_id` returned.
#[derive(Debug)]
pub(crate) struct Fetch<'a> {
    queues: Option<&'a str>,
}

impl<'a> Command for Fetch<'a> {
    const NAME: &'static str = "FETCH";
}

/// The result for each job is either an `Ack` or a `Fail`.
/// Sending an `Ack` means the job succeeded. Returns either an `OK` or
/// an `Error`.
#[derive(Debug, Serialize)]
pub(crate) struct Ack<'a> {
    #[serde(rename = "jid")]
    job_id: &'a str,
}

impl<'a> Ack<'a> {
    pub(crate) fn new(job_id: &'a str) -> Self {
        Self { job_id }
    }
}

impl<'a> Command for Ack<'a> {
    const NAME: &'static str = "ACK";
}

/// The result for each job is either an `Ack` or a `Fail`.
/// Sending a `Fail` means the job failed and can provide additional information
/// to the web ui
///
/// # Responses
/// Returns either an `OK` or an `Error`.
#[derive(Debug, Serialize)]
pub(crate) struct Fail<'a> {
    /// Id for the failed job.
    #[serde(rename = "jid")]
    pub(crate) job_id: &'a str,

    /// Error that prevent the completion of the job.
    #[serde(rename = "errtype")]
    pub(crate) error_type: &'a str,

    /// Short description of the occured error.
    pub(crate) message: &'a str,

    /// Longer multi-line backtrace for the error.
    pub(crate) backtrace: Vec<&'a str>,
}

impl<'a> Command for Fail<'a> {
    const NAME: &'static str = "FAIL";
}

/// All workers must send a heartbeat every N seconds, after 60 seconds have
/// passed Faktory will remove them.
///
/// # Responses
/// Can either just respond with a simple `OK`, in addition it can respond with the
/// json `{state: String}` for a server intitiated state change. In addition an `Error`
/// can be returned for a malformed och rejected request.
#[derive(Debug, Serialize)]
pub(crate) struct Beat<'a> {
    #[serde(rename = "wid")]
    pub(crate) worker_id: &'a str,
}

impl<'a> Command for Beat<'a> {
    const NAME: &'static str = "BEAT";
}

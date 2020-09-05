use serde::{Serialize, Deserialize};
use serde_json::Value;
use chrono::{DateTime, Utc};

/// Represents the job data that gets passed to/from the server.
#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    /// Unique ID for this job.
    #[serde(rename = "jid")]
    pub id: String,

    /// Worker uses the jobtype to determine how to execute the job.
    #[serde(rename = "jobtype")]
    pub kind: String,

    /// Arguments passed to this job, only JSON native values. i.e. they have be
    /// of string, number, boolean, map, array or null.
    pub args: Vec<Value>,

    /// The queue to push to job onto.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// How long the worker reserves the job, after which is should ACK of FAIL the job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserve_for: Option<u64>,

    /// When this job should be run, the string has to be formatted in Go's RFC3339Nano time format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,

    /// The number of retries if the job fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<i64>,

    /// How many lines to retain in a backtrace if the job fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<i64>,

    /// When the job was created. Either the client passes it or faktory sets it upon receiving the job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    /// Faktory server sets this when enqueueing the job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enqueued_at: Option<DateTime<Utc>>,

    /// Hash with data about the job's most recent failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Custom data associated with the job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<std::collections::HashMap<String, serde_json::Value>>,
}

impl Job {
    /// Creates a new job with a generated id and the passed handler kind required for processing.
    /// If no queue is passed the "default" queue is used.
    pub fn new<T: Into<String>>(kind: T) -> Self {
        let id = uuid::Uuid::new_v4().to_string();

        Self {
            id,
            kind: kind.into(),
            args: Vec::new(),
            queue: None,
            reserve_for: None,
            at: None,
            retry: None,
            backtrace: None,
            created_at: None,
            enqueued_at: None,
            failure: None,
            custom: None,
        }
    }

    /// Creates a new job with a generated id. `kind` is the handler required to process the job,
    /// and args are later passed to the handler. If no queue is passed the "default" queue is used.
    pub fn with_args<T, A>(kind: T, args: Vec<A>) -> Self
    where
        T: Into<String>,
        A: Into<Value>,
    {
        let id = uuid::Uuid::new_v4().to_string();

        Self {
            id,
            kind: kind.into(),
            args: args.into_iter().map(|a| a.into()).collect(),
            queue: None,
            reserve_for: None,
            at: None,
            retry: None,
            backtrace: None,
            created_at: None,
            enqueued_at: None,
            failure: None,
            custom: None,
        }
    }

    /// Set the time at which the job should run at.
    pub fn at(&mut self, at: DateTime<Utc>) -> &mut Self {
        self.at = Some(at);
        self
    }

    /// Sets the queue the job should be put onto at the server.
    pub fn queue<T: Into<String>>(&mut self, queue: T) -> &mut Self {
        self.queue = Some(queue.into());
        self
    }
}

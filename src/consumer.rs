use crate::{
    protocol::{commands, Client, Job, State, ConnectInfo},
    Error,
};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::{
    sync::mpsc::{self, error::TryRecvError},
    time::{self, Duration, timeout},
};

/// The interval at which heartbeats are sent.
const BEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Default time at which a job is reserved for, it has to completed within this timeframe.
const DEFAULT_RESERVED_FOR_TIME: u64 = 1800;

/// Handler that processes a job.
pub(crate) type HandlerFn<S, E> =
    Arc<dyn Fn(Job, S) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>> + Send + Sync>;

/// Type the mpsc::channel expects so the result of the computation of the work unit
/// can be reported back to the server. It first holds the `job_id` and then the result
/// from the handler.
type HandlerResult<E> = (String, Result<(), RunError<E>>);

/// Default hostname if none is supplied.
const DEFAULT_HOSTNAME: &'static str = "localhost";

/// Default queue is none are supplied.
const DEFAULT_QUEUE: &'static str = "default";

/// Error encapsulating task timeouts.
#[derive(Debug, Error)]
enum RunError<E>
where
    E: std::error::Error + Send + 'static,
{
    #[error("Job {} timed out after {} seconds", .0, .1)]
    Timeout(String, u64),

    #[error("{}", .0)]
    Handler(#[from] E)
}

/// `Consumer` fetches and runs the jobs that a `Producer` can push to the server.
pub struct Consumer<S, E>
where
    S: Clone + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// Client used to communicate with the faktory server.
    client: Client,

    /// The worker's id.
    worker_id: String,

    /// The job queues this worker fetches jobs from.
    queues: String,

    /// All the handlers.
    handlers: HashMap<String, HandlerFn<S, E>>,

    /// State that gets passed to each handler.
    state: S,
}

impl<S, E> Consumer<S, E>
where
    S: Clone + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// Returns a builder to create the `Consumer`.
    pub fn builder() -> Builder<S, E> {
        Builder::new()
    }

    /// Connects to the Faktory server.
    pub async fn connect(
        addr: &str,
        hostname: String,
        worker_id: String,
        pid: usize,
        queues: String,
        state: S,
        password: Option<&str>,
    ) -> Result<Self, Error> {
        let info = ConnectInfo {
            hostname: hostname.as_str(),
            worker_id: worker_id.as_str(),
            pid,
        };
        let client = Client::connect(addr, Some(info), password).await?;

        Ok(Self {
            client,
            worker_id,
            handlers: HashMap::new(),
            queues,
            state,
        })
    }

    /// Register a new handler used to process jobs.
    pub fn register<F>(&mut self, kind: String, f: fn(Job, S) -> F)
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        let f = Arc::new(move |job, state| fix_async_fn(f, job, state));
        self.handlers.insert(kind, f);
    }

    /// Try and fetch and process a single job. Does not send any heartbeats.
    pub async fn run_once(&mut self) -> Result<Option<bool>, Error> {
        if self.client.state != State::Identified {
            return Ok(None);
        }

        // Fetch jobs.
        if let Some(job) = self.client.fetch(&self.queues).await.unwrap() {
            let job_id = job.id.clone();
            let result = self.process_job(job).await?.map_err(|e| e.into());
            self.send_result((job_id, result)).await?;
            Ok(Some(true))
        } else {
            Ok(Some(false))
        }
    }

    /// Fetches and processing jobs forever. Sends heartbeats every `BEAT_INTERVAL` which is set to 15 seconds.
    /// Also checks for state changes and does not fetch any more jobs after being put in a state other than
    /// `Identified`. All processed jobs are spawned as tasks and are cancelled after the `reserve_for` time
    /// has passed. Returns when put into the `End` state.
    pub async fn run(&mut self) -> Result<(), Error> {
        let mut last_beat = time::Instant::now();
        let (ret_tx, mut ret_rx) = mpsc::channel(32);

        // TODO: We have to send End somewhere, this mostly depends on the current state (or if we want to exit).
        // We could maybe create a oneshot channel, that we communicate with the client on, and using this we can
        // spawn a task after we receive `Terminating` and then try to fail all the current task and then send an
        // end to the server (after max 30 secs).

        while self.client.state != State::End {

            // Send heartbeat if neccessary.
            if last_beat.elapsed() > BEAT_INTERVAL
                && (self.client.state == State::Identified || self.client.state == State::Quiet)
            {
                self.client.beat(&self.worker_id).await?;
                last_beat = time::Instant::now();
            }

            // Send results for all finished computations.
            loop {
                match ret_rx.try_recv() {
                    Ok(result) => self.send_result(result).await?,
                    Err(e) => match e {
                        TryRecvError::Empty => break,
                        TryRecvError::Closed => todo!(),
                    },
                }
            }

            // Fetch jobs.
            if self.client.state == State::Identified {
                if let Some(job) = self.client.fetch(&self.queues).await.unwrap() {
                    self.spawn_job(job, ret_tx.clone()).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_result(&mut self, (job_id, result): HandlerResult<E>) -> Result<(), Error> {
        match result {
            // If the work unit completed successfully we sent back `Ack`.
            Ok(_) => {
                self.client.ack(&job_id).await?;
            }

            // If the work unit failed we send back `Fail` with error information.
            Err(e) => {
                let fail = commands::Fail {
                    job_id: &job_id,
                    error_type: "JobError",
                    message: &e.to_string(),
                    backtrace: Vec::new(),
                };
                self.client.fail(&fail).await?;
            }
        }
        Ok(())
    }

    /// Spawn a task executing the job, sending back the result to the mpsc channel so it can be send back
    /// to the server.
    async fn spawn_job(
        &self,
        job: Job,
        mut result_tx: mpsc::Sender<HandlerResult<E>>,
    ) -> Result<(), Error> {
        // TODO: The tasks we spawn should probably be cancellable by us. This will allow us
        // to fail jobs if we get put in a terminating state by the server.

        if let Some(handler) = self.handlers.get(&job.kind).cloned() {
            let state = self.state.clone();
            tokio::spawn(async move {
                let job_id = job.id.clone();
                let reserve_for = job.reserve_for.unwrap_or(DEFAULT_RESERVED_FOR_TIME);
                let job_to_run = process_job(handler, job, state);

                let result = timeout(Duration::from_secs(reserve_for), job_to_run).await;
                let result = match result {
                    Ok(res) => res.map_err(|e| e.into()),
                    Err(_) => Err(RunError::Timeout(job_id.clone(), reserve_for)),
                };

                // Send back result.
                result_tx.send((job_id, result)).await.unwrap();
            });
            Ok(())
        } else {
            Err(Error::NoHandler(job.kind.clone()))
        }
    }

    /// Runs a given handler for a job if it is registered. If it succeeds it returns back
    /// with the result from the processed job.
    async fn process_job(&self, job: Job) -> Result<Result<(), E>, Error> {
        let jobtype = job.kind.clone();
        let f = self
            .handlers
            .get(&job.kind)
            .map(|h| run(h, self.state.clone(), job));

        if let Some(handler_fn) = f {
            Ok(handler_fn.await)
        } else {
            Err(Error::NoHandler(jobtype))
        }
    }
}

/// Runs a given handler for a job if it is registered.
async fn process_job<S, E>(handler: HandlerFn<S, E>, job: Job, state: S) -> Result<(), E>
where
    S: Clone + 'static,
    E: std::error::Error + Send,
{
    let handler = run(&handler, state, job);
    handler.await
}

// Runs a single
async fn run<S, E>(handler_fn: &HandlerFn<S, E>, state: S, job: Job) -> Result<(), E> {
    let id = job.id.clone();
    let name = job.kind.clone();

    let start = time::Instant::now();
    let res = handler_fn(job, state).await;
    let stop = start.elapsed();
    let stop_secs = stop.as_secs() * 1_000_000_000 + u64::from(stop.subsec_nanos());
    let stop_secs = stop_secs as f64 / 1_000_000_000.0;

    match res {
        Ok(_) => {
            // println!("Job {} {} completed in {:.2} seconds", id, name, stop_secs);
        }
        Err(_) => println!("Job {} {} failed in {:.2} seconds", id, name, stop_secs),
    }
    res
}

// Converts an async fn to something we can store in our map of handlers.
fn fix_async_fn<F, S, E>(
    run: fn(Job, S) -> F,
    job: Job,
    state: S,
) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>>
where
    S: Clone + Send + 'static,
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: std::error::Error,
{
    Box::pin(async move {
        let res = run(job, state);
        res.await?;

        Ok(())
    })
}

pub struct Builder<S, E>
where
    S: Clone + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    address: Option<String>,
    state: Option<S>,
    hostname: Option<String>,
    worker_id: Option<String>,
    pid: Option<usize>,
    queues: Vec<String>,
    password: Option<String>,
    phantom_data: std::marker::PhantomData<E>,
}

impl<S, E> Builder<S, E>
where
    S: Clone + Send + 'static,
    E: std::error::Error + Send + 'static,
{
    fn new() -> Self {
        Self {
            address: None,
            state: None,
            hostname: None,
            worker_id: None,
            pid: None,
            queues: Vec::new(),
            password: None,
            phantom_data: std::marker::PhantomData::default(),
        }
    }

    pub fn address<T: Into<String>>(mut self, address: T) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn state(mut self, state: S) -> Self {
        self.state = Some(state);
        self
    }

    pub fn hostname<T: Into<String>>(mut self, hostname: T) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    pub fn worker_id<T: Into<String>>(mut self, worker_id: T) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn pid(mut self, pid: usize) -> Self {
        self.pid = Some(pid);
        self
    }

    pub fn queue<T: Into<String>>(mut self, queue: T) -> Self {
        self.queues.push(queue.into());
        self
    }

    pub fn password<T: Into<String>>(mut self, password: T) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn queues(mut self, queues: Vec<String>) -> Self {
        if !self.queues.is_empty() {
            eprintln!(
                "Overwriting non-empty queue {:?} with {:?}",
                self.queues, queues
            );
        }
        self.queues = queues;
        self
    }

    pub async fn connect(self) -> Result<Consumer<S, E>, Error> {
        let address = self.address.or_else(try_get_addr).ok_or(Error::NoAddress)?;
        let state = self.state.ok_or(Error::NoState)?;

        let hostname = self
            .hostname
            .or_else(try_get_hostname)
            .unwrap_or_else(|| DEFAULT_HOSTNAME.to_owned());
        let worker_id = self.worker_id.unwrap_or_else(get_worker_id);
        let pid = self.pid.unwrap_or_else(get_pid);

        let queues = if self.queues.is_empty() {
            DEFAULT_QUEUE.to_owned()
        } else {
            self.queues
                .iter()
                .fold(String::new(), |acc, s| format!("{} {}", acc, s))
        };

        let password = self.password.as_ref().map(|s| s.as_str());
        Consumer::connect(&address, hostname, worker_id, pid, queues, state, password).await
    }
}

fn try_get_addr() -> Option<String> {
    std::env::var("FAKTORY_URL").ok()
}

fn try_get_hostname() -> Option<String> {
    let hostname = hostname::get().ok()?;
    hostname.into_string().ok()
}

fn get_worker_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn get_pid() -> usize {
    std::process::id() as usize
}

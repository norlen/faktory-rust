# faktory-rust

Async API bindings for [Faktory](https://github.com/contribsys/faktory/), relies on [tokio](https://tokio.rs/). Has not really been tested, so probably
should not really be used.

## Examples

Push jobs using a `Producer` and then using a `Consumer` spawn a task that continually fetches jobs from the server

```rust
use faktory_async::{Producer, Consumer, Job};
use thiserror::Error;

#[derive(Clone)]
struct MyState {
    var: u64,
}

#[derive(Debug, Error)]
enum MyError {}

async fn job_handler(job: Job, _state: MyState) -> Result<(), MyError> {
    let my_job = MyJob::from_args(&job.args).unwrap();
    println!("arg0: {}, arg1: {}", my_job.arg0, my_job.arg1);
    Ok(())
}

struct MyJob {
    arg0: usize,
    arg1: String,
}

impl MyJob {
    fn to_args(&self) -> Result<Vec<serde_json::Value>, serde_json::Error> {
        let mut args = Vec::new();
        args.push(serde_json::to_value(self.arg0)?);
        args.push(serde_json::to_value(&self.arg1)?);
        Ok(args)
    }

    fn from_args(args: &Vec<serde_json::Value>) -> Result<Self, serde_json::Error> {
        let arg0 = serde_json::from_value(args[0].clone())?;
        let arg1 = serde_json::from_value(args[1].clone())?;
        Ok(Self {
            arg0,
            arg1,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let faktory_addr = "127.0.0.1:7419";
    let mut producer = Producer::connect(faktory_addr).await?;
    let mut consumer = Consumer::builder()
        .address(faktory_addr)
        .state(MyState { var: 1})
        //.hostname("")             // Optional: defaults to current machines hostname.
        //.worker_id("")            // Optional: defaults to random id.
        //.pid(0)                   // Optional: defaults to process id.
        //.queue("")                // Optional: defaults to "default".
        .connect()
        .await?;
    
    consumer.register("handler_name".to_owned(), job_handler);
    
    // `run_once` can also be used, it only tries to fetch one job, returns Ok(true) in it can fetch
    // and complete the job, otherwise Ok(false) if no job can be found.
    let c = tokio::spawn(async move {
        consumer.run().await.unwrap();
    });

    let my_job = MyJob {
        arg0: 123,
        arg1: "Hello World!".to_owned(),
    };
    let args = my_job.to_args()?;
    let job = Job::with_args("handler_name".to_owned(), args);
    producer.push(job).await?;

    println!("consumer.run() will run indefinetely so the program won't exit.");
    tokio::join!(c).0?;
    Ok(())
}
```

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
        //.hostname("my_hostname")
        //.worker_id("my_worker_id")
        //.pid(123)
        //.queue("default")
        .connect().await?;
    
    consumer.register("test".to_owned(), job_handler);
    
    let c = tokio::spawn(async move {
        consumer.run().await.unwrap();
    });

    let my_job = MyJob {
        arg0: 123,
        arg1: "Hello World!".to_owned(),
    };
    let args = my_job.to_args()?;
    let job = Job::with_args("test".to_owned(), args);
    producer.push(job).await?;

    println!("consumer.run() will run indefinetely so the program won't exit.");
    tokio::join!(c).0?;
    Ok(())
}

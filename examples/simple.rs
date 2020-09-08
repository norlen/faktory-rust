use faktory_async::{Consumer, Job, Producer};
use thiserror::Error;

#[derive(Clone)]
struct MyState {
    var: u64,
}

#[derive(Debug, Error)]
enum MyError {}

async fn job_handler(_job: Job, _state: MyState) -> Result<(), MyError> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let faktory_addr = "127.0.0.1:7419";
    let mut producer = Producer::connect(faktory_addr).await?;
    let mut consumer = Consumer::builder()
        .address(faktory_addr)
        .state(MyState { var: 1 })
        //.hostname("my_hostname")
        //.worker_id("my_worker_id")
        //.pid(123)
        //.queue("default")
        .connect()
        .await?;

    consumer.register("test".to_owned(), job_handler);

    let c = tokio::spawn(async move {
        consumer.run().await.unwrap();
    });

    let job = Job::new("test".to_owned());
    producer.push(job).await?;

    // consumer.run will run indefinetely so the program won't exit.
    tokio::join!(c).0?;
    Ok(())
}

use crate::{
    protocol::{Client, Job},
    Error,
};

/// `Producer` can send jobs to the server which a consumer can later handle.
pub struct Producer {
    client: Client,
}

impl Producer {
    /// Connects to a server using only the address. If a password is required use `connect_with_password` instead.
    pub async fn connect(addr: &str) -> Result<Self, Error> {
        let client = Client::connect(addr, None, None).await?;
        Ok(Self { client })
    }

    /// Connects to a password protected server.
    pub async fn connect_with_password(addr: &str, password: &str) -> Result<Self, Error> {
        let client = Client::connect(addr, None, Some(password)).await?;
        Ok(Self { client })
    }

    /// Tries to push a job to the server.
    pub async fn push(&mut self, job: Job) -> Result<(), Error> {
        self.client.push(&job).await?;
        Ok(())
    }

    /// Gets information and stats about the server.
    pub async fn info(&mut self) -> Result<String, Error> {
        let info = self.client.info().await?;
        Ok(info)
    }

    /// Clears the server database. All jobs previously sent will be removed.
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.client.flush().await?;
        Ok(())
    }
}

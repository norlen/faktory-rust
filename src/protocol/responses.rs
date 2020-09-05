use serde::Deserialize;

/// On intial connection Faktory sends a `Hi` message.
///
/// If there is not a password set only the version is sent.
/// Otherwise the nonce and the iterations are sent.
#[derive(Debug, Deserialize)]
pub(crate) struct Hi {
    /// The protocol version. Always sent.
    #[serde(rename = "v")]
    pub(super) version: usize,

    /// Is set on a password enabled server.
    #[serde(rename = "s")]
    pub(super) nonce: Option<String>,

    /// Is set on a password enabled server.
    #[serde(rename = "i")]
    pub(super) iterations: Option<usize>,
}

impl Hi {
    /// Try to deserialize a response into a `Hi`. Returns Ok(None) if the response does
    /// not start with "HI", if it starts then it tries to deserialize.
    pub fn from_str(data: &str) -> Result<Option<Self>, serde_json::Error> {
        if !data.starts_with("HI") {
            return Ok(None);
        }
        let de: Hi = serde_json::from_str(&data[3..])?;
        Ok(Some(de))
    }
}

/// On `Beat`s the server can respond with a state change.
#[derive(Debug, Deserialize)]
pub(crate) struct StateChange {
    state: String,
}

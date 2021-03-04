use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KafcatError {
    #[error("Timeout error")]
    Timeout,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

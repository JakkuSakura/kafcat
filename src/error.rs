use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KafcatError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

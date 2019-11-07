use std::io;
use std::result;
use std::sync::mpsc::SendError;
use std::sync::PoisonError;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    DBCorruptionError,
    BackgroundFlushError,
    DBNameInvalidError,
    SyncPoisonError,
    SendError,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_err: PoisonError<T>) -> Error {
        Error::SyncPoisonError
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(_err: SendError<T>) -> Error {
        Error::SendError
    }
}

pub type Result<T> = result::Result<T, Error>;

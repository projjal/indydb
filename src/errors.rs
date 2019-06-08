use std::result;
use std::io;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    DBCorruptionError,
    BackgroundFlushError,
    DBNameInvalidError,
}

impl From<io::Error> for Error {
    fn from (err: io::Error) -> Error {
        Error::IOError(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

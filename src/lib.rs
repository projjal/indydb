extern crate byteorder;

pub mod db;
pub mod params;
mod errors;
mod memtable;
mod table;

pub use db::DB;
pub use params::DBParams;
pub use errors::{Error, Result};


// TODO: write tests
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

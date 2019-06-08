extern crate byteorder;

mod db;
mod errors;
mod memtable;
mod table;
mod params;

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

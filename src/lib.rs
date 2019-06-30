extern crate byteorder;

pub mod db;
pub mod params;
mod errors;
mod memtable;
mod table;

pub use db::DB;
pub use params::DBParams;
pub use errors::{Error, Result};


#[cfg(test)]
mod tests {
    use crate::db::DB;
    use crate::params::DBParams;
    use std::{str, fs};

    fn delete_db(db_name: &str) {
        fs::remove_dir_all(db_name).unwrap();
    }

    #[test]
    fn test_db_open() {
        let db_params = DBParams::new();
        let db_name = "target/testdb";
        let mut db = DB::open(&db_name, db_params).unwrap();
        db.put("test", "value").unwrap();
        let val = db.get("test").unwrap().unwrap();
        assert_eq!(str::from_utf8(&val).unwrap(), "value");
        db.close().unwrap();
        delete_db(&db_name);
    }

    #[test]
    fn test_db_get_and_put() {
        let db_params = DBParams::new();
        let db_name = "target/testdb";
        let mut db = DB::open(&db_name, db_params).unwrap();

        for i in 0..100 {
            db.put(i.to_string(), i.to_string()).unwrap();
        }
        for i in 0..100 {
            let val = db.get(i.to_string()).unwrap().unwrap();
            assert_eq!(str::from_utf8(&val).unwrap(), i.to_string());
        }

        db.close().unwrap();
        delete_db(&db_name);
    }
}

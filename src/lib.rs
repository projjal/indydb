extern crate byteorder;

pub mod db;
pub mod errors;
mod memtable;
pub mod params;
mod table;

pub use db::DB;
pub use errors::{Error, Result};
pub use params::DBParams;

#[cfg(test)]
mod tests {
    use crate::db::DB;
    use crate::params::DBParams;
    use std::{fs, str};

    fn delete_db(db_name: &str) {
        fs::remove_dir_all(db_name).unwrap();
    }

    #[test]
    fn test_db_open() {
        let db_params = DBParams::new();
        let db_name = "target/testdb1";
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
        let db_name = "target/testdb2";
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

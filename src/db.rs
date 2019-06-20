use std::fs;
use std::thread;
use std::thread::JoinHandle;
use std::io::Cursor;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::sync::Arc;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::table::{TableBuilder, Table};
use crate::memtable::{MemTable, MemValue};
use crate::params::DBParams;
use crate::errors::{Result, Error};

macro_rules! get_mem_value {
    ($e:expr) => {
        match $e {
            Some(MemValue::Value(value)) => return Ok(Some(value.clone())),
            Some(MemValue::Delete) => return Ok(None),
            _ => ()
        };
    };
}

pub struct DB {
    db_name : String,
    mem_table : MemTable,
    flush_table : Arc<Option<MemTable>>,
    db_params : DBParams,
    files : u64,
    flush_thread_handle: Option<JoinHandle<()>>,
}

impl DB {
    pub fn open(db_name : &str, db_params: DBParams) -> Result<DB> {
        let num_files = DB::create_db_dir(db_name)?;
        let db = DB {
            db_name : String::from(db_name),
            mem_table : MemTable::new(),
            flush_table : Arc::new(None),
            db_params : db_params,
            files : num_files,
            flush_thread_handle : None,
        };
        Ok(db)
    }

    fn create_db_dir(db_name: &str) -> Result<u64> {
        let path = Path::new(db_name);
        let is_path_exists = path.exists();
        let num_files = if is_path_exists && path.is_dir() {
            if let Ok(mut file) = File::open(format!("{}/METADATA", db_name)) {
                let mut buf = [0;8];
                file.read(&mut buf)?;
                Cursor::new(buf).read_u64::<BigEndian>()?
            } else {
                return Err(Error::DBNameInvalidError);
            }
        } else if is_path_exists {
            return Err(Error::DBNameInvalidError);
        } else {
            fs::create_dir_all(db_name)?;
            0
        };
        Ok(num_files)
    }

    pub fn get<S: Into<Vec<u8>>>(&self, key : S) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.into();
        get_mem_value!(self.mem_table.get(&key_bytes));

        if let Some(ref table) = *self.flush_table {
            get_mem_value!(table.get(&key_bytes));
        }

        for i in 0..self.files {
            let table = Table::open(&self.db_name, self.files-i-1)?;
            let val = table.get(&key_bytes)?;
            get_mem_value!(val);
        }
        Ok(None)
    }

    pub fn put<S: Into<Vec<u8>>>(&mut self, key: S, value: S) -> Result<()> {
        self.mem_table.put(key.into(), value.into())?;
        if self.mem_table.size() >= self.db_params.write_buffer_size {
            self.start_flushing()?;
        }
        Ok(())
    }

    pub fn delete<S: Into<Vec<u8>>>(&mut self, key: S) -> Result<()> {
        self.mem_table.delete(key.into())?;
        if self.mem_table.size() >= self.db_params.write_buffer_size {
            self.start_flushing()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let flush_thread_handle_option = self.flush_thread_handle.take();
        if let Some(_flush_thread_handle) = flush_thread_handle_option {
            if let Err(_) = _flush_thread_handle.join() {
                return Err(Error::BackgroundFlushError);
            }
        };
        Ok(())
    }

    fn start_flushing(&mut self) -> Result<()>{
        let flush_thread_handle_option = self.flush_thread_handle.take();
        if let Some(_flush_thread_handle) = flush_thread_handle_option {
            if let Err(_) = _flush_thread_handle.join() {
                return Err(Error::BackgroundFlushError);
            }
            self.files += 1;
        };
        let old_mem_table = std::mem::replace(&mut self.mem_table, MemTable::new());
        self.flush_table = Arc::new(Some(old_mem_table));
        let _flush_table = Arc::clone(&self.flush_table);
        let _db_name = self.db_name.clone();
        let mut _table_builder = TableBuilder::new(_db_name.clone(), self.files);
        let thread_handle = thread::spawn(move || {
            if let Some(ref inner_table) = *_flush_table {
                for (key,value) in &inner_table.table {
                    _table_builder.add(key, value).unwrap();
                }
                _table_builder.flush().unwrap();
                let mut num_files = Vec::with_capacity(8);
                num_files.write_u64::<BigEndian>(_table_builder.file_no()).unwrap();
                File::create(format!("{}/METADATA",_db_name)).unwrap().write_all(&num_files).unwrap();
            }
        });
        self.flush_thread_handle = Some(thread_handle);
        Ok(())
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.close().expect("Failed to safely close the db");
    }
}
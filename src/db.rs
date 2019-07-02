use std::fs;
use std::thread;
use std::thread::JoinHandle;
use std::io::Cursor;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::sync::{Arc, Mutex, RwLock, Condvar};
use std::sync::mpsc::{Sender, Receiver, TryRecvError};
use std::sync::mpsc;
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
    flush_table : Arc<RwLock<Option<MemTable>>>,
    db_params : DBParams,
    files : Arc<RwLock<u64>>, 
    flush_thread_handle : Option<JoinHandle<Result<()>>>,
    cv_pair : Arc<(Mutex<bool>, Condvar)>,
    flush_thread_sender : Sender<()>,
}

impl DB {
    pub fn open(db_name : &str, db_params: DBParams) -> Result<DB> {
        let num_files = DB::create_db_dir(db_name)?;
        let _db_name = String::from(db_name);
        let _flush_table = Arc::new(RwLock::new(None));
        let _cv_pair = Arc::new((Mutex::new(false), Condvar::new()));
        let _files = Arc::new(RwLock::new(num_files));
        let (sender, receiver) = mpsc::channel();
        let join_handle = DB::start_flush_thread(receiver, _db_name.clone(), _flush_table.clone(), _cv_pair.clone(), _files.clone())?;
        
        let db = DB {
            db_name : _db_name,
            mem_table : MemTable::new(),
            flush_table : _flush_table,
            db_params : db_params,
            files : _files,
            flush_thread_handle : Some(join_handle),
            cv_pair : _cv_pair,
            flush_thread_sender : sender,
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

    fn start_flush_thread(
            receiver: Receiver<()>,
            db_name: String,
            flush_table: Arc<RwLock<Option<MemTable>>>,
            cv_pair: Arc<(Mutex<bool>, Condvar)>,
            db_files: Arc<RwLock<u64>>
        ) -> Result<JoinHandle<Result<()>>> {
                
        // background flush thread
        let thread_handle = thread::spawn(move || {
            let files = {
                let guard = db_files.read()?;
                *guard
            };
            let mut _table_builder = TableBuilder::new(&db_name, files);
            loop {

                let &(ref lock, ref cvar) = &*cv_pair;
                let mut to_flush = lock.lock()?;
                while !*to_flush {
                    to_flush = cvar.wait(to_flush)?;
                }

                match receiver.try_recv() {
                    Ok(_) | Err(TryRecvError::Disconnected) => {
                        break;
                    },
                    Err(TryRecvError::Empty) => ()
                };

                {
                    let guard = flush_table.read()?;
                    if let Some(ref inner_table) = *guard {
                        for (key,value) in &inner_table.table {
                            _table_builder.add(key, value)?;
                        }
                        _table_builder.flush()?;
                    
                        // update the metadata containing num of files
                        let mut num_files = Vec::with_capacity(8);
                        num_files.write_u64::<BigEndian>(_table_builder.file_no()).unwrap();
                        File::create(format!("{}/METADATA",db_name)).unwrap().write_all(&num_files).unwrap();

                        // update num_files property of db
                        let mut w_guard = db_files.write()?;
                        *w_guard += 1;
                    }
                }
                {
                    let mut guard = flush_table.write()?;
                    *guard = None;
                }

                *to_flush = false;
                cvar.notify_one();
            }

            Ok(())
        });
        Ok(thread_handle)
    }

    pub fn get<S: Into<Vec<u8>>>(&self, key : S) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.into();
        get_mem_value!(self.mem_table.get(&key_bytes));

        {
            let guard = self.flush_table.read()?;
            if let Some(ref table) = *guard {
                get_mem_value!(table.get(&key_bytes));
            }
        }

        {
            let guard = self.files.read()?;
            for i in 0..*guard {
                let table = Table::open(&self.db_name, *guard-i-1)?;
                let val = table.get(&key_bytes)?;
                get_mem_value!(val);
            }
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
        // db already closed
        if self.flush_thread_handle.is_none() {
            return Ok(())
        }

        self.start_flushing()?;

        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock()?;
            while *to_flush {
                to_flush = cvar.wait(to_flush)?;
            }
        }


        self.flush_thread_sender.send(())?;

        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock().unwrap();
            *to_flush = true;
            cvar.notify_one();
        }

        // join the flush_thread_handle
        let join_handle = self.flush_thread_handle.take();
        if let Err(_) = join_handle.unwrap().join() {
            return Err(Error::BackgroundFlushError);
        }
        Ok(())
    }

    fn start_flushing(&mut self) -> Result<()> {
        if self.mem_table.table.is_empty() {
            return Ok(());
        }
        
        // wait till the flush thread has finished flushing the last flush_table
        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock()?;
            while *to_flush {
                to_flush = cvar.wait(to_flush)?;  // writer blocks!! TODO: think of a better solution
            }
        }

        // replace the memtable with a new one and convert it to the flush_table
        let old_mem_table = std::mem::replace(&mut self.mem_table, MemTable::new());
        {
            let mut w_guard = self.flush_table.write()?;
            assert!(w_guard.is_none(), "flush_table is not none");
            *w_guard = Some(old_mem_table);
        }

        // signal the flush_thread to start flushing
        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock().unwrap();
            *to_flush = true;
            cvar.notify_one();
        }
        Ok(())
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.close().expect("Failed to safely close the db");
    }
}
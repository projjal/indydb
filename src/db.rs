use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::path::Path;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

use crate::errors::{Error, Result};
use crate::memtable::{MemTable, MemValue};
use crate::params::DBParams;
use crate::table::{Table, TableBuilder};

macro_rules! get_mem_value {
    ($e:expr) => {
        match $e {
            Some(MemValue::Value(value)) => return Ok(Some(value.clone())),
            Some(MemValue::Delete) => return Ok(None),
            _ => (),
        };
    };
}

pub struct DB {
    /// Name of the db
    db_name: String,
    /// In-memory table for storing key-value pairs.
    /// After mem_table reaches a specified size it is converted into flush_table
    mem_table: MemTable,
    /// Immutable table to be flushed to disk
    flush_table: Arc<RwLock<Option<MemTable>>>,
    /// DBParams object to tune the behaviour of the db
    db_params: DBParams,
    /// Number of log files belonging to the db.
    /// Contains log files numbered form 0 to <files-1>
    files: Arc<RwLock<u64>>,
    /// Join-handle of the background flush thread that flushes flush_table to disk
    flush_thread_handle: Option<JoinHandle<Result<()>>>,
    /// Condition Variable for synchronizing the flush_thread with the db thread, which waits till the flush_thread has finished flushing before converting the current mem_table to flush_table
    cv_pair: Arc<(Mutex<bool>, Condvar)>,
    /// Sender part of the channel that signals the flush_thread that db is closing
    flush_thread_sender: Sender<()>,
}

impl DB {
    /// Opens and loads a database
    pub fn open(db_name: &str, db_params: DBParams) -> Result<DB> {
        let num_files = DB::num_log_files(db_name, &db_params)?;
        let db_name = String::from(db_name);
        let flush_table = Arc::new(RwLock::new(None));
        let cv_pair = Arc::new((Mutex::new(false), Condvar::new()));
        let files = Arc::new(RwLock::new(num_files));
        let (sender, receiver) = mpsc::channel();
        let join_handle = DB::start_flush_thread(
            receiver,
            db_name.clone(),
            flush_table.clone(),
            cv_pair.clone(),
            files.clone(),
        )?;

        let db = DB {
            db_name,
            mem_table: MemTable::new(),
            flush_table,
            db_params,
            files,
            flush_thread_handle: Some(join_handle),
            cv_pair,
            flush_thread_sender: sender,
        };
        Ok(db)
    }

    // returns the number of log_files in the database
    fn num_log_files(db_name: &str, db_params: &DBParams) -> Result<u64> {
        let path = Path::new(db_name);
        let is_path_exists = path.exists();
        let num_files = if is_path_exists && path.is_dir() {
            if let Ok(mut file) = File::open(format!("{}/METADATA", db_name)) {
                let mut buf = [0; 8];
                file.read_exact(&mut buf)?;
                Cursor::new(buf).read_u64::<BigEndian>()?
            } else {
                return Err(Error::DBNameInvalidError);
            }
        } else if is_path_exists {
            // not a directory
            return Err(Error::DBNameInvalidError);
        } else if db_params.create_if_missing {
            fs::create_dir_all(db_name)?;
            0
        } else {
            return Err(Error::DBNameInvalidError);
        };
        Ok(num_files)
    }

    fn start_flush_thread(
        receiver: Receiver<()>,
        db_name: String,
        flush_table: Arc<RwLock<Option<MemTable>>>,
        cv_pair: Arc<(Mutex<bool>, Condvar)>,
        db_files: Arc<RwLock<u64>>,
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
                    }
                    Err(TryRecvError::Empty) => (),
                };

                {
                    let guard = flush_table.read()?;
                    if let Some(ref inner_table) = *guard {
                        for (key, value) in &inner_table.table {
                            _table_builder.add(key, value)?;
                        }
                        _table_builder.flush()?;

                        // update the metadata containing num of files
                        let mut num_files = Vec::with_capacity(8);
                        num_files
                            .write_u64::<BigEndian>(_table_builder.file_no())
                            .unwrap();
                        File::create(format!("{}/METADATA", db_name))
                            .unwrap()
                            .write_all(&num_files)
                            .unwrap();

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

    /// Returns the value corresponding to the key
    pub fn get<S: Into<Vec<u8>>>(&self, key: S) -> Result<Option<Vec<u8>>> {
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
                let table = Table::open(&self.db_name, *guard - i - 1)?;
                let val = table.get(&key_bytes)?;
                get_mem_value!(val);
            }
        }
        Ok(None)
    }

    /// Insertes a key-value pair to the database.
    /// If key was already present the value is updated.
    pub fn put<S: Into<Vec<u8>>>(&mut self, key: S, value: S) -> Result<()> {
        self.mem_table.put(key.into(), value.into())?;
        if self.mem_table.size() >= self.db_params.write_buffer_size {
            self.start_flushing()?;
        }
        Ok(())
    }

    /// Deletes a key from the database
    pub fn delete<S: Into<Vec<u8>>>(&mut self, key: S) -> Result<()> {
        self.mem_table.delete(key.into())?;
        if self.mem_table.size() >= self.db_params.write_buffer_size {
            self.start_flushing()?;
        }
        Ok(())
    }

    /// Safely closes the database
    /// Returns after finishing the background flush thread.
    pub fn close(&mut self) -> Result<()> {
        // db already closed
        if self.flush_thread_handle.is_none() {
            return Ok(());
        }

        // flush the mem_table since db is closing
        self.start_flushing()?;

        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock()?;
            while *to_flush {
                to_flush = cvar.wait(to_flush)?;
            }
        }

        // signal the background thread to finish and close
        self.flush_thread_sender.send(())?;

        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock().unwrap();
            *to_flush = true;
            cvar.notify_one();
        }

        // join the flush_thread_handle
        let join_handle = self.flush_thread_handle.take();
        if join_handle.unwrap().join().is_err() {
            return Err(Error::BackgroundFlushError);
        }
        Ok(())
    }

    // converts the mem_table to flush_table and signals the background flush_thread to start flushing
    fn start_flushing(&mut self) -> Result<()> {
        if self.mem_table.table.is_empty() {
            return Ok(());
        }

        // wait if the flush thread has not finished flushing the last flush_table
        {
            let &(ref lock, ref cvar) = &*self.cv_pair;
            let mut to_flush = lock.lock()?;
            while *to_flush {
                to_flush = cvar.wait(to_flush)?; // writer blocks!! TODO: think of a better solution
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

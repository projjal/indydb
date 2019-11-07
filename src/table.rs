use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::io::SeekFrom;

use crate::errors::{Error, Result};
use crate::memtable::MemValue;

// Table builder builds the on-disk log files
// Corresponding to each memtable two log files - data table and index table - are generated
pub struct TableBuilder {
    db_name: String,
    file_no: u64,
    data: Vec<u8>,
    index: Vec<u8>,
    offset: u64,
}

impl TableBuilder {
    pub fn new(db_name: &str, file_no: u64) -> TableBuilder {
        TableBuilder {
            db_name: String::from(db_name),
            file_no,
            data: Vec::new(),
            index: Vec::new(),
            offset: 0,
        }
    }

    // Adds the key and value marker (delete or data offset) to the index table and actual value to the data table
    pub fn add(&mut self, key: &[u8], value: &MemValue) -> Result<()> {
        self.index.append(&mut self.encode(key)?);
        self.index.push(value.encode());
        match value {
            MemValue::Value(val) => {
                let mut data_offset = Vec::with_capacity(8);
                data_offset.write_u64::<BigEndian>(self.offset)?;
                self.index.append(&mut data_offset);
                self.data.append(&mut self.encode(&val)?);
                self.offset += 8 + val.len() as u64;
            }
            MemValue::Delete => (),
        }
        Ok(())
    }

    // encodes data to on-disk format - <data len><data bytes>
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut vec = Vec::with_capacity(8 + data.len());
        vec.write_u64::<BigEndian>(data.len() as u64)?;
        vec.extend_from_slice(data);
        Ok(vec)
    }

    // flushes to log files in the disk
    pub fn flush(&mut self) -> Result<()> {
        File::create(format!("{}/{}.dt", self.db_name, self.file_no))?.write_all(&self.data)?;
        File::create(format!("{}/{}.ix", self.db_name, self.file_no))?.write_all(&self.index)?;

        // reset data after flush
        self.data.clear();
        self.index.clear();
        self.offset = 0;
        self.file_no += 1;
        Ok(())
    }

    pub fn file_no(&self) -> u64 {
        self.file_no
    }
}

// Delete marker or offset to the data table
enum IndexValue {
    Offset(u64),
    Delete,
}

#[repr(u8)]
#[derive(FromPrimitive)]
enum MemValueCode {
    Value = 0u8,
    Delete = 1u8,
}

// In-memory Index table corresponding to a particular log file
pub struct Table {
    db_name: String,
    // log number of the index table
    file_no: u64,
    // index maps key to an IndexValue, which is either a delete marker or an offset to corresponding data table
    index: HashMap<Vec<u8>, IndexValue>,
}

impl Table {
    // loads the index table into memory
    pub fn open(db_name: &str, file_no: u64) -> Result<Table> {
        let mut f = File::open(format!("{}/{}.ix", db_name, file_no))?;
        let mut index_buf = Vec::new();
        f.read_to_end(&mut index_buf)?;
        let mut index = HashMap::new();
        let mut i = 0;
        let buf_len = index_buf.len();
        loop {
            if i >= buf_len {
                break;
            }
            let key_size = Cursor::new(&index_buf[i..i + 8]).read_u64::<BigEndian>()?;
            let mut key = vec![0; key_size as usize];

            key.copy_from_slice(&index_buf[i + 8..i + 8 + key_size as usize]);
            i += 8 + key_size as usize;

            match FromPrimitive::from_u8(index_buf[i]) {
                Some(MemValueCode::Value) => {
                    i += 1;
                    let offset = Cursor::new(&index_buf[i..i + 8]).read_u64::<BigEndian>()?;
                    i += 8;
                    index.insert(key, IndexValue::Offset(offset));
                }
                Some(MemValueCode::Delete) => {
                    i += 1;
                    index.insert(key, IndexValue::Delete);
                }
                None => return Err(Error::DBCorruptionError),
            };
        }
        Ok(Table {
            db_name: String::from(db_name),
            file_no,
            index,
        })
    }

    // Returns the value corresponding to the key from the data table
    // Either the key corresponds to a delete marker (meaning the key is deleted)
    // or it corresponds to data table offset in which case the value is returned
    pub fn get(&self, key: &[u8]) -> Result<Option<MemValue>> {
        let value = self.index.get(key);
        match value {
            Some(IndexValue::Offset(off)) => {
                let mut f = File::open(format!("{}/{}.dt", self.db_name, self.file_no))?;
                f.seek(SeekFrom::Start(*off))?;
                let val = self.decode(&mut f)?;
                Ok(Some(MemValue::Value(val)))
            }
            Some(IndexValue::Delete) => Ok(Some(MemValue::Delete)),
            None => Ok(None),
        }
    }

    fn decode(&self, file: &mut File) -> Result<Vec<u8>> {
        let mut size_buf = [0; 8];
        file.read_exact(&mut size_buf)?;
        let size = Cursor::new(size_buf).read_u64::<BigEndian>()?;
        let mut content = vec![0; size as usize];
        let mut bytes_read = 0;
        loop {
            let rb = file.read(&mut content[bytes_read..])?;
            bytes_read += rb;
            if bytes_read >= size as usize {
                return Ok(content);
            }
        }
    }
}

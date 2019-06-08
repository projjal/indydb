use std::collections::HashMap;

use crate::errors::Result;

pub enum MemValue {
    Value(Vec<u8>),
    Delete
}

impl MemValue {
    pub fn encode(&self) -> u8 {
        match *self {
            MemValue::Value(_) => 0u8,
            MemValue::Delete => 1u8,
        }
    }
}

pub struct MemTable {
    pub table : HashMap<Vec<u8>, MemValue>,
    size: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            table: HashMap::new(),
            size: 0
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&MemValue> {
        self.table.get(key)
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.size += key.len() + value.len();
        self.table.insert(key, MemValue::Value(value));
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.size += key.len();
        self.table.insert(key, MemValue::Delete);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
use chashmap::CHashMap;

use crate::errors::Result;

#[derive(Clone)]
pub enum MemValue {
    Value(Vec<u8>),
    Delete,
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
    pub table: CHashMap<Vec<u8>, MemValue>,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            table: CHashMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<MemValue> {
        self.table.get(key).map(|guard| (*guard).clone())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.table
            .insert(key.to_vec(), MemValue::Value(value.to_vec()));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.table.insert(key.to_vec(), MemValue::Delete);
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.table.len()
    }

    pub fn  is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn clear(&self) -> MemTable {
        MemTable {
            table: self.table.clear(),
        }
    }
}

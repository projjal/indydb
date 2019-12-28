use std::borrow::Borrow;
use std::clone::Clone;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::Copy;
use std::mem;
use std::ptr;

struct RawLink<T> {
    p: *mut T,
}

impl<T> Copy for RawLink<T> {}

impl<T> Clone for RawLink<T> {
    fn clone(&self) -> Self {
        RawLink { p: self.p }
    }
}

#[doc(hidden)]
pub struct KeyRef<K> {
    k: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

impl<K> Borrow<K> for KeyRef<K> {
    fn borrow(&self) -> &K {
        unsafe { &*self.k }
    }
}

#[cfg(feature = "nightly")]
#[doc(hidden)]
pub auto trait NotKeyRef {}

#[cfg(feature = "nightly")]
impl<K> !NotKeyRef for KeyRef<K> {}

#[cfg(feature = "nightly")]
impl<K, Q> Borrow<Q> for KeyRef<K>
where
    K: Borrow<Q>,
    Q: NotKeyRef + ?Sized,
{
    fn borrow(&self) -> &Q {
        unsafe { (&*self.k) }.borrow()
    }
}

struct LRUEntry<K, V> {
    key: K,
    val: V,
    next: RawLink<LRUEntry<K, V>>,
    prev: RawLink<LRUEntry<K, V>>,
}

impl<K, V> LRUEntry<K, V> {
    fn new(key: K, val: V) -> Self {
        LRUEntry {
            key,
            val,
            prev: RawLink::none(),
            next: RawLink::none(),
        }
    }
}

pub struct LRUCache<K, V> {
    map: HashMap<KeyRef<K>, Box<LRUEntry<K, V>>>,
    cap: usize,
    head: RawLink<LRUEntry<K, V>>,
    tail: RawLink<LRUEntry<K, V>>,
}

impl<K: Hash + Eq, V> LRUCache<K, V> {
    pub fn new(cap: usize) -> Self {
        let mut cache = LRUCache {
            map: HashMap::with_capacity(cap),
            cap,
            head: RawLink {
                p: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LRUEntry<K, V>>())) },
            },
            tail: RawLink {
                p: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LRUEntry<K, V>>())) },
            },
        };

        cache.head.resolve_mut().next = cache.tail;
        cache.tail.resolve_mut().prev = cache.head;
        cache
    }

    pub fn get<'a, Q>(&'a mut self, key: &Q) -> Option<&'a V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(entry) = self.map.get_mut(key) {
            let entry_ptr = RawLink::some(&mut **entry);
            self.detach(entry_ptr);
            self.attach(entry_ptr);
            Some(unsafe { &(*entry_ptr.p).val })
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, val: V) -> Option<V> {
        if let Some(entry) = self.map.get_mut(&key) {
            let prev_val = mem::replace(&mut (*entry).val, val);
            let entry_ptr = RawLink::some(&mut **entry);
            self.detach(entry_ptr);
            self.attach(entry_ptr);
            Some(prev_val)
        } else {
            let mut entry = if self.map.len() == self.cap {
                let mut last_entry_ptr = self.tail.resolve_mut().prev;
                let last_entry = last_entry_ptr.resolve_mut();
                self.map.remove(&last_entry.key).unwrap();

                last_entry.key = key;
                last_entry.val = val;

                self.detach(last_entry_ptr);
                last_entry_ptr.to_box()
            } else {
                Box::new(LRUEntry::new(key, val))
            };

            self.attach(RawLink::some(&mut *entry));
            self.map.insert(KeyRef { k: &(*entry).key }, entry);
            None
        }
    }

    fn detach(&mut self, mut node_ptr: RawLink<LRUEntry<K, V>>) {
        let node = node_ptr.resolve_mut();
        node.prev.resolve_mut().next = node.next;
        node.next.resolve_mut().prev = node.prev;
    }

    fn attach(&mut self, mut node_ptr: RawLink<LRUEntry<K, V>>) {
        let node = node_ptr.resolve_mut();
        let head = self.head.resolve_mut();
        node.next = head.next;

        let node_ptr = RawLink::some(node);
        head.next.resolve_mut().prev = node_ptr;

        head.next = node_ptr;
    }
}

impl<T> RawLink<T> {
    fn none() -> RawLink<T> {
        RawLink { p: ptr::null_mut() }
    }

    fn some(n: &mut T) -> RawLink<T> {
        RawLink { p: n as *mut T }
    }

    fn to_box(self) -> Box<T> {
        unsafe { Box::from_raw(self.p) }
    }

    fn resolve_mut(&mut self) -> &mut T {
        unsafe { &mut *self.p }
    }
}

#[cfg(test)]
mod tests {
    use super::LRUCache;
    use std::fmt::Debug;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_put_and_get(){
        let mut cache = LRUCache::new(10);

        assert_eq!(cache.put("hello", "world"), None);
        assert_eq!(cache.put("lorem", "ipsum"), None);

        assert_opt_eq(cache.get(&"hello"), "world");
        assert_opt_eq( cache.get(&"lorem"), "ipsum");
        assert!(cache.get(&"paris").is_none());
    }

}
use typed_arena::Arena;

use std::cell::RefCell;
use std::io::{self, Write};
use std::ops::Deref;
use tikv_util::collections::HashMap;

const ARENA_VEC_DEFAULT_CAPACITY: usize = 32;

pub struct BytesArena {
    arena: Arena<u8>,
    free_lists: RefCell<HashMap<usize, Vec<*mut u8>>>,
}

unsafe impl Send for BytesArena {}

impl BytesArena {
    fn alloc_buf(&self, n: usize) -> &mut [u8] {
        if let Some(ptr) = self.free_lists.borrow_mut().get_mut(&n).and_then(Vec::pop) {
            unsafe { std::slice::from_raw_parts_mut(ptr, n) }
        } else {
            unsafe { &mut *self.arena.alloc_uninitialized(n) }
        }
    }

    pub unsafe fn add_free(&self, buf: &[u8]) {
        let len = buf.len();
        let ptr = buf.as_ptr() as *mut _;
        self.free_lists
            .borrow_mut()
            .entry(len)
            .and_modify(|v| v.push(ptr))
            .or_insert_with(|| {
                // It is OK to initialize all `Vec`s with capacity of 1024.
                // The buffer length has limited (mostly < 20) possibilities,
                // thus takes no more than 1024 * 20 * 8B = 160KB
                let mut v =
                    Vec::with_capacity(crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE);
                v.push(ptr);
                v
            });
    }

    pub fn with_capacity(n: usize) -> Self {
        BytesArena {
            arena: Arena::with_capacity(n),
            free_lists: RefCell::new(HashMap::new()),
        }
    }

    pub fn new_vec(&self) -> ArenaVec<'_> {
        self.new_vec_with_capacity(ARENA_VEC_DEFAULT_CAPACITY)
    }

    pub fn new_vec_with_capacity(&self, min_size: usize) -> ArenaVec<'_> {
        let buf = self.alloc_buf(min_size.next_power_of_two());
        ArenaVec {
            buf,
            len: 0,
            arena: self,
        }
    }
}

pub struct ArenaVec<'a> {
    buf: &'a mut [u8],
    len: usize,
    arena: &'a BytesArena,
}

impl<'a> ArenaVec<'a> {
    fn expand(&mut self, min_size: usize) {
        // We don't check length overflow because OOM should occur earlier.
        let new_buf = self.arena.alloc_buf(min_size.next_power_of_two());
        new_buf[..self.len].copy_from_slice(&self.buf[..self.len]);
        let old_buf = std::mem::replace(&mut self.buf, new_buf);
        unsafe { self.arena.add_free(old_buf); }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn into_inner(self) -> &'a mut [u8] {
        self.buf
    }
}

impl<'a> Write for ArenaVec<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let new_len = self.len + buf.len();
        if new_len > self.buf.len() {
            self.expand(new_len);
        }
        self.buf[self.len..new_len].copy_from_slice(buf);
        self.len = new_len;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Deref for ArenaVec<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.buf
    }
}

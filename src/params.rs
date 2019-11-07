#[derive(Default)]
pub struct DBParams {
    /// create a new db while opening if it doesn't exist
    pub create_if_missing: bool,
    /// size of the mem_table after which it is flushed to disk
    pub write_buffer_size: usize,
}

impl DBParams {
    /// Returns a DBParams object with default values
    pub fn new() -> DBParams {
        DBParams {
            create_if_missing: true,
            write_buffer_size: 2 << 12,
        }
    }
}

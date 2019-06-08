pub struct DBParams {
    pub create_if_missing: bool,
    pub write_buffer_size: usize,
}

impl DBParams {
    pub fn new() -> DBParams {
        DBParams {
            create_if_missing : true,
            write_buffer_size : 2 << 12,
        }
    }
}

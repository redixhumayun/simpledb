use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// A temporary directory that is deleted when it goes out of scope.
/// Used for testing
pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

impl AsRef<Path> for TestDir {
    fn as_ref(&self) -> &Path {
        self.path.as_ref()
    }
}

pub fn generate_filename() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = std::thread::current().id();
    format!("test_file_{}_{:?}", timestamp, thread_id)
}

pub fn generate_random_number() -> usize {
    let mut f = File::open("/dev/urandom").unwrap();
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf).unwrap();
    usize::from_le_bytes(buf)
}

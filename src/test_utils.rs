use std::path::{Path, PathBuf};

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

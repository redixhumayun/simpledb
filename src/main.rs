use std::{
    collections::{btree_map::Entry, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::PathBuf,
};

struct SimpleDB {
    db_directory: PathBuf,
}

impl SimpleDB {
    fn new<P: AsRef<PathBuf>>(path: P) -> Self {
        Self {
            db_directory: path.as_ref().to_path_buf(),
        }
    }
}

struct BlockId {
    filename: String,
    block_num: usize,
}

impl BlockId {
    fn new(filename: String, block_num: usize) -> Self {
        Self {
            filename,
            block_num,
        }
    }
}

struct Page {
    contents: Vec<u8>,
}

impl Page {
    const INT_BYTES: usize = 4;

    fn new(blocksize: usize) -> Self {
        Self {
            contents: vec![0; blocksize],
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { contents: bytes }
    }

    fn get_int(&self, offset: usize) -> i32 {
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        i32::from_be_bytes(bytes)
    }

    fn set_int(&mut self, offset: usize, n: i32) {
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&n.to_be_bytes());
    }

    fn get_bytes(&self, mut offset: usize) -> Vec<u8> {
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        let length = u32::from_be_bytes(bytes) as usize;
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + length].to_vec()
    }

    fn set_bytes(&mut self, mut offset: usize, bytes: &[u8]) {
        let length = bytes.len() as u32;
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&length.to_be_bytes());
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }

    fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes).unwrap()
    }

    fn set_string(&mut self, offset: usize, string: &str) {
        self.set_bytes(offset, string.as_bytes());
    }
}

struct Buffer {
    file_manager: FileManager,
    contents: Page,
    block_id: Option<BlockId>,
    pins: usize,
    txn: isize,
    lsn: isize,
}

impl Buffer {
    fn new(file_manager: FileManager) -> Self {
        let size = file_manager.blocksize;
        Self {
            file_manager,
            contents: Page::new(size),
            block_id: None,
            pins: 0,
            txn: 0,
            lsn: 0,
        }
    }
}

struct FileManager {
    db_directory: PathBuf,
    blocksize: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    fn new(db_directory: impl AsRef<PathBuf>, blocksize: usize) -> io::Result<Self> {
        let db_path = db_directory.as_ref().to_path_buf();
        fs::create_dir(&db_path)?;

        //  remove all existing files in the directory
        for entry in fs::read_dir(&db_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }

        Ok(Self {
            db_directory: db_path,
            blocksize,
            open_files: HashMap::new(),
        })
    }

    fn length(&mut self, filename: String) -> usize {
        let file = self.get_file(filename);
        let len = file.metadata().unwrap().len() as usize;
        len / self.blocksize
    }

    fn read(&mut self, block_id: BlockId, page: &mut Page) {
        let mut file = self.get_file(block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        let mut bytes = Vec::new();
        file.read(&mut bytes).unwrap();
        page.contents = bytes;
    }

    fn write(&mut self, block_id: BlockId, page: &mut Page) {
        let mut file = self.get_file(block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        file.write(&page.contents).unwrap();
    }

    fn append(&mut self, filename: String) -> BlockId {
        let new_blk_num = self.length(filename.clone());
        let block_id = BlockId::new(filename.clone(), new_blk_num);
        let buffer = Page::new(self.blocksize);
        let mut file = self.get_file(filename.clone());
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize).try_into().unwrap(),
        ))
        .unwrap();
        file.write(&buffer.contents).unwrap();
        block_id
    }

    fn get_file(&mut self, filename: String) -> File {
        self.open_files
            .entry(filename.to_string())
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.db_directory.join(filename))
                    .expect("Failed to open file")
            })
            .try_clone()
            .unwrap()
    }
}

fn main() {
    println!("Hello, world!");
}

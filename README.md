## To-do list

### Transactions
1. Implement a deadlock detection strategy (either wait-for or wait-die)

### Storage
1. Store a bitmap for presence checking
2. Store an ID table to manage offsets so that its easier to support variable length strings (similar to B-tree pages)


### Notes
The current error I'm facing for a few tests seems to be because of some file already existing

```shell
cargo test transaction_tests::test_transaction_durability -- --nocapture
   Compiling simpledb v0.1.0 (/Users/zaidhumayun/Desktop/Development.nosync/databases/simpledb)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.53s
     Running unittests src/main.rs (target/debug/deps/simpledb-fc979dcd7f8ea1ce)

running 1 test

thread 'transaction_tests::test_transaction_durability' panicked at src/test_utils.rs:20:36:
called `Result::unwrap()` on an `Err` value: Os { code: 17, kind: AlreadyExists, message: "File exists" }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test transaction_tests::test_transaction_durability ... FAILED

failures:

failures:
    transaction_tests::test_transaction_durability

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 38 filtered out; finished in 0.00s
```

Now, easiest to fix this by first making sure everything uses the harness to test rather than creating independent files. After that I can see why there are errors when creating the metadata manager.
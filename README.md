## Overview
SimpleDB is a Rust port of the Java implementation by Edward Sciore. You can read about the Java implementation in Sciore's book, [Database Design & Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7).

This port is mainly for pedagagical and experimentation reasons. I wanted to understand how query engines worked in more detail and I also wanted a playground to experiment with different ideas.

### Core Features

The database supports ACID transactions, along with some other niceties like 
* A buffer pool to manage memory
* A WAL to ensure durability
* A catalog to manage metadata for all tables
* A query engine with a simple optimizer

### Roadmap

See the full project roadmap in [docs/ROADMAP.md](docs/ROADMAP.md).
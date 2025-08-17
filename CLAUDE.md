# SimpleDB

This documentation provides information about the SimpleDB project and also provides development workflow for Claude Code agents.

## Project Overview

**Purpose**: A simple SQL database which is a port of an existing SimpleDB database written in Java to Rust. It is mainly for pedagogical purposes and also as a way to experiment with Rust code and performance optimizations

**Tech Stack**: Rust

**Repository**: https://github.com/redixhumayun/simpledb

## Architecture Overview

Almost the entirety of the code can be found in `main.rs`. This is on purpose to keep the code in one place since this repo is for pedgagogical reasons.

There are no dependencies apart from the Rust standard library and that is by design.

The code is designed to construct and answer typical SQL queries. The code will construct a query tree that will use the pull-based iterator pattern in a way that is probably typical in most SQL systems. However, the code leans towards readability rather than performance.

There is a test suite which provides basic coverage to ensure the code still works. This can be run with `cargo test`

## Development Workflow

### Git Workflow
[1. ](### Git Workflow (REQUIRED)
1. **Always start by syncing with master**:
   ```bash
   git checkout master
   git pull origin master
   ```

2. **Create feature branch with descriptive name**:
   ```bash
   git checkout -b feature/descriptive-name
   # or fix/bug-description, enhance/improvement-name
   ```

3. **Work autonomously using available tools** until blocked

4. **Test thoroughly before committing**:
   ```bash
   cargo build
   cargo test
   # Verify build works and tests pass
   ```

5. **Create PR with descriptive title and summary**
   - Include what was implemented
   - Note any breaking changes

6. **Address feedback as separate commits**)
# Formal Specs

This directory holds experimental formal models for SimpleDB protocols.

## Buffer Pool

`buffer_pool.qnt` models the core buffer-pool residency protocol:

- directory lookup and publish states
- frame loading / resident / evicting states
- optimistic resident-hit pin validation
- miss install and eviction transitions

The model intentionally omits dirty-page writeback and replacement-policy details.

Useful commands from the repo root:

```bash
quint typecheck specs/buffer_pool.qnt
quint run specs/buffer_pool.qnt --main=buffer_pool_2x2x2 --invariant=safetyOK
quint verify specs/buffer_pool.qnt --main=buffer_pool_2x2x2 --invariant=safetyOK --max-steps=15
```

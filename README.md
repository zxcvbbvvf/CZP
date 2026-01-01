# CZP
Deterministic, content-aware archival format (CDC + dedupe + verification) with reproducible builds.

## What is CZP?

CZP is an experimental, deterministic, content-aware archival format and tool.

It combines:
- content-defined chunking (CDC)
- optional deduplication
- multiple internal “lanes” optimized for different data shapes
- explicit integrity verification
- reproducible, bit-identical builds

## What CZP is not

- Not a drop-in replacement for zip or tar
- Not optimized for maximum compression ratio
- Not intended for already-compressed media (jpg/mp4/zip-heavy datasets)
- Not guaranteed stable across major versions (yet)

## Determinism

Given:
- identical input data
- identical CZP version
- identical settings

CZP guarantees bit-identical archive output.

This makes archives suitable for reproducible builds, artifact caching,
and regression detection.

## Status

CZP is experimental.

The format, heuristics, and internal layout may change.
Correctness and determinism are prioritized over backward compatibility.

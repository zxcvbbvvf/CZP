# CZP

Deterministic, content-aware archival format with reproducible builds.

## What is CZP?

CZP is an experimental, deterministic, content-aware archival format and tool.

It treats archival as a **reproducible computation** rather than simple byte packing.

CZP combines:
- content-defined chunking (CDC)
- optional deduplication
- multiple internal *lanes* optimized for different data shapes
- explicit block-level and file-level integrity verification
- reproducible, bit-identical archive builds

The primary goal is **correctness, determinism, and explainability** — not maximum compression ratio.

---

## Lanes (internal strategies)

CZP automatically selects one of several internal lanes per file, based on deterministic heuristics:

- **WHOLE** – single-stream compression or store  
  Used for entropic or low-gain data.

- **CDC** – content-defined chunking  
  Used for moderately compressible data and large files.

- **PI (Patch-Indexed)** – delta encoding against similar files  
  Used for near-identical variants (e.g. successive builds, similar assets).

- **DNA** – motif-based encoding  
  Used for highly repetitive or structured text.

Lane selection is automatic by default and deterministic for a given build and input set.

---

## What CZP is not

- Not a drop-in replacement for zip or tar
- Not optimized for maximum compression ratio
- Not intended for already-compressed media (jpg/mp4/zip-heavy datasets)
- Not guaranteed stable across major versions
- Not a general-purpose backup system

---

## Determinism

Given:
- identical input data
- identical CZP version and build
- identical settings
- identical environment

CZP guarantees **bit-identical archive output**.

This makes CZP suitable for:
- reproducible builds
- artifact caching
- regression detection
- integrity-sensitive archival

If two CZP archives differ, something materially changed.

---

## Requirements

- Python 3.8+
- Optional: `zstandard` (for faster compression)

To install the optional dependency:

```
pip install zstandard
```

CZP is a single-file tool. No installation step is required.

---

## Usage

Run CZP directly with Python:

```
python czp.py <command> [options]
```

### Commands

- `a` – create/add archive
- `l` – list archive contents
- `t` – test and verify archive integrity
- `x` – extract archive
- `info` – show archive metadata

---

## Create an archive

```
python czp.py a archive.czp input_directory/
```

This builds a deterministic archive from the input directory.

Given identical input data, CZP version/build, and settings, the resulting archive will be
bit-identical across runs.

---

## Verify determinism (recommended)

```
python czp.py a out1.czp input_directory/
python czp.py a out2.czp input_directory/
sha256sum out1.czp out2.czp
```

Matching hashes confirm reproducible output.

---

## List archive contents

```
python czp.py l archive.czp
```

---

## Test archive integrity

```
python czp.py t archive.czp
```

This verifies:
- block-level CRCs
- file-level CRCs
- correct reconstruction paths

Any mismatch is treated as an error.

---

## Extract an archive

```
python czp.py x archive.czp output_directory/
```

Extracted files are byte-identical to the original inputs.

---

## Inspect archive metadata

```
python czp.py info archive.czp
```

Shows:
- tool version
- build identifier
- format version
- lane statistics
- archive policy flags

---

## Planning / dry-run mode

```
python czp.py a --plan-only archive.czp input_directory/
```

Prints a machine-readable description of:
- selected lanes
- heuristic decisions
- expected archive structure

No archive is written.

---

## Status

CZP is **experimental**.

The format, heuristics, and internal layout may change.
Correctness and determinism are prioritized over backward compatibility.

Backward-incompatible changes are reflected by version and build identifiers.

---

## License

MIT License. See LICENSE for details.

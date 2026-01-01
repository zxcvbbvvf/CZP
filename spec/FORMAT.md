# CZP Format Specification (FORMAT_VERSION=1)

This document is an **implementation-matched** specification for the current reference tool (`czp.py`).
It describes the exact on-disk layout used by this build (magic `b"CZP3"`).

CZP is **experimental**. Backward-incompatible changes MUST bump `FORMAT_VERSION`.
Heuristic / lane-selection changes SHOULD bump the tool build identifier.

---

## 1. Byte order and basic rules

- All fixed-width integer fields are **little-endian** (`struct` format strings begin with `<`).
- **Sections** are written sequentially; readers MUST reject truncation.
- Safety cap: `MAX_SECTION = 2 * 1024 * 1024 * 1024  # 2147483648 bytes`

---

## 2. File header

`FILE_HDR = struct.Struct("<4sH")`  (`<4sH`)

| Field | Type | Meaning |
|---|---:|---|
| `magic` | `4s` | Must equal `b"CZP3"` |
| `format_version` | `u16` | Must equal `1` |

Readers MUST reject unknown `magic`. Readers MAY reject unsupported `format_version`.

---

## 3. Section framing

Each section is:

1) `SEC_HDR`
2) `payload` (`payload_len` bytes)

`SEC_HDR = struct.Struct("<4sQ")`  (`<4sQ`)

| Field | Type | Meaning |
|---|---:|---|
| `tag` | `4s` | 4-byte ASCII tag |
| `payload_len` | `u64` | Payload length in bytes |

Readers MUST reject `payload_len > MAX_SECTION` and truncated payloads.

---

## 4. Section tags

The reference writer uses these tags:

| Tag | Bytes | Purpose |
|---|---|---|
| HEAD | `b"HEAD"` | Archive metadata (JSON); multiple allowed, **last wins** |
| CHNK | `b"CHNK"` | Chunk block (WHOLE or CDC chunk) |
| BLK2 | `b"BLK2"` | Micro-pack block |
| DNA1 | `b"DNA1"` | DNA lane block (motifs + token stream) |
| PI01 | `b"PI01"` | PI lane block (delta-from-base) |
| FIDX | `b"FIDX"` | File index (records + spans) |
| END! | `b"END!"` | Terminator (payload length = 0) |

Parsers MUST accept multiple HEAD sections and treat the **final** HEAD as authoritative.

---

## 5. Codec identifiers

Block headers record a codec id:

| Name | Value |
|---|---:|
| CODEC_STORE | 0 |
| CODEC_ZSTD  | 1 |
| CODEC_ZLIB  | 2 |

- STORE: bytes are stored as-is.
- ZSTD: Zstandard frame (requires zstandard dependency to decode).
- ZLIB: zlib/DEFLATE frame.

Readers MUST support STORE and ZLIB; ZSTD support is optional but recommended.

---

## 6. Span flags and AUX encoding

Spans carry a 32-bit `span_flags` value. Low 16 bits are type flags; upper 16 bits carry lane-specific auxiliary data (currently: MICRO entry index).

Flags (low bits):

| Flag | Value | Meaning |
|---|---:|---|
| SPANF_LAST | 1 | Span terminates the file stream |
| SPANF_MICRO | 2 | Span is a BLK2 entry (MICRO) |
| SPANF_STORE | 4 | Underlying payload was stored (no compression) |
| SPANF_DNA | 8 | DNA lane |
| SPANF_PI | 16 | PI lane |
| SPANF_HIGHENT | 32 | Diagnostic: high-entropy suppression active |

AUX encoding:

- `aux_u16 = (span_flags >> 16) & 0xFFFF`
- `base_flags = span_flags & 0xFFFF`

Writers MUST set SPANF_LAST on the final span of each file.

---

## 7. File index (FIDX)

`FIDX` payload encodes all file records and their spans.

### 7.1 FIDX header

`FILEIDX_HEAD = struct.Struct("<I")`  (`<I`)

| Field | Type | Meaning |
|---|---:|---|
| `file_count` | `u32` | Number of file records |

### 7.2 File record header

`FILE_REC_HDR = struct.Struct("<HQQII BBH")`

Note: the format string contains whitespace; it is equivalent to `"<HQQIIBBH"`.

Fields (in pack order):

| Field | Type | Meaning |
|---|---:|---|
| `path_len` | `u16` | Number of UTF-8 bytes in path |
| `mtime_ns` | `u64` | Source mtime in ns (may be canonicalized under reproducible mode) |
| `raw_size` | `u64` | File size in bytes |
| `file_crc32` | `u32` | CRC32 of reconstructed file bytes |
| `span_count` | `u32` | Number of spans following |
| `class_id` | `u8` | File class id (heuristic; metadata) |
| `file_flags` | `u8` | File flags (metadata) |
| `reserved` | `u16` | Must be 0 in current writer |

After `FILE_REC_HDR`:
- `path` bytes: UTF-8, length `path_len`
- then `span_count` span records.

### 7.3 Span record

`SPAN_REC = struct.Struct("<QII")`  (`<QII`)

| Field | Type | Meaning |
|---|---:|---|
| `block_id` | `u64` | Referenced block id |
| `span_raw_len` | `u32` | Bytes contributed to file output |
| `span_flags` | `u32` | Flags + optional AUX |

Reconstruction concatenates spans in order. For non-MICRO spans, the reader:
- decodes the referenced block’s raw bytes
- appends up to `span_raw_len` bytes (tail CDC span may be shorter and is trimmed)

---

## 8. Blocks

Blocks are stored as sections whose payload begins with a block header struct, followed by data.

### 8.1 CHNK block

`CHNK_HDR = struct.Struct("<QBBHIII32s")`  (`<QBBHIII32s`)

Fields (pack order):

| Field | Type | Meaning |
|---|---:|---|
| `block_id` | `u64` | Block identifier |
| `codec_id` | `u8` | CODEC_* |
| `flags` | `u8` | Block flags (writer-defined) |
| `reserved` | `u16` | Must be 0 |
| `raw_len` | `u32` | Decompressed length |
| `comp_len` | `u32` | Compressed length |
| `raw_crc32` | `u32` | CRC32 over decompressed raw bytes |
| `h32` | `32s` | 32-byte hash (used for dedupe/diagnostics) |

Immediately after the header: `comp_len` bytes of payload (stored or compressed according to `codec_id`).

Readers MUST:
- read `comp_len` bytes
- decompress if needed
- verify `len(raw) == raw_len`
- verify `crc32(raw) == raw_crc32`

### 8.2 BLK2 (MICRO) block

`BLK2_HDR = struct.Struct("<QBBHIIII")`  (`<QBBHIIII`)

| Field | Type | Meaning |
|---|---:|---|
| `block_id` | `u64` | Block identifier |
| `codec_id` | `u8` | CODEC_* |
| `flags` | `u8` | Currently 0 |
| `reserved` | `u16` | Must be 0 |
| `raw_len` | `u32` | Decompressed blob length |
| `comp_len` | `u32` | Compressed blob length |
| `raw_crc32` | `u32` | CRC32 over decompressed blob |
| `entry_count` | `u32` | Number of micro entries |

Then `entry_count` entries of:

`BLK2_ENTRY = struct.Struct("<IIII")`  (`<IIII`)

| Field | Type | Meaning |
|---|---:|---|
| `file_index` | `u32` | File index associated with this entry (diagnostic) |
| `off` | `u32` | Offset into decompressed blob |
| `len` | `u32` | Length of this entry |
| `crc32` | `u32` | CRC32 of this entry’s bytes |

After the entry table: `comp_len` bytes (compressed blob).

MICRO spans reference a BLK2 block and choose an entry via `aux_u16` stored in the span flags (upper 16 bits).
Readers MUST validate entry CRC32 before appending entry bytes.

### 8.3 DNA1 block

`DNA1_HDR = struct.Struct("<QBBHHHIII")`  (`<QBBHHHIII`)

| Field | Type | Meaning |
|---|---:|---|
| `block_id` | `u64` | Block identifier |
| `codec_id` | `u8` | CODEC_* |
| `flags` | `u8` | Currently 0 |
| `motif_len` | `u16` | Motif byte length (current constant: 32) |
| `motif_count` | `u16` | Number of motifs |
| `reserved` | `u16` | Must be 0 |
| `raw_len` | `u32` | Reconstructed raw byte length |
| `comp_len` | `u32` | Token stream compressed length |
| `raw_crc32` | `u32` | CRC32 over reconstructed raw bytes |

After the header:
1) `motif_count * motif_len` bytes of motif table
2) `comp_len` bytes of compressed token stream

Readers MUST decompress the token stream, decode deterministically using the motif table, and verify CRC32.

### 8.4 PI01 block

`PI01_HDR = struct.Struct("<QBBHIIII")`  (`<QBBHIIII`)

| Field | Type | Meaning |
|---|---:|---|
| `block_id` | `u64` | Block identifier |
| `codec_id` | `u8` | CODEC_* |
| `flags` | `u8` | Currently 0 |
| `reserved` | `u16` | Must be 0 |
| `base_index` | `u32` | Index into the FIDX file list to use as base |
| `raw_len` | `u32` | Reconstructed raw byte length |
| `comp_len` | `u32` | Delta ops compressed length |
| `raw_crc32` | `u32` | CRC32 over reconstructed raw bytes |

After the header: `comp_len` bytes of compressed delta ops.

Readers MUST:
1) reconstruct the base file bytes referenced by `base_index`
2) decode delta ops deterministically
3) reconstruct the output bytes
4) verify length and CRC32

(Delta opcodes are defined by the reference implementation; readers must reject out-of-bounds ops.)

---

## 9. HEAD section (JSON metadata)

`HEAD` payload is UTF-8 JSON.

The reference writer serializes deterministically:
- keys sorted
- compact separators (no whitespace)
- `ensure_ascii=false`

Multiple HEAD sections may occur; the **last** HEAD is authoritative.

HEAD is metadata; reconstruction depends on blocks + FIDX.

---

## 10. END! section

`END!` terminates the archive. Payload length MUST be 0 in the current writer.
Readers MAY ignore trailing bytes after END!, but the reference writer does not emit any.


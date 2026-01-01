"""CZP v14 reference implementation (Python-only).

CZP is a deterministic, content-aware archive format with multiple "lanes" that trade
CPU time for size reduction:

- WHOLE: compress whole files (fast path).
- CDC: content-defined chunking with dedupe.
- MICRO: specialized handling for tiny files.
- DNA: dictionary + token stream (optional).
- PI: delta-from-base with verification (optional).

Determinism is the default contract:
- Archives created from identical inputs are bit-identical by default.
- Timestamps and mtimes are canonicalized (SOURCE_DATE_EPOCH or 0).
- File ordering is stable and any parallel work is merged deterministically.

CLI (subcommands):
  a  add/create archive
  l  list
  info  show metadata
  t  test/verify (CRC)
  x  extract

Notable flags:
  --preset auto|default|fast-game   choose heuristics (auto is dataset-driven)
  --plan-only                      emit one JSON line describing auto selection and exit
  --bitio-selftest                 run internal bit-IO selftest (global flag; before subcommand)

This file is forward-only; version strings intentionally change output identity across versions.
"""



from __future__ import annotations

import collections
import argparse
import concurrent.futures
import threading
import io
import json
import math
import os
import pathlib
import struct
import sys
import time

class PhaseTimer:
    __slots__ = ("t0","acc")
    def __init__(self) -> None:
        self.t0 = time.perf_counter()
        self.acc = {}  # phase -> seconds

    def mark(self, phase: str) -> None:
        t = time.perf_counter()
        dt = t - self.t0
        self.acc[phase] = self.acc.get(phase, 0.0) + dt
        self.t0 = t

    def report(self) -> str:
        items = sorted(self.acc.items(), key=lambda kv: (-kv[1], kv[0]))
        total = sum(v for _,v in items) or 1e-9
        parts = [f"{k}={v:.3f}s({(100.0*v/total):.1f}%)" for k,v in items]
        return " | ".join(parts)
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple

import hashlib
import zlib

# -----------------------------
# Optional deps
# -----------------------------
HAVE_ZSTD = False
try:
    import zstandard as zstd  # type: ignore
    HAVE_ZSTD = True
except Exception:
    HAVE_ZSTD = False


# Thread-local compressor cache (for parallel chunk compression without IPC overhead)
_TLS = threading.local()

def _get_zstd_compressor(level: int):
    """Return a per-thread cached ZstdCompressor for the given level."""
    if not HAVE_ZSTD:
        return None
    cache = getattr(_TLS, "zstd_cache", None)
    if cache is None:
        cache = {}
        _TLS.zstd_cache = cache
    c = cache.get(level)
    if c is None:
        # threads=1 for deterministic framing and to avoid internal worker variability
        c = zstd.ZstdCompressor(level=level, threads=1)
        cache[level] = c
    return c

HAVE_BLAKE3 = False
try:
    from blake3 import blake3  # type: ignore
    HAVE_BLAKE3 = True
except Exception:
    HAVE_BLAKE3 = False

# -----------------------------
# Versioning / format
# -----------------------------
# Tool (human-facing) version: bump when you tag a release.
TOOL_VERSION = "0.1.0"
# Build identifier: bump whenever output bytes may change (forward-only).
BUILD_ID = "phase3d5f_fix2_bitio12_repro2_auto2_fix16_pi_bypass1_dna_large2"
# Full version string stored in metadata/logs.
VERSION_STR = f"{TOOL_VERSION}+{BUILD_ID}"
__version__ = VERSION_STR

MAGIC = b"CZP3"
# Format version: bump ONLY when older readers cannot decode (hard break).
FORMAT_VERSION = 1  # CZP3 v1

FILE_HDR = struct.Struct("<4sH")  # magic, version
SEC_HDR = struct.Struct("<4sQ")   # tag, payload_len
MAX_SECTION = 2 * 1024 * 1024 * 1024  # 2GiB guard

TAG_HEAD = b"HEAD"  # JSON config+policies+stats
TAG_BLK2 = b"BLK2"  # micro block: many small files
TAG_CHNK = b"CHNK"  # normal block (whole or CDC chunk)
TAG_DNA1 = b"DNA1"  # DNA lane block: dict + tokens
TAG_PI01 = b"PI01"  # PI lane block: delta-from-base
TAG_FIDX = b"FIDX"  # file index
TAG_END  = b"END!"

# -----------------------------
# Codecs
# -----------------------------
CODEC_STORE = 0
CODEC_ZSTD  = 1
CODEC_ZLIB  = 2
CODEC_NAME = {CODEC_STORE: "store", CODEC_ZSTD: "zstd", CODEC_ZLIB: "zlib"}

# -----------------------------
# Span flags / packing
# -----------------------------
# low bits are type flags; upper bits can store auxiliary (e.g. micro entry index)
SPANF_LAST     = 1 << 0
SPANF_MICRO    = 1 << 1
SPANF_STORE    = 1 << 2
SPANF_DNA      = 1 << 3
SPANF_PI       = 1 << 4
SPANF_HIGHENT  = 1 << 5  # gate diagnostic: high entropy suppression active

# micro entry index stored in upper 16 bits of span_flags
def spanflags_with_aux(base_flags: int, aux_u16: int) -> int:
    return (base_flags & 0xFFFF) | ((aux_u16 & 0xFFFF) << 16)

def spanflags_aux(span_flags: int) -> int:
    return (span_flags >> 16) & 0xFFFF

def spanflags_base(span_flags: int) -> int:
    return span_flags & 0xFFFF

# -----------------------------
# Chunk/block headers
# -----------------------------
# CHNK payload header:
#   u64 block_id
#   u8  codec
#   u8  flags
#   u16 rsv
#   u32 raw_len
#   u32 comp_len
#   u32 raw_crc32
#   32B hash (for dedupe; all-zero if unused)
CHNK_HDR = struct.Struct("<QBBHIII32s")

# BLK2 header:
#   u64 block_id
#   u8  codec
#   u8  flags
#   u16 rsv
#   u32 raw_total
#   u32 comp_len
#   u32 raw_crc32
#   u32 entry_count
BLK2_HDR = struct.Struct("<QBBHIIII")
# BLK2 entry:
#   u32 file_index
#   u32 off
#   u32 len
#   u32 crc32
BLK2_ENTRY = struct.Struct("<IIII")

# DNA1 header:
#   u64 block_id
#   u8  codec_tokens
#   u8  flags
#   u16 motif_len (fixed for v1)
#   u16 motif_count
#   u16 rsv
#   u32 raw_len
#   u32 tokens_comp_len
#   u32 raw_crc32
DNA1_HDR = struct.Struct("<QBBHHHIII")

# PI01 header:
#   u64 block_id
#   u8  codec
#   u8  flags
#   u16 rsv
#   u32 base_file_index
#   u32 raw_len
#   u32 comp_len
#   u32 raw_crc32
PI01_HDR = struct.Struct("<QBBHIIII")

# -----------------------------
# FIDX encoding
# -----------------------------
FILEIDX_HEAD = struct.Struct("<I")
# File record:
# u16 path_len
# u64 mtime_ns
# u64 raw_size
# u32 file_crc32
# u32 span_count
# u8  class_id
# u8  file_flags
# u16 rsv
FILE_REC_HDR = struct.Struct("<HQQII BBH")
# Span record: u64 block_id, u32 span_raw_len, u32 span_flags
SPAN_REC = struct.Struct("<QII")

# -----------------------------
# Defaults / knobs
# -----------------------------
IO_CHUNK = 1024 * 1024

DEF_CHUNK_SIZE = 256 * 1024

DEF_PROBE_BYTES = 65536
DEF_PROBE_LEVEL = 1
DEF_THR = 0.05
DEF_THR_SMALL = 0.02
DEF_SMALL_CUTOFF = 256 * 1024
DEF_WHOLE_MAX = 32 * 1024 * 1024

DEF_MIN_GAIN = 0.02
DEF_DEDUPE = True

# -----------------------------
# Python-first performance options (phase3d5f_fix2)
# -----------------------------
# Goals:
#   - Stay deterministic.
#   - Avoid Python-heavy work (probing/feature extraction) on likely high-entropy blobs.
#   - Optional multiprocessing for *cheap* gating and (optionally) CDC-chunk compression of very large files.

FASTGAME_EXT_MEDIA = {
    "png","jpg","jpeg","webp","gif","bmp","tga",
    "mp4","mkv","avi","mov","webm",
    "mp3","ogg","flac","wav",
    "zip","7z","rar","gz","bz2","xz","zst",
    "pak","wad","dat","bin","iso","jar","class",
}

FASTGAME_PROBE_BYTES = 16 * 1024
FASTGAME_PROBE_LEVEL = 1
FASTGAME_BIGFILE_CUTOFF = 8 * 1024 * 1024
FASTGAME_BIG_CHUNK = 1024 * 1024

DEF_PRESET = "auto"
DEF_MP_GATE = False
DEF_MP_CDC_BIG = False
DEF_MP_CDC_CUTOFF = 64 * 1024 * 1024  # only offload *very* large CDC files by default
DEF_PROFILE = False

MICRO_ENABLE = True
MICRO_CUTOFF = 64 * 1024
MICRO_BLOCK_MAX = 4 * 1024 * 1024
MICRO_ENTRY_MAX = 4096

# DNA lane v1
DNA_ENABLE = True
DNA_MOTIF_LEN = 32
DNA_MAX_MOTIFS = 512
DNA_MIN_MOTIFS = 16
DNA_SCORE_THR = 0.12          # repeated-shingle density threshold
DNA_ONLY_TEXTLIKE = True      # avoid false positives on containers
DNA_TEXTLIKE_EXT = {
    "txt","md","json","xml","yaml","yml","csv","tsv","log","ini","cfg",
    "py","js","ts","css","html","htm","c","cpp","h","hpp","rs","go","java","cs",
    "tex","rtf","srt","ass"
}


# File class buckets (for threshold floors / probe policy)
CLASS_TEXT_EXT = set(DNA_TEXTLIKE_EXT) | {
    "txt","md","rst","log","csv","tsv","ini","cfg","conf","toml","yaml","yml",
    "json","xml","html","htm","css","js","ts","py","java","c","cc","cpp","h","hpp",
    "go","rs","cs","php","rb","pl","lua","sql","sh","bat","ps1","nut","lng",
    "make","mk","cmake","gradle","bazel","bzl",
}
CLASS_DOC_EXT = {
    "pdf","doc","docx","ppt","pptx","xls","xlsx","odt","odp","ods","rtf","epub",
}
CLASS_MEDIA_EXT = {
    "jpg","jpeg","png","gif","bmp","webp","tif","tiff","heic",
    "mp4","mov","m4v","webm","mkv","avi","wmv","flv",
    "mp3","aac","m4a","ogg","opus","flac","wav",
    "zip","7z","rar","gz","bz2","xz","zst",
}
# For media-like types, probe multiple offsets to avoid header-only false positives
MEDIA_PROBE_OFFSETS = (0.0, 0.25, 0.75)

def file_class_for(ext: str) -> str:
    ext = (ext or "").lower()
    if ext in CLASS_MEDIA_EXT:
        return "media"
    if ext in CLASS_DOC_EXT:
        return "doc"
    if ext in CLASS_TEXT_EXT:
        return "text"
    return "bin"

# Per-class threshold floors (prevents EMA collapse from bleeding into all types)
THR_FLOOR_BY_CLASS = {
    "text": 0.00,
    "doc": 0.03,
    "bin": 0.12,
    "media": 0.30,
}

# PI lane v1 (delta-from-base)
PI_ENABLE = True
PI_SIM_THR = 0.20             # Jaccard-ish sample similarity threshold
PI_BH_BYPASS_SIM = 0.80      # allow PI even under BH suppression when similarity is very high
PI_MAX_BASE_CAND = 16         # compare against last K matching-ext files
PI_BASE_STORE_MAX = 256  # bounded in-memory PI base cache (bytes)

PI_MIN_RAW = 32 * 1024        # don't bother for tiny files
PI_MAX_RAW = 256 * 1024 * 1024  # safety cap for in-memory delta v1
DNA_MAX_RAW = 256 * 1024 * 1024  # allow DNA lane up to this raw size (bytes)

# black-hole policy (work suppression near entropy horizon)
BH_ENABLE = True
BH_ENT0 = 0.965               # start suppression
BH_ENT1 = 0.995               # near horizon
BH_GAMMA = 2.0                # work factor exponent
BH_THR_BUMP_MAX = 0.20        # how much threshold can increase near horizon

# invariant policy: deterministic per-archive adaptive thr nudging
INV_ENABLE = True
INV_EMA_ALPHA = 0.12          # EMA of observed whole/CDC gain
INV_THR_STEP_MAX = 0.010      # clamp adjust per file
INV_TARGET_GAIN = 0.06        # try to keep CDC decisions above this expected gain

# CDC overhead model (used only as a veto/penalty)
OVERHEAD_PER_CHUNK = 96

# -----------------------------
# Utilities
# -----------------------------
def now_utc_ns() -> int:
    return time.time_ns()

def source_date_epoch_ns() -> Optional[int]:
    """
    Reproducible-build timestamp source.

    If the environment variable SOURCE_DATE_EPOCH is set (integer seconds since Unix epoch),
    return it in nanoseconds. Otherwise return None.
    """
    v = os.environ.get("SOURCE_DATE_EPOCH")
    if not v:
        return None
    try:
        sec = int(v.strip())
        if sec < 0:
            return None
        return sec * 1_000_000_000
    except Exception:
        return None
    return time.time_ns()

def ensure_parent(p: pathlib.Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def crc32_update(crc: int, data: bytes) -> int:
    return zlib.crc32(data, crc) & 0xFFFFFFFF

def crc32_bytes(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF

def stable_hash32(data: bytes) -> bytes:
    if HAVE_BLAKE3:
        return blake3(data).digest(length=32)
    return hashlib.sha256(data).digest()

def relpath_str(root: pathlib.Path, p: pathlib.Path) -> str:
    try:
        rp = str(p.relative_to(root))
    except Exception:
        rp = str(p)
    return rp.replace("\\", "/")

def u32(x: int) -> int:
    return x & 0xFFFFFFFF

# -----------------------------
# Varint (for PI ops + DNA tokens)
# -----------------------------
def put_uvarint(n: int) -> bytes:
    if n < 0:
        raise ValueError("uvarint requires non-negative")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def get_uvarint(buf: bytes, off: int) -> Tuple[int, int]:
    shift = 0
    val = 0
    while True:
        if off >= len(buf):
            raise ValueError("truncated uvarint")
        b = buf[off]
        off += 1
        val |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return val, off
        shift += 7
        if shift > 63:
            raise ValueError("uvarint too large")


# -----------------------------
# Bit IO (deterministic by default)
# -----------------------------
class BitWriter:
    """
    Deterministic bitstream writer.

    Canonical choices (MUST remain stable for CZP bit-identical reproducibility):
      - Bit order: MSB-first within each byte (bit 7 -> bit 0).
      - Final partial byte padding: zero bits.
      - No implicit alignment: caller must request byte alignment explicitly.

    This class is intentionally small and pure-Python to avoid platform variability.
    """

    __slots__ = ("_out", "_acc", "_nbits")

    def __init__(self) -> None:
        self._out = bytearray()
        self._acc = 0      # accumulator for pending bits (stored in the low bits)
        self._nbits = 0    # number of valid bits currently in _acc (0..7)

    def tell_bits(self) -> int:
        """Total bits written so far (including pending bits)."""
        return (len(self._out) * 8) + self._nbits

    def put_bits(self, value: int, nbits: int) -> None:
        """
        Append exactly `nbits` bits from `value` to the stream.

        Bits are taken from the *most significant* side of the nbits window:
          value's bit (nbits-1) is written first.

        Raises:
            ValueError: if nbits is negative or value doesn't fit in nbits.
        """
        if nbits < 0:
            raise ValueError("nbits must be >= 0")
        if nbits == 0:
            return
        if value < 0 or value >= (1 << nbits):
            raise ValueError("value does not fit in nbits")

        # Push bits MSB-first.
        for i in range(nbits - 1, -1, -1):
            bit = (value >> i) & 1
            self._acc = (self._acc << 1) | bit
            self._nbits += 1
            if self._nbits == 8:
                self._out.append(self._acc & 0xFF)
                self._acc = 0
                self._nbits = 0

    def align_to_byte(self) -> None:
        """Pad with zero bits until the stream is byte-aligned."""
        if self._nbits:
            pad = 8 - self._nbits
            self.put_bits(0, pad)

    def finish(self) -> bytes:
        """
        Finalize the stream, returning bytes.

        Behavior:
          - If there is a partial final byte, it is zero-padded on the right (LSB side),
            consistent with MSB-first emission.
        """
        if self._nbits:
            self._acc <<= (8 - self._nbits)
            self._out.append(self._acc & 0xFF)
            self._acc = 0
            self._nbits = 0
        return bytes(self._out)


class BitReader:
    """
    Deterministic bitstream reader matching BitWriter (MSB-first, canonical).

    The reader does not attempt to infer or validate padding: the higher-level format
    must specify exact bit lengths, or enforce byte alignment using align_to_byte().
    """

    __slots__ = ("_buf", "_off", "_acc", "_nbits")

    def __init__(self, buf: bytes) -> None:
        self._buf = buf
        self._off = 0
        self._acc = 0
        self._nbits = 0  # bits available in _acc

    def remaining_bits(self) -> int:
        return ((len(self._buf) - self._off) * 8) + self._nbits

    def bits_remaining(self) -> int:
        """Backward-compatible alias for remaining_bits()."""
        return self.remaining_bits()


    def get_bits(self, nbits: int) -> int:
        """
        Read exactly `nbits` bits and return them as an integer.

        The first bit read becomes the most significant bit of the returned value.
        """
        if nbits < 0:
            raise ValueError("nbits must be >= 0")
        if nbits == 0:
            return 0

        val = 0
        for _ in range(nbits):
            if self._nbits == 0:
                if self._off >= len(self._buf):
                    raise ValueError("truncated bitstream")
                self._acc = self._buf[self._off]
                self._off += 1
                self._nbits = 8
            bit = (self._acc >> (self._nbits - 1)) & 1
            self._nbits -= 1
            val = (val << 1) | bit
        return val

    def align_to_byte(self) -> None:
        """Discard any remaining bits in the current partial byte."""
        self._nbits = 0

    def tell_bits(self) -> int:
        """Bit position from start of buffer."""
        return (self._off * 8) - self._nbits


def _bitio_selftest(verbose: bool = False) -> bool:
    """
    Internal self-test for canonical bit IO.

    Design goals:
      - Deterministic: no randomness, time, or platform-dependent inputs.
      - Fast: safe to run by default.
      - Canonical: MSB-first, explicit zero-padding on byte flush, strict decoding.
    """
    # pattern: alternating widths
    seq = [(0b1, 1), (0b01, 2), (0b101, 3), (0b11110000, 8), (0b0, 1), (0xABCD, 16)]
    bw = BitWriter()
    for v, n in seq:
        bw.put_bits(v, n)
    bw.align_to_byte()
    blob = bw.finish()

    br = BitReader(blob)
    out = []
    for _, n in seq:
        out.append(br.get_bits(n))
    br.align_to_byte()
    if br.bits_remaining() != 0:
        raise AssertionError("bitio selftest: reader did not end on byte boundary after align")

    if [v for v, _ in seq] != out:
        raise AssertionError(f"bitio selftest: roundtrip mismatch: {out} != {[v for v,_ in seq]}")

    # canonicality: identical emission across independent writers
    bw2 = BitWriter()
    for v, n in seq:
        bw2.put_bits(v, n)
    bw2.align_to_byte()
    blob2 = bw2.finish()
    if blob2 != blob:
        raise AssertionError("bitio selftest: non-deterministic emission")

    if verbose:
        print("OK: bit-IO selftest")
    return True



# -----------------------------
# Metrics / gating primitives
# -----------------------------
def shannon_entropy_8bit(sample: bytes) -> float:
    if not sample:
        return 0.0
    freq = [0] * 256
    for b in sample:
        freq[b] += 1
    n = float(len(sample))
    ent = 0.0
    for c in freq:
        if c:
            p = c / n
            ent -= p * math.log2(p)
    return ent / 8.0  # normalized ~[0,1]

def uniq_ratio_2b(sample: bytes) -> float:
    if len(sample) < 2:
        return 1.0
    seen = set()
    mv = memoryview(sample)
    for i in range(0, len(sample) - 1):
        seen.add(int.from_bytes(mv[i:i+2], "little", signed=False))
        if len(seen) > 65535:
            break
    return min(1.0, len(seen) / max(1, (len(sample) - 1)))

def compress_probe_len(sample: bytes, level: int) -> Tuple[int, int]:
    if not sample:
        return CODEC_STORE, 0
    if HAVE_ZSTD:
        c = zstd.ZstdCompressor(level=level)
        comp = c.compress(sample)
        return CODEC_ZSTD, len(comp)
    comp = zlib.compress(sample, level=min(9, max(1, level)))
    return CODEC_ZLIB, len(comp)

def choose_level_from_gain(gain: float) -> int:
    if gain >= 0.30:
        return 7
    if gain >= 0.18:
        return 5
    return 3

def looks_like_container(ext: str, sample: bytes) -> bool:
    e = ext.lower().lstrip(".")
    if e in {
        "mp4","m4v","mov","mkv","webm","avi","mp3","m4a","aac","flac","ogg",
        "jpg","jpeg","png","gif","webp","heic","heif",
        "zip","7z","rar","gz","bz2","xz","zst","lz4",
        "pdf","docx","pptx","xlsx"
    }:
        return True
    # tiny magic sniff
    if sample.startswith(b"%PDF"):
        return True
    if sample.startswith(b"PK\x03\x04"):
        return True
    return False

# -----------------------------
# Black-hole suppression (policy overlay)
# -----------------------------
def bh_work_factor(ent: float) -> float:
    if not BH_ENABLE:
        return 1.0
    if ent <= BH_ENT0:
        return 1.0
    if ent >= BH_ENT1:
        return 0.0
    t = (ent - BH_ENT0) / (BH_ENT1 - BH_ENT0)
    t = max(0.0, min(1.0, t))
    # as ent approaches horizon, work collapses
    return max(0.0, (1.0 - t) ** BH_GAMMA)

def bh_thr_bump(ent: float) -> float:
    """Increase threshold near horizon to avoid wasted CDC/structure work."""
    if not BH_ENABLE:
        return 0.0
    if ent <= BH_ENT0:
        return 0.0
    if ent >= BH_ENT1:
        return BH_THR_BUMP_MAX
    t = (ent - BH_ENT0) / (BH_ENT1 - BH_ENT0)
    t = max(0.0, min(1.0, t))
    return BH_THR_BUMP_MAX * t

# -----------------------------
# Invariant policy (deterministic per-build)
# -----------------------------
@dataclass
class InvariantState:
    ema_gain: float = 0.0   # EMA of realized/estimated gain
    thr_bias: float = 0.0   # additive bias applied to thr

    def update(self, observed_gain: float) -> None:
        if not INV_ENABLE:
            return
        a = INV_EMA_ALPHA
        self.ema_gain = (1.0 - a) * self.ema_gain + a * observed_gain
        # If ema is below target, raise thr (be more conservative); else slightly relax.
        delta = 0.0
        if self.ema_gain < INV_TARGET_GAIN:
            delta = +min(INV_THR_STEP_MAX, (INV_TARGET_GAIN - self.ema_gain) * 0.05)
        else:
            delta = -min(INV_THR_STEP_MAX, (self.ema_gain - INV_TARGET_GAIN) * 0.02)
        self.thr_bias = max(-0.05, min(0.20, self.thr_bias + delta))


@dataclass
class MultiInvariantState:
    # Keep a global channel ("all") for legacy stats, plus per-class channels.
    states: Dict[str, InvariantState] = field(default_factory=lambda: {"all": InvariantState()})

    def get(self, class_tag: str) -> InvariantState:
        k = class_tag or "bin"
        if k not in self.states:
            self.states[k] = InvariantState()
        return self.states[k]

    def update(self, class_tag: str, observed_gain: float) -> None:
        # Always update global + class
        self.states["all"].update(observed_gain)
        self.get(class_tag).update(observed_gain)

# -----------------------------
# DNA lane: motif scoring + encoding
# -----------------------------
def dna_motif_score(sample: bytes, motif_len: int = DNA_MOTIF_LEN) -> float:
    """
    Very fast repeated-shingle density metric.
    - hashes fixed-length shingles, counts repeats.
    - score ~ repeats / total_windows
    """
    if len(sample) < motif_len * 2:
        return 0.0
    stride = 4
    total = 0
    repeats = 0
    seen: Dict[int, int] = {}
    mv = memoryview(sample)
    for i in range(0, len(sample) - motif_len + 1, stride):
        h = zlib.crc32(mv[i:i+motif_len]) & 0xFFFFFFFF
        c = seen.get(h, 0)
        if c >= 1:
            repeats += 1
        seen[h] = c + 1
        total += 1
        if total >= 4096:
            break
    if total <= 0:
        return 0.0
    return repeats / float(total)

def dna_build_motifs_from_sample(sample: bytes, motif_len: int, max_motifs: int) -> List[bytes]:
    """Pick most frequent motif_len shingles from sample (bounded)."""
    if len(sample) < motif_len:
        return []
    stride = 4
    counts: Dict[int, int] = {}
    firstpos: Dict[int, int] = {}
    mv = memoryview(sample)
    for i in range(0, len(sample) - motif_len + 1, stride):
        h = zlib.crc32(mv[i:i+motif_len]) & 0xFFFFFFFF
        counts[h] = counts.get(h, 0) + 1
        if h not in firstpos:
            firstpos[h] = i
        if len(counts) > 20000:
            break
    # select top frequent hashes
    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    motifs: List[bytes] = []
    used = set()
    for h, c in items:
        if c < 2:
            break
        if h in used:
            continue
        i = firstpos[h]
        m = bytes(mv[i:i+motif_len])
        # avoid duplicates
        if m in used:
            continue
        motifs.append(m)
        used.add(h)
        if len(motifs) >= max_motifs:
            break
    return motifs

def dna_encode_bytes(raw: bytes, motifs: List[bytes], motif_len: int) -> bytes:
    """
    Token stream format (v1):
      stream = sequence of records:
        0x00 LIT: uvarint(len) + bytes
        0x01 REF: uvarint(motif_id)  (implies fixed motif_len)
    Greedy scan: if next motif_len bytes match a motif hash, emit REF; else extend literal.
    """
    if not raw:
        return b""
    # map crc32(motif) -> (id, motif_bytes)  (resolve collisions by byte compare)
    mtab: Dict[int, List[Tuple[int, bytes]]] = {}
    for idx, m in enumerate(motifs):
        h = zlib.crc32(m) & 0xFFFFFFFF
        mtab.setdefault(h, []).append((idx, m))

    out = bytearray()
    i = 0
    lit_start = 0
    mv = memoryview(raw)

    def flush_lit(to_i: int) -> None:
        nonlocal lit_start
        if to_i > lit_start:
            out.append(0x00)
            out.extend(put_uvarint(to_i - lit_start))
            out.extend(mv[lit_start:to_i])
        lit_start = to_i

    while i + motif_len <= len(raw):
        h = zlib.crc32(mv[i:i+motif_len]) & 0xFFFFFFFF
        cand = mtab.get(h)
        matched = False
        if cand:
            seg = mv[i:i+motif_len].tobytes()
            for mid, m in cand:
                if seg == m:
                    flush_lit(i)
                    out.append(0x01)
                    out.extend(put_uvarint(mid))
                    i += motif_len
                    lit_start = i
                    matched = True
                    break
        if not matched:
            i += 1

    flush_lit(len(raw))
    return bytes(out)

def dna_decode_tokens(tokens: bytes, motifs: List[bytes], motif_len: int, raw_len: int) -> bytes:
    out = bytearray()
    i = 0
    while i < len(tokens):
        tag = tokens[i]
        i += 1
        if tag == 0x00:
            ln, i = get_uvarint(tokens, i)
            if i + ln > len(tokens):
                raise ValueError("DNA tokens truncated (lit)")
            out.extend(tokens[i:i+ln])
            i += ln
        elif tag == 0x01:
            mid, i = get_uvarint(tokens, i)
            if mid >= len(motifs):
                raise ValueError("DNA bad motif id")
            out.extend(motifs[mid])
        else:
            raise ValueError(f"DNA bad token tag {tag}")
        if len(out) > raw_len + 16:
            raise ValueError("DNA decode overflow")
    if len(out) != raw_len:
        raise ValueError(f"DNA decode size mismatch {len(out)} != {raw_len}")
    return bytes(out)

# -----------------------------
# PI lane: delta-from-base (copy/add)
# -----------------------------
# ops stream (v1) is a varint-coded sequence:
#  OP_ADD  = 0  => [0][len][bytes]
#  OP_COPY = 1  => [1][off][len]
OP_ADD = 0
OP_COPY = 1

def pi_sample_signature(sample: bytes, motif_len: int = 32) -> List[int]:
    """Return small set of hashed shingles for similarity."""
    if len(sample) < motif_len:
        return []
    stride = 8
    sig: List[int] = []
    mv = memoryview(sample)
    for i in range(0, len(sample) - motif_len + 1, stride):
        sig.append(zlib.crc32(mv[i:i+motif_len]) & 0xFFFFFFFF)
        if len(sig) >= 512:
            break
    return sig

def pi_similarity(sig_a: List[int], sig_b: List[int]) -> float:
    if not sig_a or not sig_b:
        return 0.0
    sa = set(sig_a)
    sb = set(sig_b)
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / float(union if union else 1)



def simhash64_from_sample(sample: bytes, *, max_bytes: int = 8192, step: int = 4) -> int:
    """Fast 64-bit SimHash-like fingerprint over 4-grams using crc32; deterministic, cheap."""
    if not sample:
        return 0
    n = min(len(sample), max_bytes)
    mv = memoryview(sample)[:n]
    acc = [0] * 64
    # 4-grams with coarse step to stay fast in Python
    for i in range(0, max(0, n - 3), step):
        gram = mv[i:i+4]
        h1 = zlib.crc32(gram) & 0xFFFFFFFF
        h2 = zlib.crc32(gram, 0xFFFFFFFF) & 0xFFFFFFFF
        h = (h2 << 32) | h1
        for b in range(64):
            acc[b] += 1 if (h >> b) & 1 else -1
    fp = 0
    for b in range(64):
        if acc[b] >= 0:
            fp |= (1 << b)
    return fp

def hamming64(a: int, b: int) -> int:
    return int((a ^ b).bit_count())

@dataclass
class PIIndex:
    """Bounded candidate index for PI base selection (per-archive, deterministic)."""
    by_ext_bucket: Dict[str, Dict[int, List[Tuple[int, int, List[int]]]]] = field(default_factory=dict)
    bucket_bits: int = 12
    bucket_max_list: int = 128

    def _bucket(self, fp: int) -> int:
        return int((fp >> (64 - self.bucket_bits)) & ((1 << self.bucket_bits) - 1))

    def add(self, file_index: int, ext: str, sig: List[int], fp: int) -> None:
        if not ext:
            ext = ""
        b = self._bucket(fp)
        ex = self.by_ext_bucket.setdefault(ext, {})
        lst = ex.setdefault(b, [])
        lst.append((file_index, fp, sig))
        # keep bounded (most recent)
        if len(lst) > self.bucket_max_list:
            del lst[0:len(lst) - self.bucket_max_list]

    def query(self, ext: str, fp: int, *, k: int = 3) -> List[Tuple[int, float, List[int]]]:
        """Return list of candidate bases as (file_index, approx_sim, sig)."""
        if not ext:
            ext = ""
        ex = self.by_ext_bucket.get(ext)
        if not ex:
            return []
        b = self._bucket(fp)
        # same bucket + immediate neighbors (cheap)
        buckets = (b, (b - 1) & ((1 << self.bucket_bits) - 1), (b + 1) & ((1 << self.bucket_bits) - 1))
        cands: List[Tuple[int, int, List[int]]] = []
        for bb in buckets:
            cands.extend(ex.get(bb, []))
        if not cands:
            return []
        scored: List[Tuple[int, int, List[int]]] = []
        for (fi, fp2, sig) in cands:
            scored.append((fi, hamming64(fp, fp2), sig))
        scored.sort(key=lambda t: (t[1], t[0]))  # deterministic
        out: List[Tuple[int, float, List[int]]] = []
        for fi, ham, sig in scored[:max(1, k)]:
            approx_sim = 1.0 - (ham / 64.0)
            out.append((fi, approx_sim, sig))
        return out

def pi_delta_encode(raw: bytes, base: bytes) -> bytes:
    """
    Simple bounded delta:
      - build hash->positions map on base for 32-byte windows (stride 8)
      - scan raw; when match found, extend match; emit COPY else ADD runs
    This is not "best" delta, but fast enough for v1.
    """
    if not raw:
        return b""
    if not base:
        # all add
        return bytes([OP_ADD]) + put_uvarint(len(raw)) + raw

    W = 32
    stride = 8
    base_map: Dict[int, List[int]] = {}
    mvb = memoryview(base)
    for i in range(0, max(0, len(base) - W + 1), stride):
        h = zlib.crc32(mvb[i:i+W]) & 0xFFFFFFFF
        lst = base_map.get(h)
        if lst is None:
            base_map[h] = [i]
        else:
            if len(lst) < 8:
                lst.append(i)
    out = bytearray()
    mvr = memoryview(raw)
    i = 0
    lit_start = 0

    def emit_add(end: int) -> None:
        nonlocal lit_start
        if end > lit_start:
            out.append(OP_ADD)
            out.extend(put_uvarint(end - lit_start))
            out.extend(mvr[lit_start:end])
        lit_start = end

    while i + W <= len(raw):
        h = zlib.crc32(mvr[i:i+W]) & 0xFFFFFFFF
        pos_list = base_map.get(h)
        best_off = -1
        best_len = 0
        if pos_list:
            seg = mvr[i:i+W].tobytes()
            for off0 in pos_list:
                if mvb[off0:off0+W].tobytes() != seg:
                    continue
                # extend match
                j = W
                # cap extension to avoid worst-case
                cap = 256 * 1024
                while (i + j < len(raw)) and (off0 + j < len(base)) and (mvr[i+j] == mvb[off0+j]) and (j < cap):
                    j += 1
                if j > best_len:
                    best_len = j
                    best_off = off0
                if best_len >= 4096:
                    break
        if best_len >= W:
            emit_add(i)
            out.append(OP_COPY)
            out.extend(put_uvarint(best_off))
            out.extend(put_uvarint(best_len))
            i += best_len
            lit_start = i
        else:
            i += 1

    emit_add(len(raw))
    return bytes(out)

def pi_delta_decode(ops: bytes, base: bytes, raw_len: int) -> bytes:
    out = bytearray()
    i = 0
    while i < len(ops):
        op = ops[i]
        i += 1
        if op == OP_ADD:
            ln, i = get_uvarint(ops, i)
            if i + ln > len(ops):
                raise ValueError("PI ops truncated (add)")
            out.extend(ops[i:i+ln])
            i += ln
        elif op == OP_COPY:
            off, i = get_uvarint(ops, i)
            ln, i = get_uvarint(ops, i)
            if off + ln > len(base):
                raise ValueError("PI copy out of range")
            out.extend(base[off:off+ln])
        else:
            raise ValueError(f"PI bad op {op}")
        if len(out) > raw_len + 16:
            raise ValueError("PI decode overflow")
    if len(out) != raw_len:
        raise ValueError(f"PI decode size mismatch {len(out)} != {raw_len}")
    return bytes(out)

# -----------------------------
# Compression helpers
# -----------------------------
def compress_bytes(data: bytes, prefer_codec: int, level: int) -> Tuple[int, bytes]:
    if not data:
        return CODEC_STORE, b""
    if prefer_codec == CODEC_STORE:
        return CODEC_STORE, data
    if prefer_codec == CODEC_ZSTD and HAVE_ZSTD:
        c = _get_zstd_compressor(level)
        if c is None:
            return CODEC_ZLIB, zlib.compress(data, level=min(9, max(1, level)))
        return CODEC_ZSTD, c.compress(data)
    if prefer_codec == CODEC_ZLIB:
        return CODEC_ZLIB, zlib.compress(data, level=min(9, max(1, level)))
    # auto
    if HAVE_ZSTD:
        c = _get_zstd_compressor(level)
        if c is None:
            return CODEC_ZLIB, zlib.compress(data, level=min(9, max(1, level)))
        return CODEC_ZSTD, c.compress(data)
    return CODEC_ZLIB, zlib.compress(data, level=min(9, max(1, level)))

def decompress_bytes(codec: int, comp: bytes, raw_len: int) -> bytes:
    if codec == CODEC_STORE:
        return comp
    if codec == CODEC_ZSTD:
        if not HAVE_ZSTD:
            raise RuntimeError("zstandard not installed; cannot decode zstd")
        d = zstd.ZstdDecompressor()
        return d.decompress(comp, max_output_size=raw_len)
    if codec == CODEC_ZLIB:
        return zlib.decompress(comp)
    raise ValueError(f"unknown codec {codec}")

# -----------------------------
# Parallel compression worker (deterministic output; main thread preserves write order)
# -----------------------------
def _compress_task(payload: Tuple[bytes, int, int, float]) -> Tuple[int, bytes]:
    """Worker-safe wrapper: returns (codec_id, comp_bytes).
    Applies the same min_gain store-fallback rule used in the main loop.
    """
    data, prefer_codec_id, level, min_gain = payload
    codec, comp = compress_bytes(data, prefer_codec_id, level)
    if codec != CODEC_STORE:
        gain = 1.0 - (len(comp) / float(len(data) if data else 1))
        if gain < min_gain:
            codec, comp = CODEC_STORE, data
    return int(codec), comp


# -----------------------------
# Gate state machine
# -----------------------------
class GateState(Enum):
    RAW_ENTROPIC = 1
    GENERIC_COMPRESSIBLE = 2
    CONTAINER_ENTROPIC = 3
    CONTAINER_COMPRESSIBLE = 4
    STRUCT_MOTIF = 5
    STRUCT_GENERATIVE = 6

@dataclass
class Features:
    size: int
    ext: str
    probe_gain: float
    entropy: float
    uniq: float
    is_container: bool
    motif_score: float
    pi_sig: List[int]
    pi_hash64: int
    pi_sim_hint: float
    class_tag: str

@dataclass
class Plan:
    state: GateState
    action: str  # "CDC","WHOLE","MICRO","DNA","PI"
    eff_chunk: int
    eff_level: int
    thr_used: float
    flags: int
    dbg: Dict[str, object]


def compute_features(path: pathlib.Path, probe_bytes: int, probe_level: int) -> Features:
    raw_size = path.stat().st_size
    ext = path.suffix.lower().lstrip(".")
    class_tag = file_class_for(ext)

    # Probe policy:
    # - default: probe at start only
    # - media-like: probe multiple offsets to avoid compressible headers bias
    samples: List[bytes] = []
    ent_list: List[float] = []
    uniq_list: List[float] = []
    gain_list: List[float] = []

    with path.open("rb") as f:
        if raw_size <= 0:
            samples = [b""]
        elif class_tag == "media" and raw_size > probe_bytes * 2:
            for frac in MEDIA_PROBE_OFFSETS:
                off = int(frac * raw_size)
                # clamp: avoid seeking past EOF
                off = max(0, min(max(0, raw_size - 1), off))
                try:
                    f.seek(off)
                except OSError:
                    f.seek(0)
                s = f.read(min(probe_bytes, raw_size - off))
                if not s and off != 0:
                    f.seek(0)
                    s = f.read(min(probe_bytes, raw_size))
                samples.append(s)
        else:
            samples = [f.read(min(probe_bytes, raw_size))]

    for s in samples:
        ent_list.append(shannon_entropy_8bit(s))
        uniq_list.append(uniq_ratio_2b(s))
        _c, clen = compress_probe_len(s, level=probe_level)
        g = 0.0
        if s:
            g = 1.0 - (clen / float(len(s)))
        gain_list.append(g)

    # conservative aggregation:
    # - media: min gain (protect against header-only optimism)
    # - others: first-sample gain (already in gain_list[0])
    probe_gain = float(min(gain_list) if (class_tag == "media" and len(gain_list) > 1) else gain_list[0])
    ent = float(sum(ent_list) / max(1, len(ent_list)))
    uniq = float(sum(uniq_list) / max(1, len(uniq_list)))

    head_sample = samples[0] if samples else b""
    is_cont = looks_like_container(ext, head_sample)
    mscore = dna_motif_score(head_sample) if DNA_ENABLE else 0.0
    psig = pi_sample_signature(head_sample) if PI_ENABLE else []
    phash = simhash64_from_sample(head_sample) if PI_ENABLE else 0

    return Features(
        size=int(raw_size),
        ext=ext,
        probe_gain=probe_gain,
        entropy=ent,
        uniq=uniq,
        is_container=bool(is_cont),
        motif_score=float(mscore),
        pi_sig=psig,
        pi_hash64=int(phash),
        pi_sim_hint=0.0,
        class_tag=class_tag,
    )



# -----------------------------
# Fast-stage gating (Python-first)
# -----------------------------
class GateHint:
    """Deterministic per-file knobs applied *before* compute_features/plan selection.

    These are intentionally coarse: they reduce probing cost and disable expensive lanes
    for files that are very likely high-entropy or already-compressed.
    """
    __slots__ = ("skip_deep","disable_structure_lanes","force_whole","probe_bytes","probe_level","chunk_size")
    def __init__(self, *, skip_deep: bool, disable_structure_lanes: bool, force_whole: bool,
                 probe_bytes: int, probe_level: int, chunk_size: int) -> None:
        self.skip_deep = bool(skip_deep)
        self.disable_structure_lanes = bool(disable_structure_lanes)
        self.force_whole = bool(force_whole)
        self.probe_bytes = int(probe_bytes)
        self.probe_level = int(probe_level)
        self.chunk_size = int(chunk_size)

def stage1_gate_hint(path: pathlib.Path, *, preset: str, base_probe_bytes: int, base_probe_level: int,
                     base_chunk_size: int, whole_max: int) -> GateHint:
    """Cheap, deterministic gate hint based on extension + size + tiny sample stats.

    - Never reads more than FASTGAME_PROBE_BYTES (per sample).
    - Does *not* change default behavior unless preset == 'fast-game'.
    """
    if preset != "fast-game":
        return GateHint(skip_deep=False, disable_structure_lanes=False, force_whole=False,
                        probe_bytes=base_probe_bytes, probe_level=base_probe_level, chunk_size=base_chunk_size)

    ext = path.suffix.lower().lstrip(".")
    try:
        raw_size = path.stat().st_size
    except OSError:
        raw_size = 0

    # fast knobs
    probe_bytes = min(int(base_probe_bytes), FASTGAME_PROBE_BYTES)
    probe_level = min(int(base_probe_level), FASTGAME_PROBE_LEVEL)
    chunk_size = FASTGAME_BIG_CHUNK if raw_size >= FASTGAME_BIGFILE_CUTOFF else int(base_chunk_size)

    # Extension-first: known media / already-compressed -> skip deep work and strongly prefer WHOLE.
    if ext in FASTGAME_EXT_MEDIA:
        return GateHint(skip_deep=True, disable_structure_lanes=True, force_whole=True,
                        probe_bytes=probe_bytes, probe_level=probe_level, chunk_size=chunk_size)

    # Size heuristic: tiny files are already handled by WHOLE/MICRO; avoid deep probes.
    if raw_size <= max(whole_max, 256*1024):
        return GateHint(skip_deep=True, disable_structure_lanes=True, force_whole=False,
                        probe_bytes=probe_bytes, probe_level=probe_level, chunk_size=chunk_size)

    # Tiny sample entropy check (first probe_bytes only).
    try:
        with path.open("rb") as f:
            sample = f.read(min(probe_bytes, raw_size))
    except OSError:
        sample = b""

    ent = shannon_entropy_8bit(sample) if sample else 0.0
    if ent >= 7.85:  # very high entropy -> likely incompressible
        return GateHint(skip_deep=True, disable_structure_lanes=True, force_whole=True,
                        probe_bytes=probe_bytes, probe_level=probe_level, chunk_size=chunk_size)

    return GateHint(skip_deep=False, disable_structure_lanes=False, force_whole=False,
                    probe_bytes=probe_bytes, probe_level=probe_level, chunk_size=chunk_size)



# -----------------------------
# Auto preset (deterministic dataset-level selector)
# -----------------------------
AUTO_SAMPLE_FILES = 64
AUTO_SAMPLE_BYTES = 16 * 1024
AUTO_ENTROPY_HI = 0.97
AUTO_GAIN_LO = 0.03
AUTO_FASTGAME_SCORE_THR = 0.35  # fraction threshold

def auto_select_preset(
    paths: List[pathlib.Path],
    *,
    sample_files: int = AUTO_SAMPLE_FILES,
    sample_bytes: int = AUTO_SAMPLE_BYTES,
    entropy_hi: float = AUTO_ENTROPY_HI,
    gain_lo: float = AUTO_GAIN_LO,
    fastgame_score_thr: float = AUTO_FASTGAME_SCORE_THR,
    small_bytes_thr: int = 256 * 1024 * 1024,
    small_files_thr: int = 8192,
) -> Tuple[str, Dict[str, float]]:
    """
    Deterministically choose between presets based on a small, bounded sample of inputs.

    This is **dataset-level**, runs before lane selection, and MUST remain deterministic:
      - stable ordering (paths must already be canonical-sorted by caller)
      - bounded reads (sample_bytes)
      - fixed thresholds unless explicitly overridden

    Returns:
      (preset_effective, stats) where stats is safe to embed in archive HEAD.
    """
    total_files = int(len(paths))
    if total_files <= 0:
        return "default", {"sampled": 0.0, "score": 0.0, "total_files": 0.0, "total_bytes_est": 0.0}


    # Fast-path by file count alone (avoid whole-tree stat() cost on some Windows setups).
    # If file count is already under the "small dataset" threshold, choose fast-game immediately.
    # We intentionally skip total byte accounting here to keep auto selection latency low and stable.
    if total_files <= int(small_files_thr):
        stats = {
            "sampled": 0.0,
            "score": 0.0,
            "media_frac": 0.0,
            "hi_ent_frac": 0.0,
            "lo_gain_frac": 0.0,
            "big_frac": 0.0,
            "total_files": float(total_files),
            "total_bytes_est": -1.0,
            "total_bytes_exact": -1.0,
            "reason": 1.0,  # 1 == SMALL_DATASET fast-path (file-count)
            "cfg_sample_files": float(sample_files),
            "cfg_sample_bytes": float(sample_bytes),
            "cfg_entropy_hi": float(entropy_hi),
            "cfg_gain_lo": float(gain_lo),
            "cfg_fastgame_score_thr": float(fastgame_score_thr),
            "cfg_small_bytes_thr": float(small_bytes_thr),
            "cfg_small_files_thr": float(small_files_thr),
        }
        return "fast-game", stats
    # Deterministic dataset-size estimate.
    #
    # Goal: keep auto selection *fast* without paying whole-tree stat() costs on large datasets.
    # - For tiny trees (<= 512 files): compute exact bytes (cheap).
    # - For any larger tree: compute a deterministic sampled estimate (prefix sample),
    #   and do NOT compute an exact total (avoid thousands of stat() calls on Windows).
    total_bytes_est = 0
    total_bytes_exact = None
    try:
        if total_files <= 512:
            s = 0
            for p in paths:
                try:
                    s += int(p.stat().st_size)
                except OSError:
                    pass
            total_bytes_exact = int(s)
            total_bytes_est = int(s)
        else:
            # Deterministic prefix sample for size estimate
            k = min(total_files, 1024)
            s = 0
            for p in paths[:k]:
                try:
                    s += int(p.stat().st_size)
                except OSError:
                    pass
            avg = (s / float(k)) if k > 0 else 0.0
            total_bytes_est = int(avg * float(total_files))
            total_bytes_exact = None
    except Exception:
        total_bytes_est = 0
        total_bytes_exact = None

    # Fast-path: small datasets default to fast-game to avoid Python-heavy probing overhead.
    # This is a *performance* default; it does NOT change determinism.
    if (total_bytes_est <= int(small_bytes_thr)):
        stats = {
            "sampled": 0.0,
            "score": 0.0,
            "media_frac": 0.0,
            "hi_ent_frac": 0.0,
            "lo_gain_frac": 0.0,
            "big_frac": 0.0,
            "total_files": float(total_files),
            "total_bytes_est": float(total_bytes_est),
            "total_bytes_exact": float(total_bytes_exact if total_bytes_exact is not None else -1),
            "reason": 1.0,  # 1 == SMALL_DATASET fast-path
            "cfg_sample_files": float(sample_files),
            "cfg_sample_bytes": float(sample_bytes),
            "cfg_entropy_hi": float(entropy_hi),
            "cfg_gain_lo": float(gain_lo),
            "cfg_fastgame_score_thr": float(fastgame_score_thr),
            "cfg_small_bytes_thr": float(small_bytes_thr),
            "cfg_small_files_thr": float(small_files_thr),
        }
        return "fast-game", stats

    n = min(total_files, int(sample_files))
    if n <= 0:
        return "default", {"sampled": 0.0, "score": 0.0, "total_files": float(total_files), "total_bytes_est": float(total_bytes_est)}

    scored = 0
    media = 0
    hi_ent = 0
    lo_gain = 0
    big = 0
    for i, p in enumerate(paths[:n]):
        try:
            ext = p.suffix.lower().lstrip(".")
            sz = int(p.stat().st_size)
        except OSError:
            ext = ""
            sz = 0
        if ext in FASTGAME_EXT_MEDIA:
            media += 1
        if sz >= FASTGAME_BIGFILE_CUTOFF:
            big += 1

        # Read bounded sample from start only (stable and cheap)
        try:
            with p.open("rb") as f:
                s = f.read(min(int(sample_bytes), max(0, sz)))
        except OSError:
            s = b""
        ent = shannon_entropy_8bit(s) if s else 0.0
        _c, clen = compress_probe_len(s, level=1)
        gain = (1.0 - (clen / float(len(s)))) if s else 0.0

        if ent >= float(entropy_hi):
            hi_ent += 1
        if gain <= float(gain_lo):
            lo_gain += 1

        # score bucket: any signal that "fast-game likely better"
        if (ext in FASTGAME_EXT_MEDIA) or (sz >= FASTGAME_BIGFILE_CUTOFF) or (ent >= float(entropy_hi)) or (gain <= float(gain_lo)):
            scored += 1
        # Deterministic early-exit: if we already have strong evidence for fast-game, stop sampling.
        # This reduces auto selection latency on huge datasets without changing determinism.
        k_seen = i + 1
        if k_seen >= 16:
            score_k = scored / float(k_seen)
            hi_k = hi_ent / float(k_seen)
            if (hi_k >= 0.75) and (score_k >= float(fastgame_score_thr)):
                n = k_seen
                break

    score = scored / float(n)
    stats = {
        "sampled": float(n),
        "score": float(score),
        "media_frac": float(media / float(n)),
        "hi_ent_frac": float(hi_ent / float(n)),
        "lo_gain_frac": float(lo_gain / float(n)),
        "big_frac": float(big / float(n)),
        "total_files": float(total_files),
        "total_bytes_est": float(total_bytes_est),
        "total_bytes_exact": float(total_bytes_exact if total_bytes_exact is not None else -1),
        "reason": 2.0,  # 2 == SAMPLE_SCORE
        "cfg_sample_files": float(sample_files),
        "cfg_sample_bytes": float(sample_bytes),
        "cfg_entropy_hi": float(entropy_hi),
        "cfg_gain_lo": float(gain_lo),
        "cfg_fastgame_score_thr": float(fastgame_score_thr),
        "cfg_small_bytes_thr": float(small_bytes_thr),
        "cfg_small_files_thr": float(small_files_thr),
    }
    preset_eff = "fast-game" if score >= float(fastgame_score_thr) else "default"

    return preset_eff, stats


def _mp_gate_worker(item: Tuple[int, str, str, int, int, int, int]) -> Tuple[int, GateHint]:
    i, p, preset, probe_bytes, probe_level, chunk_size, whole_max = item
    return i, stage1_gate_hint(pathlib.Path(p), preset=preset,
                              base_probe_bytes=probe_bytes, base_probe_level=probe_level,
                              base_chunk_size=chunk_size, whole_max=whole_max)
def classify_state(feat: Features) -> GateState:
    # horizon-ish entropic
    if feat.entropy >= 0.985 or feat.probe_gain <= 0.02:
        return GateState.CONTAINER_ENTROPIC if feat.is_container else GateState.RAW_ENTROPIC
    # structure candidates
    if DNA_ENABLE and (feat.motif_score >= DNA_SCORE_THR):
        if (not feat.is_container) and (not DNA_ONLY_TEXTLIKE or feat.ext in DNA_TEXTLIKE_EXT):
            return GateState.STRUCT_MOTIF
    # PI structure generally useful when similarity is strong; classification done after hint computed.
    return GateState.CONTAINER_COMPRESSIBLE if feat.is_container else GateState.GENERIC_COMPRESSIBLE

def choose_plan(
    path: pathlib.Path,
    feat: Features,
    inv: MultiInvariantState,
    *,
    chunk_size: int,
    thr: float,
    thr_small: float,
    small_cutoff: int,
    whole_max: int,
    min_gain: float,
    micro_enabled: bool,
    dedupe: bool,
    recent_pi_bases: List[Tuple[int, str, List[int]]],  # legacy (kept for compat)
    pi_index: PIIndex,
) -> Plan:
    raw_size = feat.size
    thr0 = thr_small if raw_size <= small_cutoff else thr

    # invariant nudging (deterministic, per-class)
    invc = inv.get(feat.class_tag)
    thr_eff = thr0 + invc.thr_bias

    # black-hole bump near horizon
    thr_eff += bh_thr_bump(feat.entropy)

    # per-class floor (prevents EMA collapse from dragging all classes)
    thr_eff = max(THR_FLOOR_BY_CLASS.get(feat.class_tag, 0.0), thr_eff)

    thr_eff = max(0.0, min(0.40, thr_eff))

    state = classify_state(feat)

    # PI sim hint: shortlist candidate bases via fingerprint index (bounded, deterministic)
    pi_best = (0.0, -1)
    if PI_ENABLE and (raw_size >= PI_MIN_RAW) and (raw_size <= PI_MAX_RAW):
        cands = pi_index.query(feat.ext, feat.pi_hash64, k=min(4, PI_MAX_BASE_CAND))
        best_sim = 0.0
        best_idx = -1
        for (fi, approx_sim, sig) in cands:
            sim = pi_similarity(feat.pi_sig, sig)
            # slight tie-break toward higher approx_sim (deterministic)
            if sim > best_sim or (sim == best_sim and approx_sim > 0 and fi < best_idx):
                best_sim = sim
                best_idx = fi
        feat.pi_sim_hint = best_sim
        if best_sim >= PI_SIM_THR:
            state = GateState.STRUCT_GENERATIVE
            pi_best = (best_sim, best_idx)
    # Work suppression near horizon (black-hole policy)
    work = bh_work_factor(feat.entropy)

    # PI bypass: if we have a very strong PI match, allow PI even when BH work collapses.
    pi_bypass = bool(PI_ENABLE and (pi_best[1] >= 0) and (pi_best[0] >= PI_BH_BYPASS_SIM))

    if work <= 0.05 and not pi_bypass:
        # near horizon: avoid CDC/structure
        state = GateState.CONTAINER_ENTROPIC if feat.is_container else GateState.RAW_ENTROPIC
    elif work <= 0.05 and pi_bypass:
        # Keep structural state so PI can fire.
        state = GateState.STRUCT_GENERATIVE

    # default behavior based on state
    # Choose eff chunk and level
    eff_level = choose_level_from_gain(feat.probe_gain) if work > 0.0 else 3
    eff_chunk = chunk_size

    if raw_size > whole_max:
        # Do not buffer huge whole files in memory.
        #
        # Exception: we *do* allow STRUCT lanes (PI/DNA) to operate on larger inputs,
        # but only up to explicit per-lane caps. Past that, we fall back to streaming CDC.
        allow_struct = state in (GateState.STRUCT_GENERATIVE, GateState.STRUCT_MOTIF)
        if not allow_struct:
            return Plan(
                state=state,
                action="CDC",
                eff_chunk=eff_chunk,
                eff_level=eff_level,
                thr_used=thr_eff,
                flags=(SPANF_HIGHENT if state in (GateState.RAW_ENTROPIC, GateState.CONTAINER_ENTROPIC) else 0),
                dbg={
                    "force_stream": True,
                    "reason": "LARGE_FILE_STREAMING",
                    "work": work,
                    "thr_bias": invc.thr_bias,
                    "bh_bump": bh_thr_bump(feat.entropy),
                    "pi_best": pi_best,
                },
            )

        # STRUCT_GENERATIVE (PI lane) is permitted up to PI_MAX_RAW.
        if state is GateState.STRUCT_GENERATIVE and raw_size > PI_MAX_RAW:
            return Plan(
                state=state,
                action="CDC",
                eff_chunk=eff_chunk,
                eff_level=eff_level,
                thr_used=thr_eff,
                flags=(SPANF_HIGHENT if state in (GateState.RAW_ENTROPIC, GateState.CONTAINER_ENTROPIC) else 0),
                dbg={
                    "force_stream": True,
                    "reason": "LARGE_FILE_STREAMING_PI_CAP",
                    "cap": PI_MAX_RAW,
                    "work": work,
                    "thr_bias": invc.thr_bias,
                    "bh_bump": bh_thr_bump(feat.entropy),
                    "pi_best": pi_best,
                },
            )

        # STRUCT_MOTIF (DNA lane) is permitted up to DNA_MAX_RAW.
        if state is GateState.STRUCT_MOTIF and raw_size > DNA_MAX_RAW:
            return Plan(
                state=state,
                action="CDC",
                eff_chunk=eff_chunk,
                eff_level=eff_level,
                thr_used=thr_eff,
                flags=(SPANF_HIGHENT if state in (GateState.RAW_ENTROPIC, GateState.CONTAINER_ENTROPIC) else 0),
                dbg={
                    "force_stream": True,
                    "reason": "LARGE_FILE_STREAMING_DNA_CAP",
                    "cap": DNA_MAX_RAW,
                    "work": work,
                    "thr_bias": invc.thr_bias,
                    "bh_bump": bh_thr_bump(feat.entropy),
                    "pi_best": pi_best,
                },
            )


    # MICRO lane (only if WHOLE and small)
    def micro_ok() -> bool:
        return micro_enabled and (not dedupe) and (raw_size <= MICRO_CUTOFF) and (work > 0.0)

    # DNA lane candidate
    if state == GateState.STRUCT_MOTIF and work > 0.2:
        # Use DNA if probe gain suggests compressible AND motif_score is meaningful
        # Also avoid if near-horizon bump is huge (wasteful)
        if feat.probe_gain >= max(0.03, thr_eff * 0.7):
            return Plan(state=state, action="DNA", eff_chunk=eff_chunk, eff_level=eff_level, thr_used=thr_eff,
                        flags=0, dbg={"motif_score": feat.motif_score, "work": work, "thr_bias": invc.thr_bias})

    # PI lane candidate
    if state == GateState.STRUCT_GENERATIVE and pi_best[1] >= 0 and (work > 0.2 or pi_best[0] >= PI_BH_BYPASS_SIM):
        # Even if probe_gain is low, delta can win; accept if similarity high
        if pi_best[0] >= PI_SIM_THR:
            return Plan(state=state, action="PI", eff_chunk=eff_chunk, eff_level=eff_level, thr_used=thr_eff,
                        flags=0, dbg={"pi_sim": pi_best[0], "base_index": pi_best[1], "work": work, "pi_bypass": pi_bypass})

    # Entropic states: WHOLE store-ish
    if state in (GateState.RAW_ENTROPIC, GateState.CONTAINER_ENTROPIC):
        # prefer WHOLE, and likely store (min_gain will switch)
        act = "MICRO" if micro_ok() else "WHOLE"
        return Plan(state=state, action=act, eff_chunk=eff_chunk, eff_level=3, thr_used=thr_eff,
                    flags=SPANF_HIGHENT, dbg={"work": work, "thr_bias": invc.thr_bias})

    # Container compressible: be conservative: require larger gain
    if state == GateState.CONTAINER_COMPRESSIBLE:
        thr_c = max(thr_eff, 0.20)
        act = "CDC" if feat.probe_gain >= thr_c else ("MICRO" if micro_ok() else "WHOLE")
        return Plan(state=state, action=act, eff_chunk=eff_chunk, eff_level=eff_level, thr_used=thr_c,
                    flags=0, dbg={"work": work, "thr_bias": invc.thr_bias})

    # Generic compressible:
    # net-gain veto for CDC (rough)
    m = (raw_size + eff_chunk - 1) // eff_chunk
    net = feat.probe_gain * min(raw_size, DEF_PROBE_BYTES) - m * OVERHEAD_PER_CHUNK
    if feat.probe_gain >= thr_eff and net > 0 and work > 0.0:
        return Plan(state=state, action="CDC", eff_chunk=eff_chunk, eff_level=eff_level, thr_used=thr_eff,
                    flags=0, dbg={"net_est": net, "work": work, "thr_bias": invc.thr_bias})
    else:
        act = "MICRO" if micro_ok() else "WHOLE"
        return Plan(state=state, action=act, eff_chunk=eff_chunk, eff_level=eff_level, thr_used=thr_eff,
                    flags=0, dbg={"net_est": net, "work": work, "thr_bias": invc.thr_bias})

# -----------------------------
# Archive data structures
# -----------------------------
@dataclass
class BlockMeta:
    block_id: int
    tag: bytes
    codec: int
    flags: int
    raw_len: int
    comp_len: int
    raw_crc32: int
    h32: bytes
    comp_offset: int
    # BLK2:
    micro_entries: Optional[List[Tuple[int,int,int,int]]] = None
    # DNA1:
    dna_motif_len: int = 0
    dna_motifs: Optional[List[bytes]] = None
    # PI01:
    pi_base_index: int = -1

@dataclass
class FileMeta:
    path: str
    mtime_ns: int
    raw_size: int
    file_crc32: int
    class_id: int
    file_flags: int
    spans: List[Tuple[int,int,int]]  # (block_id, span_raw_len, span_flags)

@dataclass
class Archive:
    path: pathlib.Path
    head: dict
    blocks: Dict[int, BlockMeta]
    files: List[FileMeta]

# -----------------------------
# I/O sections
# -----------------------------
def write_section(f: io.BufferedWriter, tag: bytes, payload: bytes) -> None:
    if len(tag) != 4:
        raise ValueError("tag must be 4 bytes")
    ln = len(payload)
    if ln > MAX_SECTION:
        raise ValueError(f"section too large: {ln}")
    f.write(SEC_HDR.pack(tag, ln))
    f.write(payload)

def read_section_header(f: io.BufferedReader) -> Tuple[bytes, int]:
    raw = f.read(SEC_HDR.size)
    if not raw:
        raise EOFError
    if len(raw) != SEC_HDR.size:
        raise EOFError("truncated section header")
    tag, ln = SEC_HDR.unpack(raw)
    ln = int(ln)
    if ln < 0 or ln > MAX_SECTION:
        raise ValueError("corrupt section length")
    return tag, ln

def build_fidx(files: List[FileMeta]) -> bytes:
    parts: List[bytes] = [FILEIDX_HEAD.pack(len(files))]
    for fm in files:
        p = fm.path.encode("utf-8", errors="replace")
        parts.append(FILE_REC_HDR.pack(
            len(p),
            int(fm.mtime_ns),
            int(fm.raw_size),
            int(fm.file_crc32),
            int(len(fm.spans)),
            int(fm.class_id) & 0xFF,
            int(fm.file_flags) & 0xFF,
            0
        ))
        parts.append(p)
        for bid, span_raw_len, span_flags in fm.spans:
            parts.append(SPAN_REC.pack(int(bid), int(span_raw_len), int(span_flags)))
    return b"".join(parts)

def parse_fidx(payload: bytes) -> List[FileMeta]:
    b = memoryview(payload)
    off = 0
    if len(b) < FILEIDX_HEAD.size:
        raise ValueError("corrupt FIDX")
    (fcnt,) = FILEIDX_HEAD.unpack_from(b, off)
    off += FILEIDX_HEAD.size
    files: List[FileMeta] = []
    for _ in range(int(fcnt)):
        if off + FILE_REC_HDR.size > len(b):
            raise ValueError("corrupt FIDX file hdr")
        path_len, mtime_ns, raw_size, file_crc32, span_count, class_id, file_flags, _rsv = FILE_REC_HDR.unpack_from(b, off)
        off += FILE_REC_HDR.size
        if off + int(path_len) > len(b):
            raise ValueError("corrupt FIDX path")
        path = bytes(b[off:off+int(path_len)]).decode("utf-8", errors="replace")
        off += int(path_len)
        spans: List[Tuple[int,int,int]] = []
        for _si in range(int(span_count)):
            if off + SPAN_REC.size > len(b):
                raise ValueError("corrupt FIDX span")
            bid, sln, sfl = SPAN_REC.unpack_from(b, off)
            off += SPAN_REC.size
            spans.append((int(bid), int(sln), int(sfl)))
        files.append(FileMeta(
            path=path,
            mtime_ns=int(mtime_ns),
            raw_size=int(raw_size),
            file_crc32=int(file_crc32),
            class_id=int(class_id),
            file_flags=int(file_flags),
            spans=spans
        ))
    return files

def parse_archive(path: pathlib.Path) -> Archive:
    blocks: Dict[int, BlockMeta] = {}
    files: List[FileMeta] = []
    head: dict = {}

    with path.open("rb") as f:
        hdr = f.read(FILE_HDR.size)
        if len(hdr) != FILE_HDR.size:
            raise ValueError("truncated header")
        magic, ver = FILE_HDR.unpack(hdr)
        if magic != MAGIC:
            raise ValueError(f"bad magic {magic!r}")
        if ver != FORMAT_VERSION:
            raise ValueError(f"unsupported version {ver}")

        while True:
            try:
                tag, ln = read_section_header(f)
            except EOFError:
                break
            payload_off = f.tell()
            payload = f.read(ln)
            if len(payload) != ln:
                raise EOFError("truncated payload")

            if tag == TAG_HEAD:
                head = json.loads(payload.decode("utf-8", errors="replace"))
            elif tag == TAG_CHNK:
                if len(payload) < CHNK_HDR.size:
                    raise ValueError("corrupt CHNK")
                bid, codec, flags, _rsv, raw_len, comp_len, raw_crc, h32 = CHNK_HDR.unpack_from(payload, 0)
                comp = payload[CHNK_HDR.size:]
                if len(comp) != comp_len:
                    raise ValueError("corrupt CHNK comp_len")
                blocks[int(bid)] = BlockMeta(
                    block_id=int(bid),
                    tag=TAG_CHNK,
                    codec=int(codec),
                    flags=int(flags),
                    raw_len=int(raw_len),
                    comp_len=int(comp_len),
                    raw_crc32=int(raw_crc),
                    h32=bytes(h32),
                    comp_offset=payload_off + CHNK_HDR.size,
                )
            elif tag == TAG_BLK2:
                if len(payload) < BLK2_HDR.size:
                    raise ValueError("corrupt BLK2")
                bid, codec, flags, _rsv, raw_total, comp_len, raw_crc, entry_count = BLK2_HDR.unpack_from(payload, 0)
                off = BLK2_HDR.size
                entries: List[Tuple[int,int,int,int]] = []
                for _ in range(int(entry_count)):
                    if off + BLK2_ENTRY.size > len(payload):
                        raise ValueError("corrupt BLK2 entries")
                    fi, o, ln2, c32 = BLK2_ENTRY.unpack_from(payload, off)
                    off += BLK2_ENTRY.size
                    entries.append((int(fi), int(o), int(ln2), int(c32)))
                comp = payload[off:]
                if len(comp) != comp_len:
                    raise ValueError("corrupt BLK2 comp_len")
                blocks[int(bid)] = BlockMeta(
                    block_id=int(bid),
                    tag=TAG_BLK2,
                    codec=int(codec),
                    flags=int(flags),
                    raw_len=int(raw_total),
                    comp_len=int(comp_len),
                    raw_crc32=int(raw_crc),
                    h32=b"\x00"*32,
                    comp_offset=payload_off + off,
                    micro_entries=entries,
                )
            elif tag == TAG_DNA1:
                if len(payload) < DNA1_HDR.size:
                    raise ValueError("corrupt DNA1")
                bid, codec, flags, motif_len, motif_count, _rsv, raw_len, tok_comp_len, raw_crc = DNA1_HDR.unpack_from(payload, 0)
                off = DNA1_HDR.size
                motifs: List[bytes] = []
                ml = int(motif_len)
                mc = int(motif_count)
                if ml <= 0 or mc < 0 or mc > 20000:
                    raise ValueError("corrupt DNA1 motif header")
                if off + (ml * mc) > len(payload):
                    raise ValueError("corrupt DNA1 motifs")
                for i in range(mc):
                    motifs.append(payload[off + i*ml : off + (i+1)*ml])
                off += ml * mc
                comp = payload[off:]
                if len(comp) != tok_comp_len:
                    raise ValueError("corrupt DNA1 tok len")
                blocks[int(bid)] = BlockMeta(
                    block_id=int(bid),
                    tag=TAG_DNA1,
                    codec=int(codec),
                    flags=int(flags),
                    raw_len=int(raw_len),
                    comp_len=int(tok_comp_len),
                    raw_crc32=int(raw_crc),
                    h32=b"\x00"*32,
                    comp_offset=payload_off + off,
                    dna_motif_len=ml,
                    dna_motifs=motifs,
                )
            elif tag == TAG_PI01:
                if len(payload) < PI01_HDR.size:
                    raise ValueError("corrupt PI01")
                bid, codec, flags, _rsv, base_index, raw_len, comp_len, raw_crc = PI01_HDR.unpack_from(payload, 0)
                comp = payload[PI01_HDR.size:]
                if len(comp) != comp_len:
                    raise ValueError("corrupt PI01 comp_len")
                blocks[int(bid)] = BlockMeta(
                    block_id=int(bid),
                    tag=TAG_PI01,
                    codec=int(codec),
                    flags=int(flags),
                    raw_len=int(raw_len),
                    comp_len=int(comp_len),
                    raw_crc32=int(raw_crc),
                    h32=b"\x00"*32,
                    comp_offset=payload_off + PI01_HDR.size,
                    pi_base_index=int(base_index),
                )
            elif tag == TAG_FIDX:
                files = parse_fidx(payload)
            elif tag == TAG_END:
                break
            else:
                # ignore unknown future tags
                pass

    return Archive(path=path, head=head, blocks=blocks, files=files)

# -----------------------------
# Input gathering
# -----------------------------
def gather_input_paths(inputs: List[str]) -> Tuple[pathlib.Path, List[pathlib.Path]]:
    ps = [pathlib.Path(x) for x in inputs]
    root = pathlib.Path(os.path.commonpath([str(p.resolve()) for p in ps])).resolve()
    out: List[pathlib.Path] = []
    for p in ps:
        p = p.resolve()
        if p.is_dir():
            for dp, _, fnames in os.walk(p):
                for n in fnames:
                    out.append(pathlib.Path(dp, n))
        else:
            out.append(p)
    out = [p for p in out if p.is_file()]
    out.sort()
    return root, out

# -----------------------------
# Build archive (v13.1)
# -----------------------------
def build_archive(
    out_path: pathlib.Path,
    inputs: List[str],
    *,
    chunk_size: int,
    preset: str,
    mp_gate: bool,
    mp_cdc_big: bool,
    mp_cdc_cutoff: int,
    profile: bool,
    prefer_codec: str,
    zstd_level: int,
    jobs: int,
    dedupe: bool,
    gate_enabled: bool,
    probe_bytes: int,
    probe_level: int,
    auto_sample_files: int,
    auto_sample_bytes: int,
    auto_entropy_hi: float,
    auto_gain_lo: float,
    auto_fastgame_score_thr: float,
    auto_small_bytes_thr: int,
    auto_small_files_thr: int,
    thr: float,
    thr_small: float,
    small_cutoff: int,
    whole_max: int,
    min_gain: float,
    micro_enabled: bool,
    dna_enabled: bool,
    pi_enabled: bool,
    verbose_gate: bool,
    deterministic: bool,
    reproducible: bool,
) -> None:
    if prefer_codec not in ("auto","zstd","zlib","store"):
        raise ValueError("codec must be auto|zstd|zlib|store")
    # Profiling (does not affect archive bytes)
    pt = PhaseTimer() if profile else None


    # Reproducible mode: canonicalize timestamps/metadata to make archives bit-identical
    # across rebuilds/machines (content-only determinism).
    if reproducible:
        deterministic = True
    canon_epoch_ns = source_date_epoch_ns() if reproducible else None
    canon_created_ns = (canon_epoch_ns if canon_epoch_ns is not None else 0) if reproducible else None
    canon_mtime_ns = (canon_epoch_ns if canon_epoch_ns is not None else 0) if reproducible else None

    if prefer_codec == "zstd" and not HAVE_ZSTD:
        raise RuntimeError("zstandard not installed")
    if prefer_codec == "store":
        prefer_codec_id = CODEC_STORE
    elif prefer_codec == "zstd":
        prefer_codec_id = CODEC_ZSTD
    elif prefer_codec == "zlib":
        prefer_codec_id = CODEC_ZLIB
    else:
        prefer_codec_id = CODEC_ZSTD if HAVE_ZSTD else CODEC_ZLIB

    # Parallelism: deterministic parallel compression (writer order remains fixed).
    if jobs <= 0:
        cpu = os.cpu_count() or 1
        # leave one core for OS/UI; cap to avoid huge worker counts
        eff_jobs = max(1, min(32, cpu - 1))
    else:
        eff_jobs = max(1, int(jobs))
    pool = None
    if eff_jobs > 1:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=eff_jobs)

    root, paths = gather_input_paths(inputs)

    preset_requested = str(preset)
    preset_effective = str(preset)
    auto_stats: Dict[str, float] = {}
    if preset_requested == "auto":
        preset_effective, auto_stats = auto_select_preset(
            paths,
            sample_files=auto_sample_files,
            sample_bytes=auto_sample_bytes,
            entropy_hi=auto_entropy_hi,
            gain_lo=auto_gain_lo,
            fastgame_score_thr=auto_fastgame_score_thr,
            small_bytes_thr=auto_small_bytes_thr,
            small_files_thr=auto_small_files_thr,
        )
        preset = preset_effective

    if pt: pt.mark('walk')

    # Optional Stage-1 gating hints (cheap). Only affects behavior in preset 'fast-game'.
    gate_hints: List[GateHint] = []
    if paths:
        if mp_gate and preset == "fast-game":
            import multiprocessing as _mp  # local import
            cpu = os.cpu_count() or 1
            proc = max(1, min(cpu, 32))
            ctx = _mp.get_context("spawn")
            with ctx.Pool(processes=proc) as mp_pool:
                items = [(i, str(p), preset, probe_bytes, probe_level, chunk_size, whole_max) for i, p in enumerate(paths)]
                tmp = [None] * len(paths)
                for i, hint in mp_pool.imap_unordered(_mp_gate_worker, items, chunksize=32):
                    tmp[i] = hint
                gate_hints = [h if h is not None else stage1_gate_hint(p, preset=preset,
                               base_probe_bytes=probe_bytes, base_probe_level=probe_level,
                               base_chunk_size=chunk_size, whole_max=whole_max) for p,h in zip(paths,tmp)]
        else:
            gate_hints = [stage1_gate_hint(p, preset=preset,
                               base_probe_bytes=probe_bytes, base_probe_level=probe_level,
                               base_chunk_size=chunk_size, whole_max=whole_max) for p in paths]

    if pt: pt.mark('gate')
    inv = MultiInvariantState()

    head = {
        "format": {"magic": MAGIC.decode("ascii"), "version": FORMAT_VERSION, "version_str": VERSION_STR},
        "created_utc_ns": (canon_created_ns if canon_created_ns is not None else (0 if deterministic else now_utc_ns())),
        "reproducible": bool(reproducible),
        "source_date_epoch": (int((canon_epoch_ns or 0)//1_000_000_000) if reproducible else None),
        "chunk_size": int(chunk_size),
        "preset_requested": str(preset_requested),
        "preset_effective": str(preset_effective),
        "preset": str(preset_effective),
        "auto_preset": (auto_stats if preset_requested == "auto" else None),
        "mp_gate": bool(mp_gate),
        "profile": bool(profile),
        "codec_pref": prefer_codec,
        "zstd_level": int(zstd_level),
        "hash": "blake3-256" if HAVE_BLAKE3 else "sha256",
        "dedupe": bool(dedupe),
        "gate": {
            "enabled": bool(gate_enabled),
            "probe_bytes": int(probe_bytes),
            "probe_level": int(probe_level),
            "thr": float(thr),
            "thr_small": float(thr_small),
            "small_cutoff": int(small_cutoff),
            "whole_max": int(whole_max),
            "min_gain": float(min_gain),
        },
        "lanes": {
            "micro": bool(micro_enabled),
            "dna": bool(dna_enabled),
            "pi": bool(pi_enabled),
        },
        "policies": {
            "black_hole": {
                "enabled": bool(BH_ENABLE),
                "ent0": BH_ENT0,
                "ent1": BH_ENT1,
                "gamma": BH_GAMMA,
                "thr_bump_max": BH_THR_BUMP_MAX,
            },
            "invariant": {
                "enabled": bool(INV_ENABLE),
                "ema_alpha": INV_EMA_ALPHA,
                "thr_step_max": INV_THR_STEP_MAX,
                "target_gain": INV_TARGET_GAIN,
            },
        },
        "stats": {
            "inv_ema_gain_final": 0.0,
            "inv_thr_bias_final": 0.0,
            "dna_files": 0,
            "pi_files": 0,
            "micro_files": 0,
        }
    }

    ensure_parent(out_path)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink(missing_ok=True)

    # block ids
    next_block_id = 1
    # micro blocks use same id space
    # dedupe map (raw_hash -> block_id) for CDC chunks
    hash_to_block: Dict[bytes, int] = {}

    # MICRO accumulator
    micro_blob = bytearray()
    micro_entries: List[Tuple[int,int,int,int]] = []  # (file_index, off, len, crc32)
    micro_block_id = -1  # allocated when we start a micro block

    files_meta: List[FileMeta] = []
    raw_sum = 0

    # PI base signatures cache (legacy; kept for compat/debug)
    recent_pi_bases: List[Tuple[int, str, List[int]]] = []
    # PI fingerprint index (v14 phase2)
    pi_index = PIIndex()

    # store raw bytes for PI bases (bounded) by file_index
    pi_base_bytes: Dict[int, bytes] = {}

    pi_base_order: collections.deque[int] = collections.deque()

    # codec counts
    codec_counts = {CODEC_STORE: 0, CODEC_ZSTD: 0, CODEC_ZLIB: 0}
    blocks_written = 0

    def flush_micro(fout: io.BufferedWriter) -> int:
        nonlocal micro_blob, micro_entries, micro_block_id, next_block_id, blocks_written
        if not micro_entries:
            micro_block_id = -1
            return -1

        raw = bytes(micro_blob)
        raw_crc = crc32_bytes(raw)

        # compress micro payload
        level = 3
        codec, comp = compress_bytes(raw, prefer_codec_id, level)
        if codec != CODEC_STORE:
            gain = 1.0 - (len(comp) / float(len(raw) if raw else 1))
            if gain < min_gain:
                codec, comp = CODEC_STORE, raw

        bid = micro_block_id
        hdr = BLK2_HDR.pack(
            int(bid),
            int(codec) & 0xFF,
            0,
            0,
            len(raw),
            len(comp),
            raw_crc,
            len(micro_entries),
        )
        ent = b"".join(BLK2_ENTRY.pack(fi, off, ln, c32) for (fi, off, ln, c32) in micro_entries)
        payload = hdr + ent + comp
        write_section(fout, TAG_BLK2, payload)

        blocks_written += 1
        codec_counts[codec] += 1

        micro_blob = bytearray()
        micro_entries = []
        micro_block_id = -1
        return bid

    t0 = time.time()


    with tmp.open("wb") as fout:
        fout.write(FILE_HDR.pack(MAGIC, FORMAT_VERSION))
        write_section(fout, TAG_HEAD, json.dumps(head, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))

        for file_index, p in enumerate(paths):
            st = p.stat()
            raw_size = int(st.st_size)
            raw_sum += raw_size
            rp = relpath_str(root, p)


            hint = gate_hints[file_index] if (gate_hints and file_index < len(gate_hints)) else GateHint(
                skip_deep=False, disable_structure_lanes=False, force_whole=False,
                probe_bytes=probe_bytes, probe_level=probe_level, chunk_size=chunk_size
            )
            local_chunk_size = hint.chunk_size

            feat = compute_features(p, probe_bytes=hint.probe_bytes, probe_level=hint.probe_level)
            if pt: pt.mark('feat')

            # fast-game hint: disable expensive structure lanes on likely high-entropy blobs
            if preset == "fast-game" and hint.disable_structure_lanes:
                feat.motif_score = 0.0
                feat.pi_sig = []
                feat.pi_hash64 = 0

            # fast-game hint: force WHOLE preference
            if preset == "fast-game" and hint.force_whole:
                feat.entropy = max(feat.entropy, 7.95)
                feat.probe_gain = min(feat.probe_gain, 0.0)

            # if gate disabled, flatten to generic compressible features
            if not gate_enabled:
                feat.entropy = 0.5
                feat.motif_score = 0.0
                feat.pi_sig = []
                feat.is_container = False

            # choose plan
            plan = choose_plan(
                p, feat, inv,
                chunk_size=local_chunk_size,
                thr=thr, thr_small=thr_small, small_cutoff=small_cutoff,
                whole_max=whole_max,
                min_gain=min_gain,
                micro_enabled=micro_enabled,
                dedupe=dedupe,
                recent_pi_bases=recent_pi_bases,
                pi_index=pi_index,
            )
            if pt: pt.mark('plan')

            # show decision line
            print(f"  - {rp} :: size={raw_size} probe_gain={feat.probe_gain:.4f} thr={plan.thr_used:.3f} -> {plan.action}")
            if verbose_gate:
                dbg = dict(plan.dbg)
                dbg.update({
                    "class": feat.class_tag,
                    "state": plan.state.name,
                    "ent": round(feat.entropy, 3),
                    "uniq": round(feat.uniq, 3),
                    "motif": round(feat.motif_score, 3),
                    "pi_sim": round(feat.pi_sim_hint, 3),
                    "inv_thr_bias": round(inv.get(feat.class_tag).thr_bias, 4),
                    "inv_ema_gain": round(inv.get(feat.class_tag).ema_gain, 4),
                    "inv_thr_bias_all": round(inv.states.get('all', InvariantState()).thr_bias, 4),
                    "inv_ema_gain_all": round(inv.states.get('all', InvariantState()).ema_gain, 4),
                })
                print("      gate_dbg: " + " ".join(f"{k}={v}" for k, v in dbg.items()))

            file_flags = 0
            spans: List[Tuple[int,int,int]] = []

            # Helper: realized gain estimate for invariant update
            # Use probe_gain as a proxy; update after we actually encode (cheap, deterministic).
            observed_gain = max(0.0, feat.probe_gain)

            # MICRO lane: pack bytes directly (WHOLE+small) into current micro blob
            if plan.action == "MICRO":
                if not micro_enabled:
                    plan.action = "WHOLE"
                else:
                    head["stats"]["micro_files"] += 1
                    file_flags |= 0x01  # whole
                    # allocate micro block id if needed
                    if micro_block_id < 0:
                        micro_block_id = next_block_id
                        next_block_id += 1
                    # flush if would exceed
                    if (len(micro_blob) + raw_size > MICRO_BLOCK_MAX) or (len(micro_entries) >= MICRO_ENTRY_MAX):
                        flush_micro(fout)
                        micro_block_id = next_block_id
                        next_block_id += 1

                    data = p.read_bytes()
                    c32 = crc32_bytes(data)
                    off0 = len(micro_blob)
                    micro_blob.extend(data)
                    entry_index = len(micro_entries)
                    micro_entries.append((file_index, off0, raw_size, c32))

                    span_flags = spanflags_with_aux(SPANF_MICRO | SPANF_LAST, entry_index)
                    spans.append((micro_block_id, raw_size, span_flags))

                    files_meta.append(FileMeta(
                        path=rp,
                        mtime_ns=(canon_mtime_ns if canon_mtime_ns is not None else (canon_mtime_ns if canon_mtime_ns is not None else int(st.st_mtime_ns))),
                        raw_size=raw_size,
                        file_crc32=file_crc,
                        class_id=4,
                        file_flags=file_flags,
                        spans=spans
                    ))
                    inv.update(feat.class_tag, observed_gain)
                    # update PI bases signatures
                    if PI_ENABLE and pi_enabled:
                        recent_pi_bases.append((file_index, feat.ext, feat.pi_sig))
                        pi_index.add(file_index, feat.ext, feat.pi_sig, feat.pi_hash64)
                        # store base bytes for delta if it's a plausible base (bounded)
                        if raw_size <= PI_MAX_RAW and raw_size >= PI_MIN_RAW:
                            pi_base_bytes[file_index] = data
                    continue

            # if not micro, flush any pending micro block to keep ordering simple
            flush_micro(fout)

            # DNA lane
            if plan.action == "DNA" and dna_enabled and DNA_ENABLE:
                data = p.read_bytes()
                raw_crc = crc32_bytes(data)
                motifs = dna_build_motifs_from_sample(data[:min(len(data), probe_bytes)], DNA_MOTIF_LEN, DNA_MAX_MOTIFS)
                if len(motifs) < DNA_MIN_MOTIFS:
                    # fallback
                    plan.action = "WHOLE"
                else:
                    tokens = dna_encode_bytes(data, motifs, DNA_MOTIF_LEN)
                    # compress tokens
                    codec, comp = compress_bytes(tokens, prefer_codec_id, plan.eff_level)
                    if codec != CODEC_STORE:
                        gain = 1.0 - (len(comp) / float(len(tokens) if tokens else 1))
                        if gain < min_gain:
                            codec, comp = CODEC_STORE, tokens

                    bid = next_block_id
                    next_block_id += 1
                    motifs_blob = b"".join(motifs)
                    hdr = DNA1_HDR.pack(
                        int(bid),
                        int(codec) & 0xFF,
                        0,
                        DNA_MOTIF_LEN,
                        len(motifs),
                        0,
                        len(data),
                        len(comp),
                        raw_crc,
                    )
                    payload = hdr + motifs_blob + comp
                    write_section(fout, TAG_DNA1, payload)
                    blocks_written += 1
                    codec_counts[codec] += 1

                    # verify decode quickly (safety)
                    tok_raw = decompress_bytes(codec, comp, len(tokens) if codec != CODEC_STORE else len(comp))
                    dec = dna_decode_tokens(tok_raw, motifs, DNA_MOTIF_LEN, len(data))
                    if crc32_bytes(dec) != raw_crc:
                        raise RuntimeError("DNA lane verification failed")

                    spans.append((bid, len(data), SPANF_DNA | SPANF_LAST | (SPANF_HIGHENT if (plan.flags & SPANF_HIGHENT) else 0)))
                    files_meta.append(FileMeta(
                        path=rp,
                        mtime_ns=(canon_mtime_ns if canon_mtime_ns is not None else (canon_mtime_ns if canon_mtime_ns is not None else int(st.st_mtime_ns))),
                        raw_size=len(data),
                        file_crc32=raw_crc,
                        class_id=2,
                        file_flags=0x02,  # dna marker
                        spans=spans
                    ))
                    head["stats"]["dna_files"] += 1

                    # invariants: observed gain = (raw - payload)/raw
                    encoded_bytes = SEC_HDR.size + len(payload)  # rough accounting
                    observed_gain = max(0.0, 1.0 - (encoded_bytes / float(len(data) if data else 1)))
                    inv.update(feat.class_tag, observed_gain)

                    # PI bases signatures
                    if PI_ENABLE and pi_enabled:
                        recent_pi_bases.append((file_index, feat.ext, feat.pi_sig))
                        if len(data) <= PI_MAX_RAW and len(data) >= PI_MIN_RAW:
                            pi_base_bytes[file_index] = data
                    continue

            # PI lane
            if plan.action == "PI" and pi_enabled and PI_ENABLE:
                base_index = int(plan.dbg.get("base_index", -1))
                if base_index < 0 or base_index not in pi_base_bytes:
                    # fallback if base not available
                    plan.action = "WHOLE"
                else:
                    raw = p.read_bytes()
                    raw_crc = crc32_bytes(raw)
                    base = pi_base_bytes[base_index]
                    ops = pi_delta_encode(raw, base)

                    # compress ops
                    codec, comp = compress_bytes(ops, prefer_codec_id, plan.eff_level)
                    if codec != CODEC_STORE:
                        gain = 1.0 - (len(comp) / float(len(ops) if ops else 1))
                        if gain < min_gain:
                            codec, comp = CODEC_STORE, ops

                    # verify decode (mandatory)
                    ops_raw = decompress_bytes(codec, comp, len(ops) if codec != CODEC_STORE else len(comp))
                    dec = pi_delta_decode(ops_raw, base, len(raw))
                    if crc32_bytes(dec) != raw_crc:
                        # fallback to whole if delta fails (shouldn't)
                        plan.action = "WHOLE"
                    else:
                        bid = next_block_id
                        next_block_id += 1
                        hdr = PI01_HDR.pack(
                            int(bid),
                            int(codec) & 0xFF,
                            0,
                            0,
                            int(base_index),
                            len(raw),
                            len(comp),
                            raw_crc,
                        )
                        payload = hdr + comp
                        write_section(fout, TAG_PI01, payload)
                        blocks_written += 1
                        codec_counts[codec] += 1

                        spans.append((bid, len(raw), SPANF_PI | SPANF_LAST))
                        files_meta.append(FileMeta(
                            path=rp,
                            mtime_ns=(canon_mtime_ns if canon_mtime_ns is not None else (canon_mtime_ns if canon_mtime_ns is not None else int(st.st_mtime_ns))),
                            raw_size=len(raw),
                            file_crc32=raw_crc,
                            class_id=3,
                            file_flags=0x03,  # pi marker
                            spans=spans
                        ))
                        head["stats"]["pi_files"] += 1

                        encoded_bytes = SEC_HDR.size + len(payload)
                        observed_gain = max(0.0, 1.0 - (encoded_bytes / float(len(raw) if raw else 1)))
                        inv.update(feat.class_tag, observed_gain)

                        # update bases
                        recent_pi_bases.append((file_index, feat.ext, feat.pi_sig))
                        if len(raw) <= PI_MAX_RAW and len(raw) >= PI_MIN_RAW:
                            pi_base_bytes[file_index] = raw
                        continue

            # CDC lane
            if plan.action == "CDC":
                file_crc = 0
                pending: List[Tuple[int, bytes, int, int, int]] = []  # (bid, chunk, raw_crc, h32, flags_base)
                for chunk in iter_file_chunks(p, plan.eff_chunk):
                    raw_crc = crc32_bytes(chunk)
                    file_crc = crc32_update(file_crc, chunk)
                    h32 = stable_hash32(chunk)

                    if dedupe and h32 in hash_to_block:
                        bid = hash_to_block[h32]
                        spans.append((bid, len(chunk), 0))
                        continue

                    flags_base = 0
                    if plan.flags & SPANF_HIGHENT:
                        flags_base |= SPANF_HIGHENT

                    bid = next_block_id
                    next_block_id += 1
                    if dedupe:
                        hash_to_block[h32] = bid

                    pending.append((bid, chunk, raw_crc, h32, flags_base))
                    spans.append((bid, len(chunk), 0))

                # compress+write in deterministic order; optionally parallelize compression
                # compress+write in deterministic order; optionally parallelize compression.
                # Use a bounded batch pipeline to avoid huge task lists and (when parallel) keep memory stable.
                if pending:
                    BATCH = 256
                    for off in range(0, len(pending), BATCH):
                        batch = pending[off:off + BATCH]
                        tasks = ((ch, prefer_codec_id, plan.eff_level, min_gain) for (_, ch, _, _, _) in batch)
                        if pool is not None and eff_jobs > 1:
                            results_iter = pool.map(_compress_task, tasks)
                        else:
                            results_iter = map(_compress_task, tasks)

                        for (bid, chunk, raw_crc, h32, flags_base), (codec, comp) in zip(batch, results_iter):
                            flags = int(flags_base)
                            if codec == CODEC_STORE:
                                flags |= SPANF_STORE

                            payload = CHNK_HDR.pack(
                                int(bid),
                                int(codec) & 0xFF,
                                int(flags) & 0xFF,
                                0,
                                len(chunk),
                                len(comp),
                                raw_crc,
                                h32,
                            ) + comp
                            write_section(fout, TAG_CHNK, payload)
                            blocks_written += 1
                            codec_counts[codec] += 1
                    pending.clear()
                inv.update(feat.class_tag, observed_gain)
                if PI_ENABLE and pi_enabled:
                    # base signatures (store only if manageable)
                    data = None
                    if raw_size <= PI_MAX_RAW and raw_size >= PI_MIN_RAW:
                        data = p.read_bytes()
                        pi_base_bytes[file_index] = data
                    recent_pi_bases.append((file_index, feat.ext, feat.pi_sig))
                # NOTE: CDC lane must still emit FileMeta; otherwise the archive will
                # "lose" CDC files (listing/extraction will only see WHOLE/MICRO/DNA/PI entries).
                files_meta.append(
                    FileMeta(
                        path=rp,
                        mtime_ns=(canon_mtime_ns if canon_mtime_ns is not None else int(st.st_mtime_ns)),
                        raw_size=raw_size,
                        file_crc32=file_crc,
                        class_id=0,      # CHNK-based file
                        file_flags=0x00, # CDC (chunked) content
                        spans=spans,
                    )
                )
                continue

            # WHOLE lane (single CHNK block)
            data = p.read_bytes()
            raw_crc = crc32_bytes(data)
            codec, comp = compress_bytes(data, prefer_codec_id, plan.eff_level)
            if codec != CODEC_STORE:
                gain = 1.0 - (len(comp) / float(len(data) if data else 1))
                if gain < min_gain:
                    codec, comp = CODEC_STORE, data

            flags = 0
            if codec == CODEC_STORE:
                flags |= SPANF_STORE
            if plan.flags & SPANF_HIGHENT:
                flags |= SPANF_HIGHENT

            bid = next_block_id
            next_block_id += 1
            payload = CHNK_HDR.pack(
                int(bid),
                int(codec) & 0xFF,
                int(flags) & 0xFF,
                0,
                len(data),
                len(comp),
                raw_crc,
                b"\x00"*32
            ) + comp
            write_section(fout, TAG_CHNK, payload)
            blocks_written += 1
            codec_counts[codec] += 1

            spans.append((bid, len(data), SPANF_LAST))
            files_meta.append(FileMeta(
                path=rp,
                mtime_ns=(canon_mtime_ns if canon_mtime_ns is not None else (canon_mtime_ns if canon_mtime_ns is not None else int(st.st_mtime_ns))),
                raw_size=len(data),
                file_crc32=(raw_crc & 0xffffffff),
                class_id=0,
                file_flags=0x01,
                spans=spans
            ))

            # --- PI base cache update (phase2b): make PI bases available regardless of lane ---
            if PI_ENABLE and pi_enabled and (PI_MIN_RAW <= raw_size <= PI_MAX_RAW) and (feat.class_tag in ("text", "doc")):
                # Ensure we have raw bytes for delta bases (bounded memory)
                if file_index not in pi_base_bytes:
                    try:
                        pi_base_bytes[file_index] = p.read_bytes()
                    except Exception:
                        pi_base_bytes[file_index] = b""
                    pi_base_order.append(file_index)
                    while len(pi_base_order) > PI_BASE_STORE_MAX:
                        old = pi_base_order.popleft()
                        pi_base_bytes.pop(old, None)
                # Always index signature + fingerprint for shortlist lookup
                pi_index.add(file_index, feat.ext, feat.pi_sig, feat.pi_hash64)

            encoded_bytes = SEC_HDR.size + len(payload)
            observed_gain = max(0.0, 1.0 - (encoded_bytes / float(len(data) if data else 1)))
            inv.update(feat.class_tag, observed_gain)

            if PI_ENABLE and pi_enabled:
                recent_pi_bases.append((file_index, feat.ext, feat.pi_sig))
                if len(data) <= PI_MAX_RAW and len(data) >= PI_MIN_RAW:
                    pi_base_bytes[file_index] = data

            if pt: pt.mark('write')
        # flush tail micro
        flush_micro(fout)

        # write updated head stats (rewrite HEAD is complex; instead write final HEAD2? keep simple: write a second HEAD tagged HEAD and parse last)
        head["stats"]["inv_ema_gain_final"] = float(inv.states.get("all", InvariantState()).ema_gain)
        # per-class invariant snapshot (v14+)
        head["stats"]["inv_by_class"] = {k: {"ema_gain": float(s.ema_gain), "thr_bias": float(s.thr_bias)} for (k, s) in inv.states.items()}
        head["stats"]["inv_thr_bias_final"] = float(inv.states.get("all", InvariantState()).thr_bias)

        # write final HEAD again (parser uses last HEAD seen)
        write_section(fout, TAG_HEAD, json.dumps(head).encode("utf-8"))

        write_section(fout, TAG_FIDX, build_fidx(files_meta))
        write_section(fout, TAG_END, b"")


    # Safety: refuse to overwrite/replace a directory (common CLI order mistake: `a INPUT OUT` vs `a OUT INPUT`)
    if out_path.exists() and out_path.is_dir():
        raise SystemExit(
            f"ERROR: output path {out_path!s} is a directory. "
            f"Did you mean: `a <out.czp> <inputs...>` e.g. `a out.czp C` ?"
        )

    os.replace(tmp, out_path)


    t1 = time.time()
    arc_bytes = out_path.stat().st_size
    ratio = arc_bytes / float(raw_sum if raw_sum else 1)

    print(f"[CZP v{VERSION_STR}] OK: wrote {out_path}")
    print(f"  files={len(files_meta)} raw_sum={raw_sum} archive_bytes={arc_bytes} ratio={ratio:.4f} time={t1 - t0:.2f}s")
    print(f"  blocks_written={blocks_written} dedupe={'on' if dedupe else 'off'} micro={'on' if micro_enabled else 'off'} dna={'on' if dna_enabled else 'off'} pi={'on' if pi_enabled else 'off'}")
    print("  codecs: " + ", ".join(f"{CODEC_NAME[k]}={v}" for k, v in codec_counts.items() if v))
    if pt: print("  profile: " + pt.report())

def iter_file_chunks(path: pathlib.Path, chunk_size: int) -> Iterable[bytes]:
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            yield b

# -----------------------------
# Decode / reconstruct
# -----------------------------
def read_block_comp(arc: Archive, bm: BlockMeta) -> bytes:
    with arc.path.open("rb") as f:
        f.seek(bm.comp_offset)
        return f.read(bm.comp_len)

def decode_block_raw(arc: Archive, bm: BlockMeta) -> bytes:
    comp = read_block_comp(arc, bm)
    raw = decompress_bytes(bm.codec, comp, bm.raw_len) if bm.tag in (TAG_CHNK, TAG_BLK2) else None
    if bm.tag == TAG_CHNK:
        raw = decompress_bytes(bm.codec, comp, bm.raw_len)
        if crc32_bytes(raw) != bm.raw_crc32:
            raise ValueError(f"block CRC mismatch {bm.block_id}")
        return raw
    if bm.tag == TAG_BLK2:
        raw = decompress_bytes(bm.codec, comp, bm.raw_len)
        if crc32_bytes(raw) != bm.raw_crc32:
            raise ValueError(f"micro block CRC mismatch {bm.block_id}")
        return raw
    if bm.tag == TAG_DNA1:
        # tokens are compressed; motifs are stored in meta
        motifs = bm.dna_motifs or []
        tok_comp = comp
        tokens = decompress_bytes(bm.codec, tok_comp, 64 * 1024 * 1024)  # cap
        raw2 = dna_decode_tokens(tokens, motifs, bm.dna_motif_len, bm.raw_len)
        if crc32_bytes(raw2) != bm.raw_crc32:
            raise ValueError(f"DNA CRC mismatch {bm.block_id}")
        return raw2
    if bm.tag == TAG_PI01:
        ops_comp = comp
        ops = decompress_bytes(bm.codec, ops_comp, 256 * 1024 * 1024)
        base_i = bm.pi_base_index
        if base_i < 0 or base_i >= len(arc.files):
            raise ValueError("PI base index out of range")
        base_fm = arc.files[base_i]
        base_bytes = reconstruct_file_bytes(arc, base_fm)
        raw2 = pi_delta_decode(ops, base_bytes, bm.raw_len)
        if crc32_bytes(raw2) != bm.raw_crc32:
            raise ValueError(f"PI CRC mismatch {bm.block_id}")
        return raw2
    raise ValueError(f"unknown block tag {bm.tag!r}")

def reconstruct_file_bytes(arc: Archive, fm: FileMeta) -> bytes:
    out = bytearray()
    for bid, span_raw_len, span_flags in fm.spans:
        bm = arc.blocks.get(bid)
        if bm is None:
            raise ValueError(f"missing block id {bid} for {fm.path}")
        base_flags = spanflags_base(span_flags)

        if (base_flags & SPANF_MICRO) != 0:
            raw_blk = decode_block_raw(arc, bm)
            entries = bm.micro_entries or []
            idx = spanflags_aux(span_flags)
            if idx >= len(entries):
                raise ValueError("micro entry index out of range")
            _fi, off0, ln0, c32 = entries[idx]
            chunk = raw_blk[off0:off0+ln0]
            if ln0 != span_raw_len:
                raise ValueError("micro span len mismatch")
            if crc32_bytes(chunk) != c32:
                raise ValueError("micro entry crc mismatch")
            out.extend(chunk)
        else:
            raw = decode_block_raw(arc, bm)
            if len(raw) != span_raw_len:
                # only CDC chunks might be smaller at tail; safe trim
                raw = raw[:span_raw_len]
            out.extend(raw)

    if len(out) != fm.raw_size:
        raise ValueError(f"file size mismatch {fm.path}: {len(out)} != {fm.raw_size}")
    return bytes(out)

def stream_test_archive(arc: Archive) -> None:
    print(f"Testing {arc.path}: {len(arc.files)} file(s)")
    for fm in arc.files:
        data = reconstruct_file_bytes(arc, fm)
        c32 = crc32_bytes(data)
        if c32 != fm.file_crc32:
            raise ValueError(f"file CRC mismatch {fm.path}: {c32:08x} != {fm.file_crc32:08x}")
    print("OK: all files verified (block CRC + file CRC)")


# -----------------------------
# Commands
# -----------------------------
def cmd_add(args: argparse.Namespace) -> None:



    # Plan-only mode: deterministic input scan + preset(auto) selection, no archive write.
    if getattr(args, "plan_only", False):
        t0 = time.perf_counter()
        _, paths = gather_input_paths(args.inputs)
        t1 = time.perf_counter()

        preset_req = getattr(args, "preset", "auto")
        preset_eff = preset_req
        stats: Dict[str, float] = {}
        if preset_req == "auto":
            preset_eff, stats = auto_select_preset(
                paths,
                sample_files=int(getattr(args, "auto_sample_files", AUTO_SAMPLE_FILES)),
                sample_bytes=int(getattr(args, "auto_sample_bytes", AUTO_SAMPLE_BYTES)),
                entropy_hi=float(getattr(args, "auto_entropy_hi", AUTO_ENTROPY_HI)),
                gain_lo=float(getattr(args, "auto_gain_lo", AUTO_GAIN_LO)),
                fastgame_score_thr=float(getattr(args, "auto_fastgame_score_thr", AUTO_FASTGAME_SCORE_THR)),
                small_bytes_thr=int(getattr(args, "auto_small_bytes_thr", 256 * 1024 * 1024)),
                small_files_thr=int(getattr(args, "auto_small_files_thr", 8192)),
            )
        t2 = time.perf_counter()

        out = {
            "mode": "plan-only",
            "version": VERSION_STR,
            "preset_requested": preset_req,
            "preset_effective": preset_eff,
            "input_files": int(len(paths)),
            "timing_ms": {
                "scan": round((t1 - t0) * 1000.0, 3),
                "select": round((t2 - t1) * 1000.0, 3),
                "total": round((t2 - t0) * 1000.0, 3),
            },
            "auto": stats,
        }
        print(json.dumps(out, sort_keys=True, separators=(",", ":")))
        return

    build_archive(
        pathlib.Path(args.archive),
        args.inputs,
        chunk_size=args.chunk_size,
        preset=args.preset,
        mp_gate=args.mp_gate,
        mp_cdc_big=getattr(args,'mp_cdc_big', DEF_MP_CDC_BIG),
        mp_cdc_cutoff=getattr(args,'mp_cdc_cutoff', DEF_MP_CDC_CUTOFF),
        profile=args.profile,
        prefer_codec=args.codec,
        zstd_level=args.zstd_level,
        jobs=args.jobs,
        dedupe=(args.dedupe and (not args.no_dedupe)),
        gate_enabled=(not args.no_gate),
        probe_bytes=args.probe_bytes,
        probe_level=args.probe_level,
        auto_sample_files=args.auto_sample_files,
        auto_sample_bytes=args.auto_sample_bytes,
        auto_entropy_hi=args.auto_entropy_hi,
        auto_gain_lo=args.auto_gain_lo,
        auto_fastgame_score_thr=args.auto_fastgame_score_thr,
        auto_small_bytes_thr=args.auto_small_bytes_thr,
        auto_small_files_thr=args.auto_small_files_thr,
        thr=args.thr,
        thr_small=args.thr_small,
        small_cutoff=args.small_cutoff,
        whole_max=args.whole_max,
        min_gain=args.min_gain,
        micro_enabled=(args.micro and (not args.no_micro)),
        dna_enabled=(args.dna and (not args.no_dna)),
        pi_enabled=(args.pi and (not args.no_pi)),
        verbose_gate=args.verbose_gate,
        deterministic=args.deterministic,
        reproducible=args.reproducible,
    )

def cmd_info(args: argparse.Namespace) -> None:
    arc = parse_archive(pathlib.Path(args.archive))
    raw_sum = sum(f.raw_size for f in arc.files)
    arc_bytes = arc.path.stat().st_size
    ratio = arc_bytes / float(raw_sum if raw_sum else 1)
    counts = {CODEC_STORE: 0, CODEC_ZSTD: 0, CODEC_ZLIB: 0}
    tag_counts: Dict[bytes, int] = {}
    for bm in arc.blocks.values():
        counts[bm.codec] = counts.get(bm.codec, 0) + 1
        tag_counts[bm.tag] = tag_counts.get(bm.tag, 0) + 1

    print(f"Archive: {arc.path}")
    print(f"  magic: {MAGIC!r}  version: {FORMAT_VERSION}  czp_version: {arc.head.get('format', {}).get('version_str', '?')}")
    print(f"  files: {len(arc.files)}")
    print(f"  blocks: {len(arc.blocks)}")
    print(f"  sum raw sizes: {raw_sum}")
    print(f"  archive bytes: {arc_bytes}")
    print(f"  archive/raw ratio: {ratio:.4f}")
    fmt = arc.head.get("format", {})
    print(f"  head.format: {fmt}")
    print(f"  head.policy: reproducible={bool(arc.head.get('reproducible'))} source_date_epoch={arc.head.get('source_date_epoch')} created_utc_ns={arc.head.get('created_utc_ns')}")
    if getattr(args, "head_full", False):
        print("  head.full_json: " + json.dumps(arc.head, sort_keys=True, separators=(",", ":")))

    if "stats" in arc.head:
        print(f"  stats: {arc.head['stats']}")
    print("  block tags: " + ", ".join(f"{k.decode('ascii')}={v}" for k, v in sorted(tag_counts.items(), key=lambda kv: kv[0])))
    print("  codecs: " + ", ".join(f"{CODEC_NAME[k]}={v}" for k, v in counts.items() if v))

def cmd_list(args: argparse.Namespace) -> None:
    arc = parse_archive(pathlib.Path(args.archive))
    print(f"{arc.path}: {len(arc.files)} file(s)")
    for fm in arc.files:
        print(f"{fm.raw_size:12d}  {fm.path}")

def cmd_test(args: argparse.Namespace) -> None:
    arc = parse_archive(pathlib.Path(args.archive))
    stream_test_archive(arc)

def cmd_extract(args: argparse.Namespace) -> None:
    arc = parse_archive(pathlib.Path(args.archive))
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    for fm in arc.files:
        p = outdir / pathlib.Path(fm.path)
        ensure_parent(p)
        data = reconstruct_file_bytes(arc, fm)
        with p.open("wb") as f:
            f.write(data)
        # Restore mtimes by default for normal archives; skip by default for reproducible archives.
        restore = True
        if arc.head.get("reproducible"):
            restore = False
        if getattr(args, "restore_mtime", False):
            restore = True
        if getattr(args, "no_restore_mtime", False):
            restore = False
        if restore:
            try:
                os.utime(p, ns=(fm.mtime_ns, fm.mtime_ns))
            except Exception:
                pass
    print(f"OK: extracted {len(arc.files)} file(s) to {outdir}")

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="czp.py", add_help=True)
    ap.add_argument("--bitio-selftest", dest="bitio_selftest", action="store_true", default=False,
                    help="run deterministic canonical bit-IO selftest (diagnostic; no archive bytes affected)")
    ap.add_argument("--no-bitio-selftest", dest="bitio_selftest", action="store_false",
                    help="explicitly disable bit-IO selftest")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pa = sub.add_parser("a", help="create archive")
    pa.add_argument("archive")
    pa.add_argument("inputs", nargs="+")
    pa.add_argument("--plan-only", action="store_true", help="plan-only: scan inputs + (auto) preset selection; emit 1 JSON line; do not write archive")
    pa.add_argument("--chunk-size", type=int, default=DEF_CHUNK_SIZE)
    pa.add_argument("--codec", choices=["auto","zstd","zlib","store"], default="auto")
    pa.add_argument("--zstd-level", type=int, default=3)
    pa.add_argument("--jobs", type=int, default=0, help="parallel workers for chunk compression (0=auto, 1=off)")
    pa.add_argument("--deterministic", dest="deterministic", action="store_true", default=True,
                    help="deterministic output (DEFAULT); freezes archive-created timestamps and canonicalizes metadata")
    pa.add_argument("--no-deterministic", dest="deterministic", action="store_false",
                    help="disable deterministic output (not recommended)")
    pa.add_argument("--reproducible", dest="reproducible", action=argparse.BooleanOptionalAction, default=True,
                    help="content-only determinism across machines (DEFAULT); implies --deterministic; canonicalizes file mtimes using $SOURCE_DATE_EPOCH or 0")
    pa.add_argument("--min-gain", type=float, default=DEF_MIN_GAIN)
    pa.add_argument("--dedupe", action="store_true", default=DEF_DEDUPE)
    pa.add_argument("--no-dedupe", action="store_true")
    pa.add_argument("--micro", action="store_true", default=MICRO_ENABLE)
    pa.add_argument("--no-micro", action="store_true")
    pa.add_argument("--dna", action="store_true", default=DNA_ENABLE)
    pa.add_argument("--no-dna", action="store_true")
    pa.add_argument("--pi", action="store_true", default=PI_ENABLE)
    pa.add_argument("--no-pi", action="store_true")
    pa.add_argument("--no-gate", action="store_true")
    pa.add_argument("--probe-bytes", type=int, default=DEF_PROBE_BYTES)
    pa.add_argument("--probe-level", type=int, default=DEF_PROBE_LEVEL)
    pa.add_argument("--preset", choices=["auto","default","fast-game"], default=DEF_PRESET,
                    help="Preset: auto (deterministic selector), default (balanced), fast-game (less probing; suppress lanes on likely high-entropy blobs).")
 
    # Auto preset tuning (only used when --preset auto)
    pa.add_argument("--auto-sample-files", type=int, default=AUTO_SAMPLE_FILES,
                    help="auto preset: max files to sample (deterministic, bounded)")
    pa.add_argument("--auto-sample-bytes", type=int, default=AUTO_SAMPLE_BYTES,
                    help="auto preset: bytes read per sampled file (from start)")
    pa.add_argument("--auto-entropy-hi", type=float, default=AUTO_ENTROPY_HI,
                    help="auto preset: entropy threshold (0-8) considered high")
    pa.add_argument("--auto-gain-lo", type=float, default=AUTO_GAIN_LO,
                    help="auto preset: probe gain threshold considered low")
    pa.add_argument("--auto-fastgame-score-thr", type=float, default=AUTO_FASTGAME_SCORE_THR,
                    help="auto preset: fraction of sampled files that must look 'fast-game' to pick fast-game")
    pa.add_argument("--auto-small-bytes-thr", type=int, default=256 * 1024 * 1024,
                    help="auto preset: if dataset bytes <= this, force fast-game (performance)")
    pa.add_argument("--auto-small-files-thr", type=int, default=8192,
                    help="auto preset: if dataset file-count <= this, force fast-game (performance)")
    pa.add_argument("--mp-gate", action="store_true", default=DEF_MP_GATE,
                    help="multiprocessing Stage-1 gating (fast-game). Deterministic merge by index.")
    pa.add_argument("--profile", action="store_true", default=DEF_PROFILE,
                    help="print per-phase timing breakdown (does not affect archive bytes).")
    pa.add_argument("--thr", type=float, default=DEF_THR)
    pa.add_argument("--thr-small", type=float, default=DEF_THR_SMALL)
    pa.add_argument("--small-cutoff", type=int, default=DEF_SMALL_CUTOFF)
    pa.add_argument("--whole-max", type=int, default=DEF_WHOLE_MAX)
    pa.add_argument("--verbose-gate", action="store_true")
    pa.set_defaults(func=cmd_add)

    pl = sub.add_parser("l", help="list")
    pl.add_argument("archive")
    pl.set_defaults(func=cmd_list)

    pi = sub.add_parser("info", help="info")
    pi.add_argument("archive")
    pi.add_argument("--head-full", action="store_true", help="print full HEAD JSON (canonical)")
    pi.set_defaults(func=cmd_info)

    pt = sub.add_parser("t", help="test")
    pt.add_argument("archive")
    pt.add_argument("--bitio-selftest", dest="bitio_selftest", action="store_true", default=False,
                    help="run deterministic canonical bit-IO selftest (diagnostic; no archive bytes affected)")
    pt.add_argument("--no-bitio-selftest", dest="bitio_selftest", action="store_false",
                    help="explicitly disable bit-IO selftest (default)")
    pt.set_defaults(func=cmd_test)

    px = sub.add_parser("x", help="extract")
    px.add_argument("archive")
    px.add_argument("outdir")
    px.add_argument("--restore-mtime", action="store_true", help="force restoring mtimes on extract (overrides archive defaults)")
    px.add_argument("--no-restore-mtime", action="store_true", help="do not restore mtimes on extract (overrides archive defaults)")
    px.set_defaults(func=cmd_extract)

    return ap

def main() -> None:
    args = build_argparser().parse_args()
    # Selftests must never run implicitly; they are explicit diagnostics only.
    if not hasattr(args, "func"):
        build_argparser().print_help()
        return
    if getattr(args, "bitio_selftest", False):
        _bitio_selftest(verbose=True)
    args.func(args)

if __name__ == "__main__":
    main()

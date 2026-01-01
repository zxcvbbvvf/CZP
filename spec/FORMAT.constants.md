# CZP Format Constants (extracted)

> Auto-extracted from `czp.py` in this repo checkout.

```text
VERSION_STR = f"{TOOL_VERSION}+{BUILD_ID}"
FORMAT_VERSION = 1  # CZP3 v1
MAGIC = b"CZP3"
MAX_SECTION = 2 * 1024 * 1024 * 1024  # 2GiB guard
CODEC_STORE = 0
CODEC_ZLIB = 2
CODEC_ZSTD = 1
SPANF_LAST = 1 << 0
SPANF_MICRO = 1 << 1
SPANF_STORE = 1 << 2
SPANF_DNA = 1 << 3
SPANF_PI = 1 << 4
SPANF_HIGHENT = 1 << 5  # gate diagnostic: high entropy suppression active
SEC_HDR = struct.Struct("<4sQ")   # tag, payload_len
FILE_HDR = struct.Struct("<4sH")  # magic, version
CHNK_HDR = struct.Struct("<QBBHIII32s")
BLK2_HDR = struct.Struct("<QBBHIIII")
DNA1_HDR = struct.Struct("<QBBHHHIII")
PI01_HDR = struct.Struct("<QBBHIIII")
FILEIDX_HEAD = struct.Struct("<I")
FILE_REC_HDR = struct.Struct("<HQQII BBH")
SPAN_REC = struct.Struct("<QII")
```
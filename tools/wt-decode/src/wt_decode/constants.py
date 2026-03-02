from pathlib import Path

# Default paths for finding resources
DEFAULT_PROTO_PATHS = [
    Path.home() / "mongo/src/mongo/db/modules/atlas/src/disagg_storage/sls-proto/dist/storage/etc/protos",
    Path("/data/db/job0/mongorunner/protos"),
]

DEFAULT_PAGEDECRYPTOR_PATHS = [
    Path.home() / "mongo/bazel-bin/src/mongo/db/modules/atlas/src/disagg_storage/encryption/pagedecryptor",
    Path("/data/db/job0/mongorunner/pagedecryptor")
]

DEFAULT_KEY_FILE = Path("/data/db/job0/mongorunner/decrypt_key")
DEFAULT_PAGE_SERVER = "172.17.0.1:20044"

METADATA_TABLE_ID = 9
TURTLE_TABLE_ID = 1
TURTLE_PAGE_ID = 1

import sys
from pathlib import Path

# The generated gRPC/protobuf stubs use absolute imports like ``from ds.v1 import ...``
# which require the package directory itself to be on sys.path.  We add it once here
# at package-init time so every module that imports the stubs will work.
_PACKAGE_DIR = str(Path(__file__).resolve().parent)
if _PACKAGE_DIR not in sys.path:
    sys.path.insert(0, _PACKAGE_DIR)

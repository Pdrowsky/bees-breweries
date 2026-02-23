# pipeline config file

import os
from pathlib import Path

LAKE_ROOT = os.getenv("LAKE_ROOT", str(Path.cwd() / "storage"))
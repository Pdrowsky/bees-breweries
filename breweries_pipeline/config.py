# pipeline config file

import os
from pathlib import Path

#TODO: set it up on docker-compose
LAKE_ROOT = os.getenv("LAKE_ROOT", str(Path.cwd() / "storage"))
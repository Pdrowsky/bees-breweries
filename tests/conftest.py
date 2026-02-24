import pytest
import logging
import sys
import os
from pathlib import Path

# ==================== PATH SETUP ====================

# Get the project root (parent of tests directory)
project_root = Path(__file__).parent.parent

# Add project root to Python path
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Verify the path is correct
assert (project_root / "breweries_pipeline").exists(), \
    f"breweries_pipeline not found in {project_root}"


# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
logger.info(f"Project root: {project_root}")
logger.info(f"Python path includes: {project_root}")


# ==================== MARKERS ====================

def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "api: mark test as requiring API (external dependency)"
    )
from importlib.metadata import version

try:
    __version__ = version('bdi_api')  # Use the package name from pyproject.toml (now "bdi_api")
except Exception:
    __version__ = "0.0.1"  # Fallback to the version in pyproject.toml if metadata isn’t found
__name__ = "bdi_api"

# tests package initializer: ensure project root is on sys.path so tests can import source_jobs.*
import os
import sys

# Add the project root (parent of this tests directory) to sys.path
PACKAGE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)


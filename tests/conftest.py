"""Shared pytest configuration and fixtures."""

import os
import sys

# Ensure the project root is on the path so `from axiom import ...` works
# regardless of how pytest is invoked.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

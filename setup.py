"""Minimal setup.py shim for editable installs with older pip versions.

All project metadata is defined in pyproject.toml.
Use `pip install -e .` (pip >= 21.3) or `pip install -e .[dev]` for development.
"""
from setuptools import setup

setup()

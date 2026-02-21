# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Make src/ importable so autodoc can introspect the modules
sys.path.insert(0, os.path.abspath("../../src"))

# ---------------------------------------------------------------------------
# Project information
# ---------------------------------------------------------------------------
project = "GradCafe Analytics"
copyright = "2026, JHU Software Concepts"
author = "JHU Software Concepts"
release = "4.0.0"

# ---------------------------------------------------------------------------
# General configuration
# ---------------------------------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",       # auto-generate docs from docstrings
    "sphinx.ext.viewcode",      # add links to source code
    "sphinx.ext.napoleon",      # support Google/NumPy docstring styles
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------
html_theme = "alabaster"
html_static_path = ["_static"]

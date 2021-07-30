# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys
sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------

project = 'RQ'
copyright = '2021, RQ'
author = 'RQ'

# The full version, including alpha/beta/rc tags
from rq import VERSION
release = VERSION


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc"
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'basic'
html_logo = '_static/ribbon.png'
html_favicon = '_static/favicon.png'
html_sidebars = {
    '**': ['badges.html', 'localtoc.html', 'globaltoc.html', 'relations.html', 'darkmode.html', 'searchbox.html']
}
html_js_files = ['darkmode.js']
html_context = {
    'badges': [
        {
            'alt': 'Build status',
            'img': 'https://github.com/rq/rq/workflows/Test%20rq/badge.svg',
            'link': 'https://github.com/rq/rq/actions?query=workflow%3A%22Test+rq%22'
        },
        {
            'alt': 'PyPI',
            'img': 'https://img.shields.io/pypi/pyversions/rq.svg',
            'link': 'https://pypi.python.org/pypi/rq'
        },
        {
            'alt': 'Coverage',
            'img': 'https://codecov.io/gh/rq/rq/branch/master/graph/badge.svg',
            'link': 'https://codecov.io/gh/rq/rq'
        }
    ]
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

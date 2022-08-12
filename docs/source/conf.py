# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import shutil
import sys
from typing import Any, Dict, Optional

# -- Project information -----------------------------------------------------

# General information about the project.
project = "Sentinel Hub"
project_copyright = "2018, Sentinel Hub"
author = "Sinergise EO research team"
doc_title = "sentinelhub Documentation"


# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The release is read from __init__ file and version is shortened release string.
for line in open(os.path.join(os.path.dirname(__file__), "../../sentinelhub/_version.py")):
    if line.find("__version__") >= 0:
        release = line.split("=")[1].strip()
        release = release.strip('"').strip("'")
version = release.rsplit(".", 1)[0]

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.githubpages",
    "nbsphinx",
    "sphinx_rtd_theme",
    "m2r2",
]

# Include typehints in descriptions
autodoc_typehints = "description"

# Both the class’ and the __init__ method’s docstring are concatenated and inserted.
autoclass_content = "both"

# Content is in the same order as in module
autodoc_member_order = "bysource"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# General information about the project.


# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = "en"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ["**.ipynb_checkpoints", "custom_reference*"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# Mock imports that won't and don't have to be installed in ReadTheDocs environment
autodoc_mock_imports = ["boto3", "botocore", "pytest"]

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_logo = "./sentinel-hub-by_sinergise-dark_background.png"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
# html_theme_options = {
#     "rightsidebar": "true",
#     "relbarbgcolor": "black"}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# This is required for the alabaster theme
# refs: http://alabaster.readthedocs.io/en/latest/installation.html#sidebars
# html_sidebars = {
#    '**': [
#        'about.html',
#        'navigation.html',
#        'relations.html',  # needs 'show_related': True theme option to display
#        'searchbox.html',
#        'donate.html',
#    ]
# }

# analytics
html_js_files = [("https://cdn.usefathom.com/script.js", {"data-site": "BILSIGFB", "defer": "defer"})]


# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = "sentinelhub_doc"
# show/hide links for source
html_show_sourcelink = False

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    # 'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',
    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, "sentinelhub.tex", doc_title, author, "manual"),
]

# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [(master_doc, "sentinelhub", doc_title, [author], 1)]

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, "sentinelhub", doc_title, author, "sentinelhub", "One line description of project.", "Miscellaneous"),
]

# -- Options for Epub output ----------------------------------------------

# Bibliographic Dublin Core info.
epub_title = project
epub_author = author
epub_publisher = author
epub_copyright = project_copyright

# The unique identifier of the text. This can be a ISBN number
# or the project homepage.
#
# epub_identifier = ''

# A unique identification for the text.
#
# epub_uid = ''

# A list of files that should not be packed into the epub file.
epub_exclude_files = ["search.html"]

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {"https://docs.python.org/3.8/": None}


MARKDOWNS_FOLDER = "./markdowns"
shutil.rmtree(MARKDOWNS_FOLDER, ignore_errors=True)
os.mkdir(MARKDOWNS_FOLDER)


def process_readme():
    """Function which will process README.md file and create INTRO.md"""
    with open("../../README.md", "r") as file:
        readme = file.read()

    readme = readme.replace("[`", "[").replace("`]", "]")

    chapters = [[]]
    for line in readme.split("\n"):
        if line.strip().startswith("## "):
            chapters.append([])
        if line.startswith("<img"):
            line = "<p></p>"

        chapters[-1].append(line)

    chapters = ["\n".join(chapter) for chapter in chapters]

    intro = "\n".join(
        [
            chapter
            for chapter in chapters
            if not (chapter.startswith("## Install") or chapter.startswith("## Documentation"))
        ]
    )

    with open(os.path.join(MARKDOWNS_FOLDER, "INTRO.md"), "w") as file:
        file.write(intro)


process_readme()

# Auto-generate documentation pages
current_dir = os.path.abspath(os.path.dirname(__file__))
repository_dir = os.path.join(current_dir, "..", "..")
reference_dir = os.path.join(current_dir, "reference")
custom_reference_dir = os.path.join(current_dir, "custom_reference")
custom_reference_files = {filename.rsplit(".", 1)[0] for filename in os.listdir(custom_reference_dir)}

module = os.path.join(repository_dir, "sentinelhub")

APIDOC_EXCLUDE = [os.path.join(module, "commands.py"), os.path.join(module, "aws", "commands.py")]
APIDOC_OPTIONS = ["--module-first", "--separate", "--no-toc", "--templatedir", os.path.join(current_dir, "_templates")]

shutil.rmtree(reference_dir, ignore_errors=True)
shutil.copytree(custom_reference_dir, reference_dir)


def run_apidoc(_):
    from sphinx.ext.apidoc import main

    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

    main(["-e", "-o", reference_dir, module, *APIDOC_EXCLUDE, *APIDOC_OPTIONS])


def configure_github_link(_app: Any, pagename: str, _templatename: Any, context: Dict[str, Any], _doctree: Any) -> None:
    """Because some pages are auto-generated and some are copied from their original location the link "Edit on GitHub"
    of a page is wrong. This function computes a custom link for such pages and saves it to a custom meta parameter
    `github_url` which is then picked up by `sphinx_rtd_theme`.

    Resources to understand the implementation:
    - https://www.sphinx-doc.org/en/master/extdev/appapi.html#event-html-page-context
    - https://dev.readthedocs.io/en/latest/design/theme-context.html
    - https://sphinx-rtd-theme.readthedocs.io/en/latest/configuring.html?highlight=github_url#file-wide-metadata
    - https://github.com/readthedocs/sphinx_rtd_theme/blob/1.0.0/sphinx_rtd_theme/breadcrumbs.html#L35
    """
    # ReadTheDocs automatically sets the following parameters but for local testing we set them manually:
    show_link = context.get("display_github")
    context["display_github"] = True if show_link is None else show_link
    context["github_user"] = context.get("github_user") or "sentinel-hub"
    context["github_repo"] = context.get("github_repo") or "sentinelhub-py"
    context["github_version"] = context.get("github_version") or "develop"
    context["conf_py_path"] = context.get("conf_py_path") or "/docs/source/"

    if pagename.startswith("examples/"):
        github_url = create_github_url(context, conf_py_path="/")

    elif pagename.startswith("reference/"):
        filename = pagename.split("/", 1)[1]

        if filename in custom_reference_files:
            github_url = create_github_url(context, pagename=f"custom_reference/{filename}")
        else:
            filename = filename.replace(".", "/")
            full_path = os.path.join(repository_dir, f"{filename}.py")
            is_module = os.path.exists(full_path)

            github_url = create_github_url(
                context,
                theme_vcs_pageview_mode="blob" if is_module else "tree",
                conf_py_path="/",
                pagename=filename.replace(".", "/"),
                page_source_suffix=".py" if is_module else "",
            )
    else:
        return

    context["meta"] = context.get("meta") or {}
    context["meta"]["github_url"] = github_url


def create_github_url(
    context: Dict[str, Any],
    theme_vcs_pageview_mode: Optional[str] = None,
    conf_py_path: Optional[str] = None,
    pagename: Optional[str] = None,
    page_source_suffix: Optional[str] = None,
) -> str:
    """Creates a GitHub URL from context in exactly the same way as in
    https://github.com/readthedocs/sphinx_rtd_theme/blob/1.0.0/sphinx_rtd_theme/breadcrumbs.html#L39

    The function allows URL customization by overwriting certain parameters.
    """
    github_host = context.get("github_host") or "github.com"
    github_user = context.get("github_user", "")
    github_repo = context.get("github_repo", "")
    theme_vcs_pageview_mode = theme_vcs_pageview_mode or context.get("theme_vcs_pageview_mode") or "blob"
    github_version = context.get("github_version", "")
    conf_py_path = conf_py_path or context.get("conf_py_path", "")
    pagename = pagename or context.get("pagename", "")
    page_source_suffix = context.get("page_source_suffix", "") if page_source_suffix is None else page_source_suffix
    return (
        f"https://{github_host}/{github_user}/{github_repo}/{theme_vcs_pageview_mode}/"
        f"{github_version}{conf_py_path}{pagename}{page_source_suffix}"
    )


def setup(app):
    app.connect("builder-inited", run_apidoc)
    app.connect("html-page-context", configure_github_link)

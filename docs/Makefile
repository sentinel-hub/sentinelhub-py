# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
PROJDIR        = ../sentinelhub
SOURCEDIR      = source
BUILDDIR       = build

AUTODOC        = sphinx-apidoc -o
AUTODOCEXCLUDE = ../sentinelhub/commands.py
AUTODOCOPTS    = --separate --no-toc

SPHINXOPTS     =
SPHINXBUILD    = python3 -msphinx
SPHINXPROJ     = sentinelhub


# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

autodoc: Makefile
	$(AUTODOC) $(SOURCEDIR) $(PROJDIR) $(AUTODOCEXCLUDE) $(AUTODOCOPTS)

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	rm -rf ./build/
	cp -vr ../examples source/
	$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	rm -r source/examples source/markdowns

# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
AUTODOC = sphinx-apidoc
AUTODOCOPTS   = --separate --no-toc
SPHINXOPTS    =
SPHINXBUILD   = python3 -msphinx
SPHINXPROJ    = sentinelhub
PROJDIR       = ../sentinelhub
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	rm -rf ./build/
	cp -vr ../examples source/
	@$(SPHINXAUTODOC) -o "$(BUILDDIR)" "$(PROJDIR)" $(AUTODOCOPTS)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	rm -r source/examples source/markdowns

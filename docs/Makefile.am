## This is a automake file, part of Unidata's netCDF package.
# Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
# 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
# 2015, 2016, 2017, 2018
# University Corporation for Atmospheric Research/Unidata.

# See netcdf-c/COPYRIGHT file for more info.

# These files will be included with the dist.
EXTRA_DIST = netcdf.m4 DoxygenLayout.xml Doxyfile.in footer.html	\
mainpage.dox tutorial.dox architecture.dox internal.md			\
windows-binaries.md dispatch.md building-with-cmake.md CMakeLists.txt groups.dox	\
notes.md install-fortran.md credits.md auth.md filters.md		\
obsolete/fan_utils.html indexing.dox inmemory.md FAQ.md			\
known_problems.md COPYRIGHT.md inmeminternal.dox testserver.dox		\
byterange.md nczarr.md quantize.md all-error-codes.md                   \
quickstart_paths.md cloud.md header.html attribute_conventions.md \
file_format_specifications.md quickstart_filters.md quickstart_env.md \
doxygen-awesome-css netcdf-50x50.png pluginpath.md

# Turn off parallel builds in this directory.
.NOTPARALLEL:

# To build documentation you must have M4 in their path.
M4 = m4

SUBDIRS = images

if BUILD_DOCS

# Copy man pages.
#directory = $(top_srcdir)/docs/man/man3/
#dist_man_MANS = $(docsdirectory)/man_page_1.3 $(directory)/man_page_2.3
# $(directory)/man_page_1.3: doxyfile.stamp
# $(directory)/man_page_2.3: doxyfile.stamp

# Note: in order to work with distcheck,
# the Doxyfile needs to be preprocessed
# to insert actual location of $(srcdir)
#BUILT_SOURCES = Doxyfile.tmp

#Doxyfile.tmp: Doxyfile
#	sed -e 's|$$[({]rootdir[})]|$(abs_top_srcdir)|g' <Doxyfile > ./Doxyfile.tmp

all-local: doxyfile.stamp
clean-local:
	rm -rf $(top_builddir)/docs/man
	rm -rf $(top_builddir)/docs/html
	rm -rf $(top_builddir)/docs/latex
	rm -rf $(top_builddir)/docs/latex_main
endif

# Timestamp to prevent rebuilds.
# We must do this twice. The first time
# builds the tag files. The second time
# includes them in the documentation.

doxyfile.stamp:
	$(DOXYGEN) -u $(top_builddir)/docs/Doxyfile && \
	$(DOXYGEN) -w html $(top_builddir)/docs/header.html \
		$(top_builddir)/docs/footer.html $(top_builddir)/docs/custom_style.css $(top_builddir)/docs/Doxyfile && \
	$(DOXYGEN) $(top_builddir)/docs/Doxyfile && \
	cp $(top_srcdir)/docs/auth.md $(top_srcdir)/docs/obsolete/fan_utils.html html

CLEANFILES = doxyfile.stamp

# This builds the docs from source, if necessary, and tars up
# everything needed for the website. Run this and copy the resulting
# tarball to the /contents/netcdf/docs directory to update the on-line
# docs.
# UPDATE Nov 12, 2021: docs now published to Unidata artifacts server.
web-tarball: doxyfile.stamp
	cd html; tar cf ../netcdf_docs.tar *
	gzip -f netcdf_docs.tar

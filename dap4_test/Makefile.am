# Disable Dap4 Remote Tests until the test server is working
if AX_IGNORE
if ENABLE_DAP_REMOTE_TESTS
# Note: This program name was changed to findtestserver4
# to avoid cmake complaint about duplicate targets.
noinst_PROGRAMS += findtestserver4 pingurl4
findtestserver4_SOURCES = findtestserver4.c
pingurl4_SOURCES = pingurl4.c
if BUILD_UTILITIES
  # relies on ncdump
  TESTS += test_remote.sh
endif
endif
endif #AX_IGNORE

endif #ENABLE_DAP4

EXTRA_DIST = test_parse.sh test_meta.sh test_data.sh \
             test_raw.sh test_remote.sh test_hyrax.sh test_fillmismatch.sh \
             tst_curlopt.sh d4test_common.sh \
	     daptestfiles dmrtestfiles cdltestfiles nctestfiles misctestfiles \
	     baseline baselineraw baselineremote CMakeLists.txt

CLEANFILES = *.exe
# This should only be left behind if using parallel io
CLEANFILES += tmp_*

DISTCLEANFILES = findtestserver4.c pingurl4.c

# One last thing
BUILT_SOURCES = .daprc

.daprc:
	echo "#DAPRC" >.daprc

clean-local: clean-local-check

.PHONY: clean-local-check

clean-local-check:
	-rm -rf results
	-rm -f .dodsrc .daprc

# The shell file maketests.sh is used to build the testdata
# for dap4 testing. It creates and fills the directories
# daptestfiles, dmrtestfiles, and cdltestfiles
# As a rule, this will only be invoked if there was a change
# in the GenerateRaw.java program in the thredds tree.
maketests::
	bash ./maketests.sh

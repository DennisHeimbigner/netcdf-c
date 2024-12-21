# WARNING THIS FILE MUST BE KEPT CONSISTENT WITH v3_build.cmake

# Copy selected V2 files from nczarr_test to v3_nczarr_test.
# We do not use AC_CONFIG_FILES but rather rely on the 'cp' program.

# Shell scripts that are copies of same files from nczarr_test
TESTFILES_NCZARR_SH="test_nczarr.sh run_chunkcases.sh run_corrupt.sh run_external.sh run_fillonlyz.sh run_filter.sh run_filterinstall.sh run_filter_misc.sh run_filter_vlen.sh run_interop.sh run_jsonconvention.sh run_misc.sh run_nccopy5.sh run_nccopyz.sh run_ncgen4.sh run_nczarr_fill.sh run_nczfilter.sh run_newformat.sh run_notzarr.sh run_nulls.sh run_perf_chunks1.sh run_purezarr.sh run_quantize.sh run_scalar.sh run_specific_filters.sh run_strings.sh run_unknown.sh run_unlim_io.sh run_ut_map.sh run_ut_mapapi.sh run_ut_misc.sh"

# Program files that are copies of same files from nczarr_test
TESTFILES_NCZARR_C="test_chunking.c test_filter_vlen.c test_h5_endians.c test_put_vars_two_unlim_dim.c test_quantize.c test_unlim_vars.c tst_pure_awssdk.cpp bm_utils.c timer_utils.c test_utils.c test_utils.h bm_utils.h bm_timer.h testfilter.c testfilter_multi.c testfilter_misc.c testfilter_order.c testfilter_repeat.c"

# Script files that are copies of same files from nczarr_test
for u in ${TESTFILES_NCZARR_SH}; do
cp -f nczarr_test/$u v3_nczarr_test/$u 
chmod a+x v3_nczarr_test
done

# Program files that are copies of same files from nczarr_test
for u in ${TESTFILES_NCZARR_C}; do
cp -f nczarr_test/$u v3_nczarr_test/$u 
done

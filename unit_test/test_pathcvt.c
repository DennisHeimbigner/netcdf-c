/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
Test the NCpathcvt
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ncpathmgr.h"

#define DEBUG

#define NKINDS 4
static const char* kinds[NKINDS] = {"nix", "msys", "cygwin", "win"};

typedef struct Test {
    char* test;
    char* expected[NKINDS];
} Test;

/* Path conversion tests */
static Test PATHTESTS[] = {
{"/xxx/a/b",{"/xxx/a/b", NULL, NULL, NULL}},
{"d:/x/y",{ "/d/x/y", "/d/x/y",  "/cygdrive/d/x/y",  "d:\\x\\y"}},
{"/cygdrive/d/x/y",{ "/d/x/y", "/d/x/y", "/cygdrive/d/x/y",  "d:\\x\\y"}},
{"/d/x/y",{ "/d/x/y", "/d/x/y",  "/cygdrive/d/x/y",  "d:\\x\\y"}},
{"/cygdrive/d",{ "/d", "/d",  "/cygdrive/d",  "d:"}},
{"/d", {"/d", "/d",  "/cygdrive/d",  "d:"}},
{"/cygdrive/d/git/netcdf-c/dap4_test/daptestfiles/test_anon_dim.2.syn",{
    NULL,
    "/d/git/netcdf-c/dap4_test/daptestfiles/test_anon_dim.2.syn",
    "/cygdrive/d/git/netcdf-c/dap4_test/daptestfiles/test_anon_dim.2.syn",
    "d:\\git\\netcdf-c\\dap4_test\\daptestfiles\\test_anon_dim.2.syn"}},
{"[dap4]file:///cygdrive/d/git/netcdf-c/dap4_test/daptestfiles/test_anon_dim.2.syn",{
  "[dap4]file:///cygdrive/d/git/netcdf-c/dap4_test/daptestfiles/test_anon_dim.2.syn",
  NULL, NULL}},
{NULL, {NULL, NULL, NULL, NULL}}
};

int
main(int argc, char** argv)
{
    Test* test;
    int failcount = 0;
    char* cvt = NULL;

    for(test=PATHTESTS;test->test;test++) {
	int i;
	for(i=0;i<NKINDS;i++) {
	    if(test->expected[i] == NULL) {
#ifdef DEBUG
	        fprintf(stderr,"TEST %s: %s ignored\n",kinds[i],test->test);	    
#endif
	        continue;
	    }
   	    cvt = NCpathcvt_test(test->test,kinds[i]);
#ifdef DEBUG
	    fprintf(stderr,"TEST %s: input: |%s| expected=|%s| actual=|%s|: ",
			kinds[i],test->test,test->expected[i],cvt);
#endif
	    fflush(stderr); fflush(stdout);
	    if(cvt == NULL) {
#ifdef DEBUG
		fprintf(stderr," ILLEGAL");
#endif
		failcount++;
	    } else if(strcmp(cvt,test->expected[i]) != 0) {
#ifdef DEBUG
		fprintf(stderr," FAIL");
#endif
	        failcount++;
	    } else {
#ifdef DEBUG
		fprintf(stderr," PASS");
#endif
	    }
#ifdef DEBUG
	    fprintf(stderr,"\n");
#endif	    
	    nullfree( cvt); cvt = NULL;
	}
    }
    nullfree(cvt);
    fprintf(stderr,"%s test_pathcvt\n",failcount > 0 ? "***FAIL":"***PASS");
    return (failcount > 0 ? 1 : 0);
}

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
#include "netcdf.h"
#include "ncpathmgr.h"

#define DEBUG

#define NKINDS 5
static const int kinds[NKINDS] = {NCPD_NIX,NCPD_MSYS,NCPD_CYGWIN,NCPD_WIN,NCPD_MINGW};

typedef struct Test {
    char* test;
    char* expected[NKINDS];
} Test;

/* Path conversion tests */
static Test PATHTESTS[] = {
{"/xxx/a/b",{"/xxx/a/b", "/xxx/a/b", "/cygdrive/c/xxx/a/b", "c:\\xxx\\a\\b", "c:\\xxx\\a\\b"}},
{"d:/x/y",{ "/d/x/y", "/d/x/y",  "/cygdrive/d/x/y",  "d:\\x\\y", "d:\\x\\y"}},
{"d:\\x\\y",{ "/d/x/y", "/d/x/y",  "/cygdrive/d/x/y",  "d:\\x\\y", "d:\\x\\y"}},
{"/cygdrive/d/x/y",{ "/d/x/y", "/d/x/y", "/cygdrive/d/x/y",  "d:\\x\\y", "d:\\x\\y"}},
{"/d/x/y",{ "/d/x/y", "/d/x/y",  "/cygdrive/d/x/y",  "d:\\x\\y", "d:\\x\\y"}},
{"/cygdrive/d",{ "/d", "/d",  "/cygdrive/d",  "d:", "d:"}},
{"/d", {"/d", "/d",  "/cygdrive/d",  "d:", "d:"}},
{"/cygdrive/d/git/netcdf-c/dap4_test/test_anon_dim.2.syn",{
    "/d/git/netcdf-c/dap4_test/test_anon_dim.2.syn",
    "/d/git/netcdf-c/dap4_test/test_anon_dim.2.syn",
    "/cygdrive/d/git/netcdf-c/dap4_test/test_anon_dim.2.syn",
    "d:\\git\\netcdf-c\\dap4_test\\test_anon_dim.2.syn",
    "d:\\git\\netcdf-c\\dap4_test\\test_anon_dim.2.syn"}},
/* Test relative path */
{"x/y",{ "x/y", "x/y", "x/y",  "x\\y", "x/y"}},
{"x\\y",{ "x/y", "x/y", "x/y",  "x\\y", "x/y"}},
#ifndef _WIN32X
/* Test utf8 path */
{"/海/海",{ "/海/海", "/海/海", "/cygdrive/c/海/海",  "c:\\海\\海", "c:\\海\\海"}},
/* Test network path */
{"//git/netcdf-c/dap4_test",{
    "/@/git/netcdf-c/dap4_test",
    "/@/git/netcdf-c/dap4_test",
    "/cygdrive/@/git/netcdf-c/dap4_test",
    "\\\\git\\netcdf-c\\dap4_test",
    "\\\\git\\netcdf-c\\dap4_test"}},
#endif
{NULL, {NULL, NULL, NULL, NULL, NULL}}
};

char* macros[128];

/*Forward */
static const char* kind2string(int kind);
static char* expand(const char* s);
static void setmacros(void);
static void reclaimmacros(void);

int
main(int argc, char** argv)
{
    Test* test;
    int failcount = 0;
    char* cvt = NULL;
    char* unescaped = NULL;
    char* expanded = NULL;
    int k;
    int drive = 'c';

    nc_initialize();

    setmacros();

    /* Test localkind X path-kind */
    for(test=PATHTESTS;test->test;test++) {
        /* Iterate over the test paths */
        for(k=0;k<NKINDS;k++) {
	    int kind = kinds[k];
	    /* Compare output for the localkind */
            if(test->expected[k] == NULL) {
#ifdef DEBUG
	        fprintf(stderr,"TEST local=%s: %s ignored\n",kind2string(kind),test->test);
#endif
	        continue;
	    }
	    /* ensure that NC_shellUnescape does not affect result */
	    unescaped = NC_shellUnescape(test->test);	
	    expanded = expand(test->expected[k]);
   	    cvt = NCpathcvt_test(unescaped,kind,drive);
#ifdef DEBUG
	    fprintf(stderr,"TEST local=%s: input: |%s| expected=|%s| actual=|%s|: ",
			kind2string(kind),test->test,expanded,cvt);
#endif
	    fflush(stderr); fflush(stdout);
	    if(cvt == NULL) {
#ifdef DEBUG
		fprintf(stderr," ILLEGAL");
#endif
		failcount++;
	    } else if(strcmp(cvt,expanded) != 0) {
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
	    nullfree(unescaped); unescaped = NULL;
	    nullfree(expanded); expanded = NULL;
	    nullfree(cvt); cvt = NULL;
	}
    }
    nullfree(cvt); nullfree(unescaped);
    fprintf(stderr,"%s test_pathcvt\n",failcount > 0 ? "***FAIL":"***PASS");

    reclaimmacros();

    nc_finalize();
    return (failcount > 0 ? 1 : 0);
}

static const char*
kind2string(int kind)
{
    switch(kind) {
    case NCPD_NIX:
	return "Linux";
    case NCPD_MSYS:
	return "MSYS";
    case NCPD_CYGWIN:
	return "Cygwin";
    case NCPD_WIN:
	return "Windows";
    case NCPD_MINGW:
	return "MINGW";
    default: break;
    }
    return "unknown";
}

static char*
expand(const char* s)
{
    const char *p;
    char expanded[8192];
    char q[2];

    q[1] = '\0';
    expanded[0] = '\0';
    for(p=s;*p;p++) {
	char c = *p;
	if(c == '%') {
	    p++;
	    c = *p;
	    if(macros[(int)c] != NULL)
	        strlcat(expanded,macros[(int)c],sizeof(expanded));
	} else {
	    q[0] = c;
	    strlcat(expanded,q,sizeof(expanded));
	}
    }
    return strdup(expanded);
}

static void
setmacros(void)
{
    int i;
    const char* m;
    for(i=0;i<128;i++) macros[i] = NULL;
    if((m=getenv("MSYS2_PREFIX"))) {
	macros['m'] = strdup(m);    
    }
}

static void
reclaimmacros(void)
{
    int i;
    for(i=0;i<128;i++) {
	if(macros[i]) free(macros[i]);
	macros[i] = NULL;
    }
}

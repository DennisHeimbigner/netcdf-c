/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#ifdef HAVE_UNISTD_H
#include "unistd.h"
#endif
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef _MSC_VER
#include "XGetopt.h"
int opterr;
int optind;
#endif

#define SETUP
#define DEBUG

#define FILE "testmeta.nc"

#define DATA1 "/data1"
#define DATA1LEN 25

typedef enum Cmds {
    cmd_none = 0,
    cmd_create = 1,
    cmd_dim1 = 2,
    cmd_var1 = 3,
} Cmds;

/* Arguments from command line */
struct Options {
    int debug;
    Cmds cmd;
} options;

static char* url = NULL;

/* Forward */
static int testcreate(void);
static int testdim1(void);
static int testvar1(void);

struct Test {
    const char* option;
    Cmds cmd;
    int (*test)(void);
} tests[] = {
{"create", cmd_create, testcreate},
{"dim1", cmd_dim1, testdim1},
{"var1", cmd_var1, testvar1},
{NULL, cmd_none, NULL}
};

#ifdef SETUP
#define NCCHECK(expr) nccheck((expr),__LINE__)
void nccheck(int stat, int line)
{
    if(stat) {
	fprintf(stderr,"%d: %s\n",line,nc_strerror(stat));
	fflush(stderr);
	exit(1);
    }
}
#endif

static void
makeurl(const char* file)
{
    char wd[4096];
    NCbytes* buf = ncbytesnew();
    ncbytescat(buf,"file://");
    (void)getcwd(wd, sizeof(wd));
    ncbytescat(buf,wd);
    ncbytescat(buf,"/");
    ncbytescat(buf,file);
    ncbytescat(buf,"#mode=zarr"); /* => use default file: format */
    url = ncbytesextract(buf);
    ncbytesfree(buf);
}

int
main(int argc, char** argv)
{
    int stat = NC_NOERR;
    int c;
    struct Test* test = NULL;

    makeurl(FILE);
#ifdef DEBUG
    fprintf(stderr,"url=|%s|\n",url);
    fflush(stderr);
#endif

    memset((void*)&options,0,sizeof(options));
    while ((c = getopt(argc, argv, "dc:")) != EOF) {
	struct Test* t;
	switch(c) {
	case 'd': 
	    options.debug = 1;	    
	    break;
	case 'c':
	    if(test != NULL) {
		fprintf(stderr,"error: multiple tests specified\n");
		stat = NC_EINVAL;
		goto done;
	    }
	    for(t=tests;t->option;t++) {
		if(strcmp(optarg,t->option)==0) {test = t;}
	    }		    
	    if(test == NULL) {
		fprintf(stderr,"unknown command: %s\n",optarg);
		stat = NC_EINVAL;
		goto done;
	    };
	    break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   stat = NC_EINVAL;
	   goto done;
	}
    }
    if(test == NULL) {
	fprintf(stderr,"no command specified\n");
	stat = NC_EINVAL;
	goto done;
    }

    /* Execute */
    test->test();

done:
    if(stat)
	nc_strerror(stat);
    return (stat ? 1 : 0);    
}

/* Create test netcdf4 file via netcdf.h API*/
static int
testcreate(void)
{
    int stat = NC_NOERR;
    int ncid;

    unlink(FILE);

    if((stat = nc_create(url, 0, &ncid)))
	goto done;

    if((stat = nc_close(ncid)))
	goto done;

done:
    return THROW(stat);
}

/* Create file and add a dimension */
static int
testdim1(void)
{
    int stat = NC_NOERR;
    int ncid, dimid;

    unlink(FILE);

    if((stat = nc_create(url, 0, &ncid)))
	goto done;

    if((stat = nc_def_dim(ncid, "dim1", (size_t)1, &dimid)))
	goto done;

    if((stat = nc_close(ncid)))
	goto done;

done:
    return THROW(stat);
}

/* Create file and add a variable */
static int
testvar1(void)
{
    int stat = NC_NOERR;
    int ncid, varid;

    unlink(FILE);

    if((stat = nc_create(url, 0, &ncid)))
	goto done;

    if((stat = nc_def_var(ncid, "var1", NC_INT, 0, NULL, &varid)))
	goto done;

    if((stat = nc_close(ncid)))
	goto done;

done:
    return THROW(stat);
}

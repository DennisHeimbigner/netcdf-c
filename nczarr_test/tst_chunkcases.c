#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <netcdf.h>
#include <nclist.h>

#include <getopt.h>

#ifdef HAVE_HDF5_H
#include <hdf5.h>
#include <H5DSpublic.h>
#endif

#define ERR(e) report(e,__LINE__)

static void
report(int err, int lineno)
{
    fprintf(stderr,"Error: %d: %s\n", lineno, nc_strerror(err));
    exit(1);
}

/* Options */

static int debug = 0;
static int wholevar = 0;
static int writing = 0;
static char* file = NULL;
static size_t dimlens[NC_MAX_VAR_DIMS];
static size_t stride[NC_MAX_VAR_DIMS];
static size_t start[NC_MAX_VAR_DIMS];
static size_t stop[NC_MAX_VAR_DIMS];
static size_t count[NC_MAX_VAR_DIMS];
static size_t chunks[NC_MAX_VAR_DIMS];
static int mode = 0; /*netcdf3*/
static int rank = 0;

static int ret = NC_NOERR;
static int ncid, varid;
static int dimids[NC_MAX_VAR_DIMS];
static int fill = -1;
static unsigned chunkprod;
static unsigned dimprod;
static int data[10000];

static NClist* capture = NULL;

static int parsevector(const char* s0, size_t* vec);
static const char* printvector(int rank, const size_t* vec);

static void
CHECKRANK(int r)
{
    if(rank == 0)
        rank = r;
    else if(r != rank) {
        fprintf(stderr,"FAIL: rank mismatch\n");
	exit(1);
    }
}

static int
writedata(void)
{
    char dname[NC_MAX_NAME];
    int i;

    if((ret = nc_create(file,mode,&ncid)))
	ERR(ret);
 
    for(i=0;i<rank;i++) {
        snprintf(dname,sizeof(dname),"d%d",i);
        if((ret = nc_def_dim(ncid,dname,dimlens[i],&dimids[i])))
	    ERR(ret);
    }
    if((ret = nc_def_var(ncid,"v",NC_INT,rank,dimids,&varid)))
	ERR(ret);
    if((ret = nc_def_var_fill(ncid,varid,0,&fill)))
	ERR(ret);
    if(mode == NC_NETCDF4) {
        if((ret = nc_def_var_chunking(ncid,varid,NC_CHUNKED,chunks)))
	    ERR(ret);
    }
    
    if((ret = nc_enddef(ncid)))
       ERR(ret);

    for(i=0;i<dimprod;i++) {
	data[i] = i;
    }
 
    fprintf(stderr,"write: dimlens=%s chunklens=%s\n",
            printvector(rank,dimlens),printvector(rank,chunks));
    if(wholevar) {
        fprintf(stderr,"write whole var\n");
        if((ret = nc_put_var(ncid,varid,data)))
	    ERR(ret);
    } else {
        fprintf(stderr,"write vars: start=%s count=%s stride=%s\n",
            printvector(rank,start),printvector(rank,count),printvector(rank,stride));
        if((ret = nc_put_vars(ncid,varid,start,count,(ptrdiff_t*)stride,data)))
	    ERR(ret);
    }

    return 0;
}

static int
readdata(void)
{
    int i;
    char dname[NC_MAX_NAME];
    
    memset(data,0,sizeof(data));

    if((ret = nc_open(file,mode,&ncid)))
	ERR(ret);
 
    for(i=0;i<rank;i++) {
        snprintf(dname,sizeof(dname),"d%d",i);
        if((ret = nc_inq_dimid(ncid,dname,&dimids[i])))
	    ERR(ret);
        if((ret = nc_inq_dimlen(ncid,dimids[i],&dimlens[i])))
	    ERR(ret);
    }
    if((ret = nc_inq_varid(ncid,"v",&varid)))
	ERR(ret);

    fprintf(stderr,"read: dimlens=%s chunklens=%s\n",
            printvector(rank,dimlens),printvector(rank,chunks));
    if(wholevar) {
        fprintf(stderr,"read whole var\n");
        if((ret = nc_get_var(ncid,varid,data)))
	    ERR(ret);
    } else {
        fprintf(stderr,"read vars: start=%s count=%s stride=%s\n",
            printvector(rank,start),printvector(rank,count),printvector(rank,stride));
        if((ret = nc_get_vars(ncid,varid,start,count,(ptrdiff_t*)stride,data)))
	    ERR(ret);
    }

    for(i=0;i<dimprod;i++) {
        printf("[%d] %d\n",i,data[i]);
    }

    return 0;
}

 
int
main(int argc, char** argv)
{
    int i,c;

    /* initialize */
    for(i=0;i<NC_MAX_VAR_DIMS;i++) {
	dimlens[i] = 4;
	stride[i] = 1;
	start[i] = 0;
	count[i] = 0;
	chunks[i] = 2;
	stop[i] = 0;
    }

    while ((c = getopt(argc, argv, "34c:d:e:f:n:p:rs:wDW")) != EOF) {
	switch(c) {
	case '3':
	    mode = 0;
	    break;
	case '4':
	    mode = NC_NETCDF4;
	    break;
	case 'c':
	    CHECKRANK(parsevector(optarg,chunks));
	    break;
	case 'd':
	    CHECKRANK(parsevector(optarg,dimlens));
	    break;
	case 'e':
	    CHECKRANK(parsevector(optarg,count));
	    break;
	case 'f':
	    CHECKRANK(parsevector(optarg,start));
	    break;
	case 'p':
	    CHECKRANK(parsevector(optarg,stop));
	    break;
	case 'r': 
	    writing = 0;
	    break;
	case 'n':
	    CHECKRANK(atoi(optarg));
	    break;
	case 's':
	    CHECKRANK(parsevector(optarg,stride));
	    break;
	case 'w': 
	    writing = 1;
	    break;
	case 'D': 
	    debug = 1;
	    break;
	case 'W': 
	    wholevar = 1;
	    break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   exit(1);
	}
    }

    /* get file argument */
    argc -= optind;
    argv += optind;

    if (argc == 0) {
	fprintf(stderr, "no input file specified\n");
	exit(1);
    }

    file = strdup(argv[0]);
    if(strchr(file,':') != NULL)
        mode = NC_NETCDF4;

    dimprod = 1;
    chunkprod = 1;
    for(i=0;i<rank;i++) {dimprod *= dimlens[i]; chunkprod *= chunks[i];}

    if(count[0] > 0 && stop[0] > 0) {
        fprintf(stderr,"cannot specify both count and stop\n");
	ERR(NC_EINVAL);
    }
    if(count[0] == 0 && stop[0] == 0) {
        for(i=0;i<rank;i++)
            count[i] = (dimlens[i]+stride[i]-1)/stride[i];
    }
    if(count[0] > 0 && stop[0] == 0) {
        for(i=0;i<rank;i++)
            stop[i] = (count[i] * stride[i]);
    }
    if(count[0] == 0 && stop[0] > 0) {
        for(i=0;i<rank;i++)
            count[i] = ((stop[i]+stride[i]-1) / stride[i]);
    }	

    if(debug) {
#ifdef USE_HDF5
        H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t)H5Eprint,stderr);
#endif
    }

    if(!writing)
        readdata();
    else
        writedata();

    if((ret = nc_close(ncid)))
       ERR(ret);

    if(file) free(file);

    nclistfreeall(capture);
    return 0;
}

static int
parsevector(const char* s0, size_t* vec)
{
    char* s = strdup(s0);
    char* p = NULL;
    int i, done;

    if(s0 == NULL || vec == NULL) abort();

    for(done=0,p=s,i=0;!done;) {
	char* q;
	q = p;
        p = strchr(q,',');
        if(p == NULL) {p = q+strlen(q); done=1;}
        *p++ = '\0';
        vec[i++] = (size_t)atol(q);
    }
    if(s) free(s);
    return i;
}

static const char*
printvector(int rank, const size_t* vec)
{
    char s[NC_MAX_VAR_DIMS*3+1];
    int i;
    char* ss = NULL;

    s[0] = '\0';
    for(i=0;i<rank;i++) {
        char e[16];
	snprintf(e,sizeof(e),"%02u",(unsigned)vec[i]);
	if(i > 0) strcat(s,",");
	strcat(s,e);
    }
    if(capture == NULL) capture = nclistnew();
    ss = strdup(s);
    nclistpush(capture,ss);
    return ss;
}

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <netcdf.h>
#include <ncpathmgr.h>
#include <nclist.h>

#include <getopt.h>

#ifdef HAVE_HDF5_H
#include <hdf5.h>
#include <H5DSpublic.h>
#endif

#define ERR(e) report(e,__LINE__)

typedef enum Op {None, Read, Write, Odom} Op;

/* Bit mask of defined options; powers of 2*/
#define HAS_DIMLENS (1<<0)
#define HAS_CHUNKS (1<<1)
#define HAS_STRIDE (1<<2)
#define HAS_START (1<<3)
#define HAS_STOP (1<<4)
#define HAS_COUNT (1<<5)
#define HAS_MAX (1<<6)

/* Options */

typedef struct Options {
    unsigned debug;
    int wholevar;
    int optimize;
    Op op;
    int mode;
    int rank;
    char file[1024];
    unsigned flags; 
    size_t dimlens[NC_MAX_VAR_DIMS];
    size_t chunks[NC_MAX_VAR_DIMS];
    size_t stride[NC_MAX_VAR_DIMS];
    size_t start[NC_MAX_VAR_DIMS];
    size_t stop[NC_MAX_VAR_DIMS];
    size_t count[NC_MAX_VAR_DIMS];
    size_t max[NC_MAX_VAR_DIMS];
} Options;

typedef struct Odometer {
  size_t rank; /*rank */
  size_t start[NC_MAX_VAR_DIMS];
  size_t stop[NC_MAX_VAR_DIMS];
  size_t stride[NC_MAX_VAR_DIMS];
  size_t max[NC_MAX_VAR_DIMS]; /* max size of ith index */
  size_t count[NC_MAX_VAR_DIMS];
  size_t index[NC_MAX_VAR_DIMS]; /* current value of the odometer*/
} Odometer;

static Options options;

static int ncid, varid;
static int dimids[NC_MAX_VAR_DIMS];
static int fill = -1;
static unsigned chunkprod;
static unsigned dimprod;
static int data[10000];

static NClist* capture = NULL;

static int parsevector(const char* s0, size_t* vec);
static const char* printvector(int rank, const size_t* vec);
static const char* filenamefor(const char* f0);

Odometer* odom_new(size_t rank, const size_t* start, const size_t* stop, const size_t* stride, const size_t* max);
void odom_free(Odometer* odom);
int odom_more(Odometer* odom);
int odom_next(Odometer* odom);
size_t* odom_indices(Odometer* odom);
size_t odom_offset(Odometer* odom);
const char* odom_print1(Odometer* odom, int isshort);
const char* odom_print(Odometer* odom);
const char* odom_printshort(Odometer* odom);

extern int nc__testurl(const char*,char**);

static void
report(int err, int lineno)
{
    fprintf(stderr,"Error: %d: %s\n", lineno, nc_strerror(err));
    exit(1);
}

static void
CHECKRANK(int r)
{
    if(options.rank == 0)
        options.rank = r;
    else if(r != options.rank) {
        fprintf(stderr,"FAIL: options.rank mismatch\n");
	exit(1);
    }
}

static int
writedata(void)
{
    int ret = NC_NOERR;
    char dname[NC_MAX_NAME];
    int i;

    if((ret = nc_create(options.file,options.mode,&ncid)))
	ERR(ret);
 
    for(i=0;i<options.rank;i++) {
        snprintf(dname,sizeof(dname),"d%d",i);
        if((ret = nc_def_dim(ncid,dname,options.dimlens[i],&dimids[i])))
	    ERR(ret);
    }
    if((ret = nc_def_var(ncid,"v",NC_INT,options.rank,dimids,&varid)))
	ERR(ret);
    if((ret = nc_def_var_fill(ncid,varid,0,&fill)))
	ERR(ret);
    if(options.mode == NC_NETCDF4) {
        if((ret = nc_def_var_chunking(ncid,varid,NC_CHUNKED,options.chunks)))
	    ERR(ret);
    }
    
    if((ret = nc_enddef(ncid)))
       ERR(ret);

    for(i=0;i<dimprod;i++) {
	data[i] = i;
    }
 
    if(options.debug >= 1)
        fprintf(stderr,"write: dimlens=%s chunklens=%s\n",
            printvector(options.rank,options.dimlens),printvector(options.rank,options.chunks));
    if(options.wholevar) {
        if(options.debug >= 1) fprintf(stderr,"write whole var\n");
        if((ret = nc_put_var(ncid,varid,data)))
	    ERR(ret);
    } else {
    if(options.debug >= 1)
        fprintf(stderr,"write vars: start=%s count=%s stride=%s\n",
            printvector(options.rank,options.start),printvector(options.rank,options.count),printvector(options.rank,options.stride));
        if((ret = nc_put_vars(ncid,varid,options.start,options.count,(ptrdiff_t*)options.stride,data)))
	    ERR(ret);
    }

    return 0;
}

static int
readdata(void)
{
    int ret = NC_NOERR;
    int i;
    char dname[NC_MAX_NAME];
    
    memset(data,0,sizeof(data));

    if((ret = nc_open(options.file,options.mode,&ncid)))
	ERR(ret);
 
    for(i=0;i<options.rank;i++) {
        snprintf(dname,sizeof(dname),"d%d",i);
        if((ret = nc_inq_dimid(ncid,dname,&dimids[i])))
	    ERR(ret);
        if((ret = nc_inq_dimlen(ncid,dimids[i],&options.dimlens[i])))
	    ERR(ret);
    }
    if((ret = nc_inq_varid(ncid,"v",&varid)))
	ERR(ret);

    if(options.debug >= 1)
        fprintf(stderr,"read: dimlens=%s chunklens=%s\n",
            printvector(options.rank,options.dimlens),printvector(options.rank,options.chunks));
    if(options.wholevar) {
	if(options.debug >= 1)
        fprintf(stderr,"read whole var\n");
        if((ret = nc_get_var(ncid,varid,data)))
	    ERR(ret);
    } else {
	if(options.debug >= 1)
            fprintf(stderr,"read vars: start=%s count=%s stride=%s\n",
                printvector(options.rank,options.start),printvector(options.rank,options.count),printvector(options.rank,options.stride));
        if((ret = nc_get_vars(ncid,varid,options.start,options.count,(ptrdiff_t*)options.stride,data)))
	    ERR(ret);
    }

    for(i=0;i<dimprod;i++) {
        printf("[%d] %d\n",i,data[i]);
    }

    return 0;
}

 
static int
genodom(void)
{
    int i,ret = NC_NOERR;
    Odometer* odom = odom_new(options.rank, options.start, options.stop, options.stride, options.max);
    if(odom == NULL) {ret = NC_ENOMEM; goto done;}
    if(options.debug > 1)
        fprintf(stderr,"genodom: odom = %s\n",odom_print(odom));
    /* Iterate the odometer */
    for(i=0;odom_more(odom);odom_next(odom),i++) {
	printf("[%02d] %s\n",i,(i==0?odom_print(odom):odom_printshort(odom)));
    }        
done:
    odom_free(odom);
    return ret;
}

int
main(int argc, char** argv)
{
    int ret = NC_NOERR;
    int i,c;
    const char* p;

    /* initialize */

    memset(&options,0,sizeof(options));

    while ((c = getopt(argc, argv, "34c:d:e:f:n:m:p:rs:wD:X:O")) != EOF) {
	switch(c) {
	case '3':
	    options.mode = 0;
	    break;
	case '4':
	    options.mode = NC_NETCDF4;
	    break;
	case 'c':
	    CHECKRANK(parsevector(optarg,options.chunks));
	    options.flags |= HAS_CHUNKS;
	    break;
	case 'd':
	    CHECKRANK(parsevector(optarg,options.dimlens));
	    options.flags |= HAS_DIMLENS;
	    break;
	case 'e':
	    CHECKRANK(parsevector(optarg,options.count));
	    options.flags |= HAS_COUNT;
	    break;
	case 'f':
	    CHECKRANK(parsevector(optarg,options.start));
	    options.flags |= HAS_START;
	    break;
	case 'p':
	    CHECKRANK(parsevector(optarg,options.stop));
	    options.flags |= HAS_STOP;
	    break;
	case 'r': 
	    options.op = Read;
	    break;
	case 'm':
	    CHECKRANK(parsevector(optarg,options.max));
	    options.flags |= HAS_MAX;
	    break;
	case 'n':
	    CHECKRANK(atoi(optarg));
	    break;
	case 's':
	    CHECKRANK(parsevector(optarg,options.stride));
	    options.flags |= HAS_STRIDE;
	    break;
	case 'w': 
	    options.op = Write;
	    break;
	case 'D': 
	    options.debug = (unsigned)atoi(optarg);
	    break;
	case 'O': 
	    options.op = Odom;
	    break;
	case 'X': 
	    for(p=optarg;*p;p++) {
	        switch (*p) {
		case 'o': options.optimize = 1; break;
		case 'w': options.wholevar = 1; break;
		default: break;
		}
	    } break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   exit(1);
	}
    }

    /* get file argument */
    argc -= optind;
    argv += optind;

    switch (options.op) {
    case Read:
    case Write:
        if (argc == 0) {fprintf(stderr, "no input file specified\n");exit(1);}
	break;
    default:
	break; /* do not need a file */
    }


    if(argc > 0) {
	char* p = NCdeescape(argv[0]);
        strcpy(options.file,filenamefor(p));
	nullfree(p);
    }

    if(options.debug) {
	char s[64];
	snprintf(s,sizeof(s),"%u",options.debug);
        setenv("NCZ_WDEBUG",s,1);
    }
    if(options.optimize) {
	unsetenv("NCZ_NOOPTIMIZE");
    } else {
        setenv("NCZ_NOOPTIMIZE","1",1);
    }

    /* Default some vectors */
    if(!(options.flags & HAS_DIMLENS)) {for(i=0;i<NC_MAX_VAR_DIMS;i++) {options.dimlens[i] = 4;}}
    if(!(options.flags & HAS_CHUNKS)) {for(i=0;i<NC_MAX_VAR_DIMS;i++) {options.chunks[i] = 2;}}
    if(!(options.flags & HAS_STRIDE)) {for(i=0;i<NC_MAX_VAR_DIMS;i++) {options.stride[i] = 1;}}

    /* Computed Defaults */
    if((options.flags & HAS_COUNT) && (options.flags & HAS_STOP)) {
        fprintf(stderr,"cannot specify both count and stop\n");
	ERR(NC_EINVAL);
    }
    if(!(options.flags & HAS_COUNT) && !(options.flags & HAS_STOP)) {
        for(i=0;i<options.rank;i++)
            options.count[i] = (options.dimlens[i]+options.stride[i]-1)/options.stride[i];
    }
    if((options.flags & HAS_COUNT) && !(options.flags & HAS_STOP)) {
        for(i=0;i<options.rank;i++)
            options.stop[i] = (options.count[i] * options.stride[i]);
    }
    if(!(options.flags & HAS_COUNT) && (options.flags & HAS_STOP)) {
        for(i=0;i<options.rank;i++)
            options.count[i] = ((options.stop[i]+options.stride[i]-1) / options.stride[i]);
    }	
    
    if(!(options.flags & HAS_MAX)) {for(i=0;i<NC_MAX_VAR_DIMS;i++) {options.max[i] = options.stop[i];}}

    dimprod = 1;
    chunkprod = 1;
    for(i=0;i<options.rank;i++) {dimprod *= options.dimlens[i]; chunkprod *= options.chunks[i];}


    if(options.debug) {
#ifdef USE_HDF5
        H5Eset_auto2(H5E_DEFAULT,(H5E_auto2_t)H5Eprint,stderr);
#endif
    }

    switch (options.op) {
    case Read:
        readdata();
        if((ret = nc_close(ncid))) ERR(ret);
	break;
    case Write:
        writedata();
        if((ret = nc_close(ncid))) ERR(ret);
	break;
    case Odom:
	genodom();
	break;
    default:
	fprintf(stderr,"Unknown operation\n");
	exit(1);
    }

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

Odometer*
odom_new(size_t rank, const size_t* start, const size_t* stop, const size_t* stride, const size_t* max)
{
     int i;
     Odometer* odom = NULL;
     if((odom = calloc(1,sizeof(Odometer))) == NULL)
	 return NULL;
     odom->rank = rank;
     for(i=0;i<rank;i++) {
	 odom->start[i] = start[i];
	 odom->stop[i] = stop[i];
 	 odom->stride[i] = stride[i];
	 odom->max[i] = (max?max[i]:stop[i]);
         odom->count[i] = (odom->stop[i]+odom->stride[i]-1)/odom->stride[i];
	 odom->index[i] = 0;
     }
     return odom;
}

void
odom_free(Odometer* odom)
{
     if(odom) free(odom);
}

int
odom_more(Odometer* odom)
{
     return (odom->index[0] < odom->stop[0]);
}

int
odom_next(Odometer* odom)
{
     size_t i;
     for(i=odom->rank-1;i>=0;i--) {
	 odom->index[i] += odom->stride[i];
	 if(odom->index[i] < odom->stop[i]) break;
	 if(i == 0) return 0; /* leave the 0th entry if it overflows */
	 odom->index[i] = odom->start[i]; /* reset this position */
     }
     return 1;
}

/* Get the value of the odometer */
size_t*
odom_indices(Odometer* odom)
{
     return odom->index;
}

size_t
odom_offset(Odometer* odom)
{
     size_t offset;
     int i;

     offset = 0;
     for(i=0;i<odom->rank;i++) {
	 offset *= odom->max[i];
	 offset += odom->index[i];
     }
     return offset;
}

const char*
odom_print1(Odometer* odom, int isshort)
{
    static char s[4096];
    static char tmp[4096];
    const char* sv;
    
    s[0] = '\0';
    strcat(s,"{");
    if(!isshort) {
        snprintf(tmp,sizeof(tmp),"rank=%u",(unsigned)odom->rank); strcat(s,tmp);    
        strcat(s," start=("); sv = printvector(odom->rank,odom->start); strcat(s,sv); strcat(s,")");
        strcat(s," stop=("); sv = printvector(odom->rank,odom->stop); strcat(s,sv); strcat(s,")");
        strcat(s," stride=("); sv = printvector(odom->rank,odom->stride); strcat(s,sv); strcat(s,")");
        strcat(s," max=("); sv = printvector(odom->rank,odom->max); strcat(s,sv); strcat(s,")");
        strcat(s," count=("); sv = printvector(odom->rank,odom->count); strcat(s,sv); strcat(s,")");
    }
    snprintf(tmp,sizeof(tmp)," offset=%u",(unsigned)odom_offset(odom)); strcat(s,tmp);
    strcat(s," indices=("); sv = printvector(odom->rank,odom->index); strcat(s,sv); strcat(s,")");
    strcat(s,"}");
    return s;
}

const char*
odom_print(Odometer* odom)
{
    return odom_print1(odom,0);
}

const char*
odom_printshort(Odometer* odom)
{
    return odom_print1(odom,1);
}

static const char* urlexts[] = {"nzf", "nz4", NULL};

static const char*
filenamefor(const char* f0)
{
    static char result[4096];
    const char** extp;
    char* p;

    strcpy(result,f0); /* default */
    if(nc__testurl(f0,NULL)) goto done;
    /* Not a URL */
    p = strrchr(f0,'.'); /* look at the extension, if any */
    if(p == NULL) goto done; /* No extension */
    p++;
    for(extp=urlexts;*extp;extp++) {
        if(strcmp(p,*extp)==0) break;
    }
    if(*extp == NULL) goto done; /* not found */
    /* Assemble the url */
    strcpy(result,"file://");
    strcat(result,f0); /* core path */
    strcat(result,"#mode=nczarr,");
    strcat(result,*extp);
done:
    return result;
}

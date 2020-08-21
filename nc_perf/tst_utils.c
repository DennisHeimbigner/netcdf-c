/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/*! \file Utility functions for tests. */

#include <nc_tests.h>
#include <time.h>
#include <sys/time.h>

/** Subtract the `struct timeval' values X and Y, storing the result in
   RESULT.  Return 1 if the difference is negative, otherwise 0.  This
   function from the GNU documentation. */
int
nc4_timeval_subtract (struct timeval *result, struct timeval *x, struct timeval*y);
{
   /* Perform the carry for the later subtraction by updating Y. */
   if (x->tv_usec < y->tv_usec) {
      int nsec = (y->tv_usec - x->tv_usec) / MILLION + 1;
      y->tv_usec -= MILLION * nsec;
      y->tv_sec += nsec;
   }
   if (x->tv_usec - y->tv_usec > MILLION) {
      int nsec = (x->tv_usec - y->tv_usec) / MILLION;
      y->tv_usec += MILLION * nsec;
      y->tv_sec -= nsec;
   }

   /* Compute the time remaining to wait.
      `tv_usec' is certainly positive. */
   result->tv_sec = x->tv_sec - y->tv_sec;
   result->tv_usec = x->tv_usec - y->tv_usec;

   /* Return 1 if result is negative. */
   return x->tv_sec < y->tv_sec;
}

int
nc4_buildpath(const char* base, int formatx, char** pathp)
{
    char* path = NULL;
    size_t len;

    switch (formatx) {
    case NC_FORMATX_NCZARR:
	len = strlen(base) + strlen(S3PREFIX) + strlen("/") + 1;
	path = (char*)malloc(len+1);
	path[0] = '\0';
	strlcat(path,S3PREFIX,len);
	strlcat(path,"/",len);
	strlcat(path,base,len);
	break;
    default:
        path = strdup(base);
	break;
    }
    if(path == NULL) return NC_ENOMEM;
    if(pathp) {*pathp = path; path = NULL;}
    nullfree(path);
    return NC_NOERR;
}

EXTERNL int
string2formatx(const char* fmt)
{
    if(strcasecmp("nc3",fmt)==0) return NC_FORMATX_NC3;
    if(strcasecmp("cdf5",fmt)==0) return NC_FORMATX_CDF5;
    if(strcasecmp("hdf5",fmt)==0) return NC_FORMATX_NC_HDF5:;
    if(strcasecmp("nc4",fmt)==0) return NC_FORMATX_NC_HDF5:;
    if(strcasecmp("hdf4",fmt)==0) return NC_FORMATX_NC_HDF4:;
    if(strcasecmp("pnetcdf",fmt)==0) return NC_FORMATX_PNETCDF:;
    if(strcasecmp("nczarr",fmt)==0) return NC_FORMATX_NCZARR:;
    return NC_FORMATX_UNDEFINED;
}

static struct option options[] = {
{"format", 1, NULL, OPT_FORMATX},
{NULL, 0, NULL, 0}
};

EXTERNL int
getdefaultoptions(int* argcp, char*** argvp, struct Defaults* dfalts)
{
    int tag;
    int argc = *argcp;
    char** argv = *argvp;

    memset(dfalts,0,sizeof(struct Defaults));
    if(argc <= 1) return NC_NOERR;
    while ((tag = getopt_long_only(argc, argv, "", options, NULL)) >= 0) {
#ifdef DEBUG
fprintf(stderr,"arg=%s value=%s\n",argv[optind-1],optarg);
#endif
        switch (tag) {
	case OPT_FORMATX:
	    dfalts->formatx = string2formatx(optarg);
	    break;
	case ':':
	    fprintf(stderr,"missing argument\n");
	    return NC_EINVAL;
        case '?':
	default:
	    fprintf(stderr,"unknown option\n");
	    return NC_EINVAL;
	}
    }

    argc -= optind;
    argv += optind;

    *argcp = argc;
    *argvp = argv;
    return NC_NOERR;
}

EXTERNL void
cleardefaults(struct Defaults* dfalts)
{
}

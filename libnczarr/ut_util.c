/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"

#ifdef HAVE_UNISTD_H
#include "unistd.h"
#endif

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#ifdef _WIN32
#include "XGetopt.h"
int opterr;
int optind;
#endif

#include "zprojtest.h"

NCbytes* buf = NULL;

static void
ranktest(ProjTest* test, char c, int count)
{
    if(test->rank != count) {
	fprintf(stderr,"Option '%c': rank mismatch: rank=%lu count=%d\n",
		c,(unsigned long)test->rank,count);
	exit(1);
    }
}

int
ut_proj_init(int argc, char** argv, ProjTest* test)
{
    int stat = NC_NOERR;
    int count,c;
    int optcount = 0;

    buf = ncbytesnew();

    while ((c = getopt(argc, argv, "r:d:c:s:R:")) != EOF) {
	switch(c) {
	case 'r': /* rank */
	    if(optcount > 0) {
		fprintf(stderr,"Error: -r flag must be first\n");
		exit(1);
	    }
	    test->rank = atoi(optarg);
	    if(test->rank <= 0) {stat = NC_EINVAL; goto done;}
	    break;
	case 'd': /* dimlens */
	    count = parsevector(optarg,test->dimlen);
	    ranktest(test,c,count);
	    break;
	case 'c': /* chunklens */
	    count = parsevector(optarg,test->chunklen);
	    ranktest(test,c,count);
	    break;
	case 's': /* slices */
	    count = parseslices(optarg,test->slices);
	    ranktest(test,c,count);
	    break;
	case 'R': {/* chunk range */
	    size64_t r[2];
	    count = parsevectorn(optarg,2,r);
	    if(count != 2) {stat = NC_EINVAL; goto done;}
	    test->range.start = r[0];
	    test->range.stop = r[1];
	    } break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   stat = NC_EINVAL;
	   goto done;
	}
	optcount++;
    }
    printf("%s\n",printtest(test));

done:
    return stat;
}

int
parseslices(const char* s0, NCZSlice* slices)
{
    int count,nchars,nslices;
    NCZSlice* p = NULL;
    const char* s = NULL;
    unsigned long start,stop,stride;

    /* First, compute number of slices */
    for(s=s0,nslices=0;*s;s++) {
	if(*s == '[') nslices++;
    }
    
    if(nslices > NC_MAX_VAR_DIMS) return -1; /* too many */

    /* Extract the slices */
    for(s=s0,p=slices;*s;s+=nchars,p++) {
	/* Try 3-element slice first */
	stride = 1; /* default */
	nchars = -1;
        count = sscanf(s,"[%lu:%lu]%n",&start,&stop,&nchars);
	if(nchars == -1) {
	    nchars = -1;
            count = sscanf(s,"[%lu:%lu:%lu]%n",&start,&stop,&stride,&nchars);
	    if(count != 3) return -1;
	}
        p->start = start;
        p->stop = stop;
        p->stride = stride;
    }
    return nslices;
}

int
parsevector(const char* s0, size64_t* vector)
{
    return parsevectorn(s0,NC_MAX_VAR_DIMS,vector);
}

int
parsevectorn(const char* s0, size_t len, size64_t* vector)
{
    int count,nchars,nelem;
    size64_t* p = NULL;
    const char* s = NULL;

    /* First, compute number of elements */
    for(s=s0,nelem=1;*s;s++) {
	if(*s == ',') nelem++;
    }
    
    if(nelem > len) return -1; /* too many */

    /* Extract the elements of the vector */
    for(s=s0,p=vector;*s;p++) {
	unsigned long elem;
	nchars = -1;
        count = sscanf(s,"%lu%n",&elem,&nchars);
	if(nchars == -1 || count != 1) return -1;
	s += nchars;
	if(*s == ',') s++;
	*p = (size_t)elem;
    }
    return nelem;
}

char*
printvec(int len, size64_t* vec)
{
    char* result = NULL;
    int i;
    char value[128];
    NCbytes* buf = ncbytesnew();

    ncbytescat(buf,"(");
    for(i=0;i<len;i++) {
        if(i > 0) ncbytescat(buf,",");
        snprintf(value,sizeof(value),"%lu",(unsigned long)vec[i]);	
	ncbytescat(buf,value);
    }
    ncbytescat(buf,")");
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

char*
printtest(ProjTest* test)

{
    char* results = NULL;
    char value[128];
    NCbytes* buf = ncbytesnew();

    ncbytescat(buf,"Test{");
    snprintf(value,sizeof(value),"R=%lu",(unsigned long)test->rank);
    ncbytescat(buf,value);
    ncbytescat(buf," dimlen=");
    ncbytescat(buf,printvec(test->rank, test->dimlen));
    ncbytescat(buf," chunklen=");
    ncbytescat(buf,printvec(test->rank, test->chunklen));
    ncbytescat(buf," slices=");
    ncbytescat(buf,printslices(test->rank, test->slices));
    ncbytescat(buf,"}");    
    results = ncbytesextract(buf);
    ncbytesfree(buf);
    return results;
}

char*
printslices(int rank, NCZSlice* slices)
{
    int i;
    char* result = NULL;
    NCbytes* buf = ncbytesnew();

    for(i=0;i<rank;i++) {
	char* ssl;
        ncbytescat(buf,"[");
	ssl = nczprint_slice(slices[i]);
	ncbytescat(buf,ssl);
        ncbytescat(buf,"]");
    }
    result = ncbytesextract(buf);
    ncbytesfree(buf);
    return result;
}

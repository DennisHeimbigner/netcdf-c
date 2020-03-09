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

#include "zvarstest.h"

#define OPEN "[{("
#define CLOSE "]})"

#define LPAREN '('
#define RPAREN ')'

NCbytes* buf = NULL;

static int finddim(const char* name, size_t ndims, struct DimDef* defs);

static void
ranktest(size_t rank, char c, int count)
{
    if(rank != count) {
	fprintf(stderr,"Option '%c': rank mismatch: rank=%lu count=%d\n",
		c,(unsigned long)rank,count);
	exit(1);
    }
}

int
parseslices(const char* s0, NCZSlice** slicesp)
{
    int count,nchars,nslices;
    NCZSlice* slices = NULL;
    NCZSlice* p = NULL;
    const char* s = NULL;
    unsigned long start,stop,stride;

    /* First, compute number of slices */
    for(s=s0,nslices=0;*s;s++) {
	if(*s == '[') nslices++;
    }
    
    if(nslices > NC_MAX_VAR_DIMS) return -1; /* too many */

    slices = calloc(nslices,sizeof(NCZSlice));

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
    if(slicesp) *slicesp = slices;
    return nslices;
}

int
parsedimdefs(const char* s0, struct DimDef** defsp)
{
    int nchars,ndefs;
    const char* s = NULL;
    struct DimDef* defs = NULL;    
    int i;

    /* First, compute number of dimdefs */
    for(s=s0,ndefs=1;*s;s++) {
	if(*s == ',') ndefs++;
    }
    if((defs = calloc(ndefs,sizeof(struct DimDef)))==NULL)
	return -1;

    /* Extract */
    for(s=s0,i=0;*s;i++) {
	struct DimDef* dd = &defs[i];
	unsigned l;
	ptrdiff_t count;
	const char* p;
	if((p = strchr(s,'=')) == NULL) abort();
	if((count = (p - s)) == 0) abort();
	dd->name = malloc(count+1);
	memcpy(dd->name,s,count);
	dd->name[count] = '\0';
	s = p+1;
        sscanf(s,"%u%n",&l,&nchars);
	if(nchars == -1) abort();
	dd->len = (size_t)l;
	s += nchars;
	if(*s != ',' && *s != '\0') abort();
	if(*s == ',') s++;
    }
    if(defsp) *defsp = defs;
    return ndefs;
}

int
parsevardefs(const char* s0, struct VarDef** varsp, size_t ndims, struct DimDef* ddefs)
{
    int count,ndefs;
    const char* s = NULL;
    struct VarDef* vdefs = NULL;    
    int i;

    /* First, compute number of vardefs */
    for(s=s0,ndefs=1;*s;s++) {
	if(*s == ';') ndefs++;
    }
    if((vdefs = calloc(ndefs,sizeof(struct VarDef)))==NULL)
	return -1;

    /* Extract */
    for(s=s0,i=0;*s;i++) {
	struct VarDef* vd = &vdefs[i];
	const char* p;
	ptrdiff_t len;
	char name[NC_MAX_NAME];
	/* Scan for the end of name */
        p = strchr(s,LPAREN);	
	if(p == NULL) return -1;
	len = (p - s);	
	if(len == 0) return -1;
        memcpy(name,s,len);
	name[len] = '\0';
	vd->name = strdup(name);     
	/* parse a vector of dimnames and chunksizes and convert */
        s = p;
        if(*s == LPAREN) {
	    char** names = NULL;
	    char* p;
	    s++;
	    count = parsestringvector(s,RPAREN,&names);
	    if(count >= NC_MAX_VAR_DIMS) return -1;
	    vd->rank = count;
	    if(vd->rank > 0) {
		int j;
		vd->dimids = calloc(vd->rank,sizeof(int));
		vd->chunksizes = calloc(vd->rank,sizeof(size_t));
		for(j=0;j<vd->rank;j++) {
		    int dimpos = -1;
		    /* Split on / to get chunksize */
		    p = strchr(names[j],'/');
		    if(p) *p++ = '\0';		    
		    if((dimpos = finddim(names[j],ndims,ddefs)) == -1)
			abort();
		    vd->dimids[j] = dimpos; /* Temporarily */
		    if(p == NULL)
			vd->chunksizes[j] = ddefs[dimpos].len;
		    else {
			unsigned long l;
			sscanf(p,"%lu",&l);
			vd->chunksizes[j] = (size_t)l;
		    }
		}
	    }
	    /* Skip past the trailing ')' */
	    if((p = strchr(s,RPAREN)) == NULL) abort();
	    p++;
	    /* And next common, if any */
	    if(*p == ',') p++;
	    s = p;
	}
    }
    if(varsp) *varsp = vdefs;
    return ndefs;
}

int
parsestringvector(const char* s0, int stopchar, char*** namesp)
{
    int nelems,i;
    const char* s;
    char** names = NULL;

    /* First, compute number of elements */
    for(s=s0,nelems=1;*s;s++) {
	if(*s == ',') nelems++;
    }
    if(nelems == 0) return -1;
    names = calloc(nelems,sizeof(char*));
    for(s=s0,i=0;i<nelems;i++) {
	ptrdiff_t len;
	const char* p = strchr(s,',');
	if(p == NULL) p = strchr(s,stopchar);
	if(p == NULL) p = s + strlen(s);
	if(names[i] == NULL) {
	    char* q;
	    len = (p - s);
	    q = malloc(1+len);
	    memcpy(q,s,len);
	    q[len] = '\0';
	    names[i] = q;
	}
	s = p+1;
    }
    if(namesp) *namesp = names;
    return nelems;
}

int
parseintvector(const char* s0, int typelen, void** vectorp)
{
    int count,nchars,nelems,index;
    const char* s = NULL;
    void* vector = NULL;

    /* First, compute number of elements */
    for(s=s0,nelems=1;*s;s++) {
	if(*s == ',') nelems++;
    }

    vector = calloc(nelems,typelen);

    /* Extract the elements of the vector */
    /* Skip any leading bracketchar */
    s=s0;
    if(strchr(OPEN,*s0) != NULL) s++;
    for(index=0;*s;index++) {
	long long elem;
	nchars = -1;
        count = sscanf(s,"%lld%n",&elem,&nchars);
	if(nchars == -1 || count != 1) return -1;
	s += nchars;
	if(*s == ',') s++;
	switch (typelen) {
	case 1: ((char*)vector)[index] = (char)elem; break;
	case 2: ((short*)vector)[index] = (short)elem; break;
	case 4: ((int*)vector)[index] = (int)elem; break;
	case 8: ((long long*)vector)[index] = (long long)elem; break;
	default: abort();
	}
    }
    assert(nelems == index);
    if(vectorp) *vectorp = vector;
    return nelems;
}

#if 0
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

#endif

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
    ncbytescat(buf,nczprint_vector(test->rank, test->dimlen));
    ncbytescat(buf," chunklen=");
    ncbytescat(buf,nczprint_vector(test->rank, test->chunklen));
    ncbytescat(buf," slices=");
    ncbytescat(buf,nczprint_slices(test->rank, test->slices));
    ncbytescat(buf,"}");    
    results = ncbytesextract(buf);
    ncbytesfree(buf);
    return results;
}

/**************************************************/
int
ut_proj_init(int argc, char** argv, ProjTest* test)
{
    int stat = NC_NOERR;
    int count,c;
    int optcount = 0;

    ut_init(argc,argv,NULL);

    buf = ncbytesnew();

    while ((c = getopt(argc, argv, "r:d:c:s:R:t:")) != EOF) {
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
	    count = parseintvector(optarg,4,(void**)&test->dimlen);
	    ranktest(test->rank,c,count);
	    break;
	case 'c': /* chunklens */
	    count = parseintvector(optarg,4,(void**)&test->chunklen);
	    ranktest(test->rank,c,count);
	    break;
	case 's': /* slices */
	    count = parseslices(optarg,&test->slices);
	    ranktest(test->rank,c,count);
	    break;
	case 'R': {/* chunk range */
	    size64_t* r;
	    count = parseintvector(optarg,4,(void**)&r);
	    if(count != 2) {stat = NC_EINVAL; goto done;}
	    test->range.start = r[0];
	    test->range.stop = r[1];
	    } break;
	case 't': /* typesize */
	    test->typesize = (unsigned)atoi(optarg);
	    break;
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
ut_vars_init(int argc, char** argv, VarsTest* test)
{
    int stat = NC_NOERR;
    int count,c;
    int optcount = 0;

    ut_init(argc,argv,NULL);

    buf = ncbytesnew();

    while ((c = getopt(argc, argv, "r:n:t:f:D:V:s:W:")) != EOF) {
	switch(c) {
	case 'r': /* rank */
	    test->rank = atoi(optarg);
	    if(test->rank <= 0) {stat = NC_EINVAL; goto done;}
	    break;
	case 'n': /* ndims */
	    test->ndims = atoi(optarg);
	    if(test->ndims <= 0) {stat = NC_EINVAL; goto done;}
	    break;
        case 't':
	    test->type = atoi(optarg);
	    if(test->type <= 0 || test->type >= NC_MAX_ATOMIC_TYPE)
		{stat = NC_EINVAL; goto done;}
	    break;
	case 'D':
	    count = parsedimdefs(optarg,&test->dimdefs);
	    test->ndims = count;
	    break;
	case 'V':
	    count = parsevardefs(optarg,&test->vardefs,test->ndims,test->dimdefs);
	    test->nvars = count;
	    break;
	case 's': /* slices */
	    count = parseslices(optarg,&test->slices);
	    ranktest(test->rank,c,count);
	    break;
	case 'W': /* wdata */
	    test->datacount = parseintvector(optarg,4,(void**)&test->wdata);
	    break;
	case 'f': /* file */
	    test->file = strdup(optarg);
	    break;
	case 'v':
	    count = parsevardefs(optarg,&test->vardefs,test->ndims,test->dimdefs);
	    if(count != test->ndims) {stat = NC_EINVAL; goto done;}	    
	    break;
	case '?':
	   fprintf(stderr,"unknown option\n");
	   stat = NC_EINVAL;
	   goto done;
	}
	optcount++;
    }

done:
    return stat;
}

int
ut_typesize(nc_type t)
{
    switch (t) {
    case NC_BYTE: case NC_UBYTE: return 1;
    case NC_SHORT: case NC_USHORT: return 2;
    case NC_INT: case NC_UINT: return 4;
    case NC_INT64: case NC_UINT64: return 8;
    case NC_FLOAT: return 4;
    case NC_DOUBLE: return 8;
    default: abort();
    }
}

static int
finddim(const char* name, size_t ndims, struct DimDef* defs)
{
    int i;
    for(i=0;i<ndims;i++) {
	struct DimDef* dd = &defs[i];
	if(strcmp(dd->name,name) == 0)
	    return i;
    }    
    return -1;
}

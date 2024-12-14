/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */


#include "config.h"
#include "netcdf_filter_build.h"
#include "netcdf_json.h"

/*
Common utilities related to filters.
Taken from libdispatch/dfilters.c.
*/

#ifdef WORDS_BIGENDIAN
/* Byte swap an 8-byte integer in place */
static void
byteswap8(unsigned char* mem)
{
    unsigned char c;
    c = mem[0];
    mem[0] = mem[7];
    mem[7] = c;
    c = mem[1];
    mem[1] = mem[6];
    mem[6] = c;
    c = mem[2];
    mem[2] = mem[5];
    mem[5] = c;
    c = mem[3];
    mem[3] = mem[4];
    mem[4] = c;
}

/* Byte swap an 8-byte integer in place */
static void
byteswap4(unsigned char* mem)
{
    unsigned char c;
    c = mem[0];
    mem[0] = mem[3];
    mem[3] = c;
    c = mem[1];
    mem[1] = mem[2];
    mem[2] = c;
}
#endif /*WORDS_BIGENDIAN*/

void
NC_h5filterspec_fix8(void* mem0, int decode)
{
    NC_UNUSED(mem0);
    NC_UNUSED(decode);    
#ifdef WORDS_BIGENDIAN
    unsigned char* mem = mem0;
    if(decode) { /* Apply inverse of the encode case */
	byteswap4(mem); /* step 1: byte-swap each piece */
	byteswap4(mem+4);
	byteswap8(mem); /* step 2: convert to little endian format */
    } else { /* encode */
	byteswap8(mem); /* step 1: convert to little endian format */
	byteswap4(mem); /* step 2: byte-swap each piece */
	byteswap4(mem+4);
    }
#else /* Little endian */
    /* No action is necessary */
#endif
}

/**************************************************/

#define RAWTAG "rawformat"

/**
Encode/decode the storage of the raw HDF5 unsigned parameters.

Codec Format
{
"rawformat": true,
"nparams": "n",
"0": "<unsigned int>",
"1": "<unsigned int>",
"2": "<unsigned int>",
...
"n": "<unsigned int>",
}
*/

/* Convert from HDF5 unsigned to a JSON dict.
@param nparams
@params params
@return the JSON dict or NULL if failed
*/
int
NCZraw_encode(unsigned nparams, const unsigned* params, NCjson** jparamsp)
{
    int stat = 0;
    unsigned i;
    NCjson* jparams = NULL;
    char digits[64]; /* decimal or hex */

    NCJnew(NCJ_DICT,&jparams);
    NCJcheck(NCJappendstring(jparams,NCJ_STRING,RAWTAG));
    NCJcheck(NCJappendstring(jparams,NCJ_BOOLEAN,"true"));
    snprintf(digits,sizeof(digits),"0x%x",nparams);
    NCJcheck(NCJappendstring(jparams,NCJ_STRING,"nparams"));
    NCJcheck(NCJappendstring(jparams,NCJ_INT,digits));
    for(i=0;i<nparams;i++) {
	snprintf(digits,sizeof(digits),"%u",i);
        NCJcheck(NCJappendstring(jparams,NCJ_STRING,digits));
	snprintf(digits,sizeof(digits),"0x%x",params[i]);
	NCJcheck(NCJappendstring(jparams,NCJ_INT,digits));
    }
done:
    if(stat) {NCJreclaim(jparams); jparams = NULL;}
    if(jparamsp) {*jparamsp = jparams; jparams = NULL;}
    return stat;
}

/* Convert JSON formatted string to HDF5 nparams + params.
@param text to parse
@param nparamsp return number of params
@params paramsp return params
@return 0 if success, -1 if fail.
*/
int
NCZraw_decode(const NCjson* jcodec, unsigned* nparamsp, unsigned** paramsp)
{
    int stat = 0;
    unsigned i;
    NCjson* jparams = NULL;
    const NCjson* jvalue = NULL;
    unsigned nparams = 0;
    unsigned* params = NULL;
    char digits[64];

    NCJcheck(NCJdictget(jparams,RAWTAG,(NCjson**)&jvalue));
    if(jvalue == NULL) {stat = -1; goto done;}
    NCJcheck(NCJdictget(jparams,"nparams",(NCjson**)&jvalue));
    if(NCJsort(jvalue) != NCJ_INT) {stat = -1; goto done;}
    if(jvalue == NULL) {
        nparams = 0;
    } else {
	if(1 != sscanf(NCJstring(jvalue),"%u",&nparams)) {stat = -1; goto done;}
	/* Simple verification */
	if(nparams != (NCJdictlength(jparams) - (2 + 2))) {stat = -1; goto done;}
    }
    if(nparams > 0) {
	if((params = (unsigned*)malloc(sizeof(unsigned)*nparams))) {stat = -1; goto done;}
    }
    for(i=0;i<nparams;i++) {
	snprintf(digits,sizeof(digits),"%u",i);
        NCJcheck(NCJdictget(jparams,digits,(NCjson**)&jvalue));
        if(jvalue == NULL) {stat = -1; goto done;} /* nparams mismatch */
        if(NCJsort(jvalue) != NCJ_INT) {stat = -1; goto done;}
	sscanf(NCJstring(jvalue),"0x%x",&params[i]);
    }
    if(nparamsp) *nparamsp = nparams;
    if(paramsp) {*paramsp = params; params = NULL;}
done:
    if(params) free(params);
    NCJreclaim(jparams);
    return stat;
}

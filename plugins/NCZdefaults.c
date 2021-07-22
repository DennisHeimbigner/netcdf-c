/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */

/*
Author: Dennis Heimbigner
*/


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "netcdf_filter.h"
#include "netcdf_filter_build.h"
#include "netcdf_json.h"

#define H5Z_FILTER_DEFLATE	1 	/*deflation like gzip	     	*/
#define H5Z_FILTER_SHUFFLE      2       /*shuffle the data              */
#define H5Z_FILTER_FLETCHER32   3       /*fletcher32 checksum of EDC    */
#define H5Z_FILTER_SZIP         4       /*szip compression              */

/**************************************************/
/* NCZarr Filter Objects */

/* Forward */
static int NCZ_shuffle_codec_setup(int ncid, int varid, void** contextp);
static int NCZ_shuffle_codec_modify(void*, size_t* nparamsp, unsigned** paramsp);
static int NCZ_shuffle_codec_to_hdf5(void*, const char* codec, size_t* nparamsp, unsigned** paramsp);
static int NCZ_shuffle_hdf5_to_codec(void*, size_t nparams, const unsigned* params, char** codecp);
static int NCZ_fletcher32_codec_to_hdf5(void*, const char* codec, size_t* nparamsp, unsigned** paramsp);
static int NCZ_fletcher32_hdf5_to_codec(void*, size_t nparams, const unsigned* params, char** codecp);
static int NCZ_deflate_codec_to_hdf5(void*, const char* codec, size_t* nparamsp, unsigned** paramsp);
static int NCZ_deflate_hdf5_to_codec(void*, size_t nparams, const unsigned* params, char** codecp);
#if 0
static int NCZ_szip_codec_modify(void*, size_t* nparamsp, unsigned** paramsp);
static int NCZ_szip_codec_to_hdf5(void*, const char* codec, size_t* nparamsp, unsigned** paramsp);
static int NCZ_szip_hdf5_to_codec(void*, size_tnparams, const unsigned* params, char** codecp);
#endif

/**************************************************/

static NCZ_codec_t NCZ_shuffle_codec = {
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "shuffle",	        /* Standard name/id of the codec */
  H5Z_FILTER_SHUFFLE,   /* HDF5 alias for shuffle */
  NCZ_shuffle_codec_to_hdf5,
  NCZ_shuffle_hdf5_to_codec,
  NCZ_shuffle_codec_setup,
  NCZ_shuffle_codec_modify,
  NULL,			/* Cleanup */
  NULL,			/* finalize function */
};


static int
NCZ_shuffle_codec_setup(int ncid, int varid, void** contextp)
{
    int stat = NC_NOERR;
    nc_type vtype;
    size_t typesize;
    char vname[NC_MAX_NAME+1];
    
    if(contextp == NULL) {stat = NC_EFILTER; goto done;}

    *contextp = NULL;

    /* Get variable info */
    if((stat = nc_inq_var(ncid,varid,vname,&vtype,NULL,NULL,NULL))) goto done;

    /* Get the typesize */
    if((stat = nc_inq_type(ncid,vtype,NULL,&typesize))) goto done;

    *contextp = (void*)((uintptr_t)typesize);

done:
    return stat;
}

static int
NCZ_shuffle_codec_modify(void* context, size_t* nparamsp, unsigned** paramsp)
{
    int stat = NC_NOERR;
    uintptr_t typesize = (uintptr_t)context;
    unsigned* param = NULL;

    if((param = (unsigned*)malloc(sizeof(unsigned)))==NULL)
        {stat = NC_ENOMEM; goto done;}

    param[0] = (unsigned)typesize;

    /* add the typesize as a new parameter */
    nullfree(*paramsp);
    *paramsp = param; param = NULL;
    *nparamsp = 1;

done:
    nullfree(param);
    return stat;
}

static int
NCZ_shuffle_codec_to_hdf5(void* context, const char* codec_json, size_t* nparamsp, unsigned** paramsp)
{
    int stat = NC_NOERR;
    unsigned* params = NULL;
    uintptr_t typesize = (uintptr_t)context;
    
    if((params = (unsigned*)malloc(sizeof(unsigned)))== NULL)
        {stat = NC_ENOMEM; goto done;}

    /* Ignore any JSON typesize */
    params[0] = (unsigned)typesize;
    if(nparamsp) *nparamsp = 1;
    if(paramsp) {*paramsp = params; params = NULL;}
    
done:
    if(params) free(params);
    return stat;
}

static int
NCZ_shuffle_hdf5_to_codec(void* context, size_t nparams, const unsigned* params, char** codecp)
{
    int stat = NC_NOERR;
    uintptr_t typesize = 0;
    char json[1024];

    typesize = (uintptr_t)context;
    snprintf(json,sizeof(json),"{\"id\": \"%s\", \"elementsize\": \"%u\"}",NCZ_shuffle_codec.codecid,(unsigned)typesize);
    if(codecp) {
        if((*codecp = strdup(json))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
done:
    return stat;
}

/**************************************************/

static NCZ_codec_t NCZ_fletcher32_codec = {/* NCZ_codec_t  codec fields */ 
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "fletcher32",	        /* Standard name/id of the codec */
  H5Z_FILTER_FLETCHER32,   /* HDF5 alias for zlib */
  NCZ_fletcher32_codec_to_hdf5,
  NCZ_fletcher32_hdf5_to_codec,
  NULL,			/* setup function */
  NULL,			/* Modify */
  NULL,			/* Cleanup */
  NULL,			/* finalize function */
};

static int
NCZ_fletcher32_codec_to_hdf5(void* context, const char* codec_json, size_t* nparamsp, unsigned** paramsp)
{
    int stat = NC_NOERR;

    NC_UNUSED(context);
    
    if(nparamsp) *nparamsp = 0;
    if(paramsp) {*paramsp = NULL;}
    
    return stat;
}

static int
NCZ_fletcher32_hdf5_to_codec(void* context, size_t nparams, const unsigned* params, char** codecp)
{
    int stat = NC_NOERR;
    char json[1024];

    NC_UNUSED(context);

    snprintf(json,sizeof(json),"{\"id\": \"%s\"}",NCZ_fletcher32_codec.codecid);
    if(codecp) {
        if((*codecp = strdup(json))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
done:
    return stat;
}

/**************************************************/

static NCZ_codec_t NCZ_zlib_codec = {/* NCZ_codec_t  codec fields */ 
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "zlib",	        /* Standard name/id of the codec */
  H5Z_FILTER_DEFLATE,   /* HDF5 alias for zlib */
  NCZ_deflate_codec_to_hdf5,
  NCZ_deflate_hdf5_to_codec,
  NULL,			/* setup function */
  NULL,			/* Modify */
  NULL,			/* Cleanup */
  NULL,			/* finalize function */
};

static int
NCZ_deflate_codec_to_hdf5(void* context, const char* codec_json, size_t* nparamsp, unsigned** paramsp)
{
    int stat = NC_NOERR;
    NCjson* jcodec = NULL;
    NCjson* jtmp = NULL;
    unsigned* params = NULL;
    struct NCJconst jc;

    NC_UNUSED(context);
    
    if((params = (unsigned*)malloc(sizeof(unsigned)))== NULL)
        {stat = NC_ENOMEM; goto done;}

    /* parse the JSON */
    if((stat = NCJparse(codec_json,0,&jcodec))) goto done;
    if(NCJsort(jcodec) != NCJ_DICT) {stat = NC_EPLUGIN; goto done;}
    /* Verify the codec ID */
    if((stat = NCJdictget(jcodec,"id",&jtmp))) goto done;
    if(jtmp == NULL || !NCJisatomic(jtmp)) {stat = NC_EINVAL; goto done;}
    if(strcmp(NCJstring(jtmp),NCZ_zlib_codec.codecid)!=0) {stat = NC_EINVAL; goto done;}

    /* Get Level */
    if((stat = NCJdictget(jcodec,"level",&jtmp))) goto done;
    if((stat = NCJcvt(jtmp,NCJ_INT,&jc))) goto done;
    if(jc.ival < 0 || jc.ival > NC_MAX_UINT) {stat = NC_EINVAL; goto done;}
    params[0] = (unsigned)jc.ival;
    if(nparamsp) *nparamsp = 1;
    if(paramsp) {*paramsp = params; params = NULL;}
    
done:
    if(params) free(params);
    NCJreclaim(jcodec);
    return stat;
}

static int
NCZ_deflate_hdf5_to_codec(void* context, size_t nparams, const unsigned* params, char** codecp)
{
    int stat = NC_NOERR;
    unsigned level = 0;
    char json[1024];

    NC_UNUSED(context);

    if(nparams == 0 || params == NULL)
        {stat = NC_EINVAL; goto done;}

    level = params[0];
    snprintf(json,sizeof(json),"{\"id\": \"%s\", \"level\": \"%u\"}",NCZ_zlib_codec.codecid,level);
    if(codecp) {
        if((*codecp = strdup(json))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
done:
    return stat;
}

/**************************************************/

NCZ_codec_t* NCZ_default_codecs[] = {
&NCZ_shuffle_codec,
&NCZ_fletcher32_codec,
&NCZ_zlib_codec,
//&NCZ_szip_codec,
NULL
};

/* External Export API */
DLLEXPORT
const void*
NCZ_codec_info_defaults(void)
{
    return (void*)&NCZ_default_codecs;
}


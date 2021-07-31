/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */

/*
Author: Dennis Heimbigner
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#ifdef USE_SZIP
#include <szlib.h>
#include "H5Zszip.h"
#endif

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
#ifdef USE_SZIP
static int NCZ_szip_codec_setup(int ncid, int varid, void** contextp);
static int NCZ_szip_codec_modify(void*, size_t* nparamsp, unsigned** paramsp);
static int NCZ_szip_codec_to_hdf5(void*, const char* codec, size_t* nparamsp, unsigned** paramsp);
static int NCZ_szip_hdf5_to_codec(void*, size_t nparams, const unsigned* params, char** codecp);
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
    
    if(contextp == NULL)
        {stat = NC_EFILTER; goto done;}

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

    if((param = (unsigned*)calloc(1,sizeof(unsigned)))==NULL)
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
    
    if((params = (unsigned*)calloc(1,sizeof(unsigned)))== NULL)
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
    
    if((params = (unsigned*)calloc(1,sizeof(unsigned)))== NULL)
        {stat = NC_ENOMEM; goto done;}

    /* parse the JSON */
    if(NCJparse(codec_json,0,&jcodec))
        {stat = NC_EFILTER; goto done;}
    if(NCJsort(jcodec) != NCJ_DICT) {stat = NC_EPLUGIN; goto done;}
    /* Verify the codec ID */
    if(NCJdictget(jcodec,"id",&jtmp))
        {stat = NC_EFILTER; goto done;}
    if(jtmp == NULL || !NCJisatomic(jtmp)) {stat = NC_EINVAL; goto done;}
    if(strcmp(NCJstring(jtmp),NCZ_zlib_codec.codecid)!=0) {stat = NC_EINVAL; goto done;}

    /* Get Level */
    if(NCJdictget(jcodec,"level",&jtmp))
        {stat = NC_EFILTER; goto done;}
    if(NCJcvt(jtmp,NCJ_INT,&jc))
        {stat = NC_EFILTER; goto done;}
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

#ifdef USE_SZIP

static NCZ_codec_t NCZ_szip_codec = {
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "szip",	        /* Standard name/id of the codec */
  H5Z_FILTER_SZIP,   /* HDF5 alias for szip */
  NCZ_szip_codec_to_hdf5,
  NCZ_szip_hdf5_to_codec,
  NCZ_szip_codec_setup,
  NCZ_szip_codec_modify,
  NULL,			/* Cleanup */
  NULL,			/* finalize function */
};

struct SzipContext {
    size_t typesize;
    size_t precision;
    size_t npoints;
    size_t offset;
    size_t scanline;
    int order;
};

static int
NCZ_szip_codec_setup(int ncid, int varid, void** contextp)
{
    int i,stat = NC_NOERR;
    nc_type vtype;
    size_t typesize, scanline, dtype_precision, npoints;
    int ndims, storage, dtype_order;
    int dimids[NC_MAX_VAR_DIMS];
    char vname[NC_MAX_NAME+1];
    size_t chunklens[NC_MAX_VAR_DIMS];
    
    if(contextp == NULL) {stat = NC_EFILTER; goto done;}

    *contextp = NULL;

    /* Get variable info */
    if((stat = nc_inq_var(ncid,varid,vname,&vtype,&ndims,dimids,NULL))) goto done;

    /* Get the typesize */
    if((stat = nc_inq_type(ncid,vtype,NULL,&typesize))) goto done;

    /* Get datatype's precision, in case is less than full bits  */
    dtype_precision = typesize;

    if(dtype_precision > 24) {
        if(dtype_precision <= 32)
            dtype_precision = 32;
        else if(dtype_precision <= 64)
            dtype_precision = 64;
    } /* end if */

    if(ndims == 0) {stat = NC_EINVAL; goto done;}
    /* Set "local" parameter for this dataset's "pixels-per-scanline" */
    if((stat = nc_inq_dimlen(ncid,dimids[ndims-1],&scanline))) goto done;

    /* Get number of elements for the dataspace;  use
       total number of elements in the chunk to define the new 'scanline' size */
    /* Compute chunksize */
    if((stat = nc_inq_var_chunking(ncid,varid,&storage,chunklens))) goto done;
    if(storage != NC_CHUNKED) {stat = NC_EFILTER; goto done;}
    npoints = 1;
    for(i=0;i<ndims;i++) npoints *= chunklens[i];

    /* Get datatype's endianness order */
    if((stat = nc_inq_var_endian(ncid,varid,&dtype_order))) goto done;

    if(contextp) {
        struct SzipContext* context = (struct SzipContext*)calloc(1,sizeof(struct SzipContext));
	if(context == NULL) {stat = NC_ENOMEM; goto done;}
	context->typesize = typesize;
	context->precision = typesize;
	context->offset = 0;
	context->order = dtype_order;
	context->scanline = scanline;
	context->npoints = npoints;
        *contextp = context; context = NULL;
    }
    
done:
    FUNC_LEAVE_NOAPI(stat)
}

static int
NCZ_szip_codec_modify(void* context, size_t* nparamsp, unsigned** paramsp)
{
    int ret_value = NC_NOERR;
    unsigned* params0 = *paramsp;
    size_t nparams0 = *nparamsp;
    struct SzipContext* ctx = (struct SzipContext*)context;
    unsigned* params = NULL;
    
    if(nparams0 > 4) nparams0 = 4;
    if((params = (unsigned*)calloc(4,sizeof(unsigned)))==NULL)
        {ret_value = NC_ENOMEM; goto done;}
    memcpy(params,params0,nparams0*sizeof(unsigned));    

    /* Set "local" parameter for this dataset's "bits-per-pixel" */
    params[H5Z_SZIP_PARM_BPP] = ctx->precision;

    /* Adjust scanline if it is smaller than number of pixels per block or
       if it is bigger than maximum pixels per scanline, or there are more than
       SZ_MAX_BLOCKS_PER_SCANLINE blocks per scanline  */
    if(ctx->scanline < params0[H5Z_SZIP_PARM_PPB]) {
        if(ctx->npoints < params0[H5Z_SZIP_PARM_PPB])
	    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pixels per block greater than total number of elements in the chunk")
	ctx->scanline = MIN((params0[H5Z_SZIP_PARM_PPB] * SZ_MAX_BLOCKS_PER_SCANLINE), ctx->npoints);
    } else {
        if(ctx->scanline <= SZ_MAX_PIXELS_PER_SCANLINE)
            ctx->scanline = MIN((params0[H5Z_SZIP_PARM_PPB] * SZ_MAX_BLOCKS_PER_SCANLINE), ctx->scanline);
        else
            ctx->scanline = params0[H5Z_SZIP_PARM_PPB] * SZ_MAX_BLOCKS_PER_SCANLINE;
    } /* end else */
    /* Assign the final value to the scanline */
    params[H5Z_SZIP_PARM_PPS] = (unsigned)ctx->scanline;

    /* Set the correct endianness flag for szip */
    /* (Note: this may not handle non-atomic datatypes well) */
    params[H5Z_SZIP_PARM_MASK] &= ~(SZ_LSB_OPTION_MASK|SZ_MSB_OPTION_MASK);
    switch(ctx->order) {
    case NC_ENDIAN_LITTLE:      /* Little-endian byte order */
        params[H5Z_SZIP_PARM_MASK] |= SZ_LSB_OPTION_MASK;
        break;
    case NC_ENDIAN_BIG:      /* Big-endian byte order */
        params[H5Z_SZIP_PARM_MASK] |= SZ_MSB_OPTION_MASK;
        break;
    default:
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype endianness order")
    } /* end switch */

    /* add the typesize as a new parameter */
    nullfree(*paramsp);
    *paramsp = params; params = NULL;
    *nparamsp = 4;

done:
    nullfree(params);
    return ret_value;
}

static int
NCZ_szip_codec_to_hdf5(void* context, const char* codec_json, size_t* nparamsp, unsigned** paramsp)
{
    int stat = NC_NOERR;
    unsigned* params = NULL;
    size_t nparams = 2;
    NCjson* json = NULL;
    NCjson* jtmp = NULL;
    struct NCJconst jc = {0,0,0,NULL};
    
    if((params = (unsigned*)calloc(nparams,sizeof(unsigned)))== NULL)
        {stat = NC_ENOMEM; goto done;}

    if(NCJparse(codec_json,0,&json))
        {stat = NC_EFILTER; goto done;}

    if(NCJdictget(json,"mask",&jtmp) || jtmp == NULL)
        {stat = NC_EFILTER; goto done;}
    if(NCJcvt(jtmp,NCJ_INT,&jc))
        {stat = NC_EFILTER;  goto done;}
    params[0] = (unsigned)jc.ival;

    jtmp = NULL;
    if(NCJdictget(json,"pixels-per-block",&jtmp) || jtmp == NULL)
        {stat = NC_EFILTER; goto done;}
    if(NCJcvt(jtmp,NCJ_INT,&jc))
        {stat = NC_EFILTER;  goto done;}
    params[1] = (unsigned)jc.ival;

    if(nparamsp) *nparamsp = nparams;
    if(paramsp) {*paramsp = params; params = NULL;}
    
done:
    NCJreclaim(json);
    if(params) free(params);
    return stat;
}

static int
NCZ_szip_hdf5_to_codec(void* context, size_t nparams, const unsigned* params, char** codecp)
{
    int stat = NC_NOERR;
    char json[2048];

    snprintf(json,sizeof(json),"{\"id\": \"%s\", \"mask\": \"%u\", \"pixels-per-block\": \"%u\"}",
    		NCZ_szip_codec.codecid,
		params[H5Z_SZIP_PARM_MASK],
		params[H5Z_SZIP_PARM_PPB]);
    if(codecp) {
        if((*codecp = strdup(json))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
done:
    return stat;
}

#endif /*USE_SZIP*/

/**************************************************/

NCZ_codec_t* NCZ_default_codecs[] = {
&NCZ_shuffle_codec,
&NCZ_fletcher32_codec,
&NCZ_zlib_codec,
#ifdef USE_SZIP
&NCZ_szip_codec,
#endif
NULL
};

/* External Export API */
DLLEXPORT
const void*
NCZ_codec_info_defaults(void)
{
    return (void*)&NCZ_default_codecs;
}


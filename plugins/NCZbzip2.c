#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <assert.h>

#include <netcdf_json.h>

#include <netcdf_filter_build.h>
#include "h5bzip2.h"

/* Provide the codec support for the HDF5 bzip library */

/* NCZarr Filter Objects */

/* Forward */
static int NCZ_bzip2_codec_to_hdf5(void* context, const char* codec, int* nparamsp, unsigned** paramsp);
static int NCZ_bzip2_hdf5_to_codec(void* context, int nparams, const unsigned* params, char** codecp);

/* Structure for NCZ_PLUGIN_CODEC */
static NCZ_codec_t NCZ_bzip2_codec = {/* NCZ_codec_t  codec fields */ 
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "bz2",	        /* Standard name/id of the codec */
  H5Z_FILTER_BZIP2,     /* HDF5 alias for bzip2 */
  NCZ_bzip2_codec_to_hdf5,
  NCZ_bzip2_hdf5_to_codec,
  NULL,			/* setup function */
  NULL,			/* reset function */
  NULL,			/* finalize function */
};

/* External Export API */
DLLEXPORT
const void*
NCZ_get_plugin_info(void)
{
    return (void*)&NCZ_bzip2_codec;
}

/* NCZarr Interface Functions */

static int
NCZ_bzip2_codec_to_hdf5(void* context, const char* codec_json, int* nparamsp, unsigned** paramsp)
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
    if(strcmp(NCJstring(jtmp),NCZ_bzip2_codec.codecid)!=0) {stat = NC_EINVAL; goto done;}

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
NCZ_bzip2_hdf5_to_codec(void* context, int nparams, const unsigned* params, char** codecp)
{
    int stat = NC_NOERR;
    unsigned level = 0;
    char json[1024];

    NC_UNUSED(context);
    
    if(nparams == 0 || params == NULL)
        {stat = NC_EINVAL; goto done;}

    level = params[0];
    snprintf(json,sizeof(json),"{\"id\": \"%s\", \"level\": \"%u\"}",NCZ_bzip2_codec.codecid,level);
    if(codecp) {
        if((*codecp = strdup(json))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
done:
    return stat;
}


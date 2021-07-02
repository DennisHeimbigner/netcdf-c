/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Robb Matzke
 *              Friday, August 27, 1999
 */

/* Converted to NCZarr support by Dennis Heimbigner 5/1/2021 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <zlib.h>

#include "netcdf_json.h"

#include "netcdf.h"
#include "netcdf_filter.h"
#include "netcdf_filter_build.h"

/* Local function prototypes */
static size_t H5Z_filter_deflate (unsigned flags, size_t cd_nelmts,
    const unsigned cd_values[], size_t nbytes, size_t *buf_size, void **buf);

const H5Z_class2_t H5Z_DEFLATE[1] = {{
    H5Z_CLASS_T_VERS,           /* NCZ_class_t version */
    H5Z_FILTER_DEFLATE,		/* Filter id number		*/
    1,                          /* encoder_present flag (set to true) */
    1,                          /* decoder_present flag (set to true) */
    "deflate",                  /* Filter name for debugging	*/
    NULL,                       /* The "can apply" callback     */
    NULL,                       /* The "set local" callback     */
    H5Z_filter_deflate,         /* The actual filter function	*/
}};

#define H5Z_DEFLATE_SIZE_ADJUST(s) (ceil(((double)(s)) * (double)1.001f) + 12)

/**************************************************/
/* HDF5 FIlter query functions */

/* External Discovery Functions */
DLLEXPORT
H5PL_type_t
H5PLget_plugin_type(void)
{
    return H5PL_TYPE_FILTER;
}

DLLEXPORT
const void*
H5PLget_plugin_info(void)
{
    return H5Z_DEFLATE;
}

/**************************************************/

/*-------------------------------------------------------------------------
 * Function:	H5Z__filter_deflate
 *
 * Purpose:	Implement an I/O filter around the 'deflate' algorithm in
 *              libz
 *
 * Return:	Success: Size of buffer filtered
 *		Failure: 0
 *
 * Programmer:	Robb Matzke
 *              Thursday, April 16, 1998
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5Z_filter_deflate(unsigned flags, size_t cd_nelmts,
		    const unsigned cd_values[], size_t nbytes,
		    size_t *buf_size, void **buf)
{
    void	*outbuf = NULL;         /* Pointer to new buffer */
    int		status;                 /* Status from zlib operation */
    size_t	ret_value = 0;          /* Return value */

    /* Sanity check */
    assert(*buf_size > 0);
    assert(buf);
    assert(*buf);

    /* Check arguments */
    if (cd_nelmts!=1 || cd_values[0]>9) {
	fprintf(stderr,"deflate: invalid deflate aggression level\n");
	goto done;
    }

    if (flags & H5Z_FLAG_REVERSE) {
	/* Input; uncompress */
	z_stream	z_strm;                 /* zlib parameters */
	size_t		nalloc = *buf_size;     /* Number of bytes for output (compressed) buffer */

        /* Allocate space for the compressed buffer */
	if (NULL==(outbuf = malloc(nalloc))) {
	    fprintf(stderr,"deflate: memory allocation failed for deflate uncompression\n");
	    goto done;
        }

        /* Set the uncompression parameters */
	memset(&z_strm, 0, sizeof(z_strm));
	z_strm.next_in = (Bytef *)*buf;
	z_strm.avail_in = (unsigned)nbytes;
	z_strm.next_out = (Bytef *)outbuf;
	z_strm.avail_out = (unsigned)nalloc;

        /* Initialize the uncompression routines */
	if (Z_OK!=inflateInit(&z_strm)) {
	    fprintf(stderr,"deflate: inflateInit() failed\n");
	    goto done;
        }

        /* Loop to uncompress the buffer */
	do {
            /* Uncompress some data */
	    status = inflate(&z_strm, Z_SYNC_FLUSH);

            /* Check if we are done uncompressing data */
	    if (Z_STREAM_END==status)
                break;	/*done*/

            /* Check for error */
	    if (Z_OK!=status) {
		(void)inflateEnd(&z_strm);
	        fprintf(stderr,"deflate: inflate() failed\n");
	        goto done;
	    }
            else {
                /* If we're not done and just ran out of buffer space, get more */
                if(0 == z_strm.avail_out) {
                    void	*new_outbuf;         /* Pointer to new output buffer */
                    /* Allocate a buffer twice as big */
                    nalloc *= 2;
                    if(NULL == (new_outbuf = realloc(outbuf, nalloc))) {
                        (void)inflateEnd(&z_strm);
	    		fprintf(stderr,"deflate: memory allocation failed for deflate uncompression\n");
			goto done;
                    } /* end if */
                    outbuf = new_outbuf;

                    /* Update pointers to buffer for next set of uncompressed data */
                    z_strm.next_out = (unsigned char*)outbuf + z_strm.total_out;
                    z_strm.avail_out = (uInt)(nalloc - z_strm.total_out);
                } /* end if */
            } /* end else */
	} while(status==Z_OK);

        /* Free the input buffer */
	if(buf && *buf) free(*buf);

        /* Set return values */
	*buf = outbuf;
	outbuf = NULL;
	*buf_size = nalloc;
	ret_value = z_strm.total_out;

        /* Finish uncompressing the stream */
	(void)inflateEnd(&z_strm);
    } /* end if */
    else {
	/*
	 * Output; compress but fail if the result would be larger than the
	 * input.  The library doesn't provide in-place compression, so we
	 * must allocate a separate buffer for the result.
	 */
	const Bytef *z_src = (const Bytef*)(*buf);
	Bytef	    *z_dst;		/*destination buffer		*/
	uLongf	     z_dst_nbytes = (uLongf)H5Z_DEFLATE_SIZE_ADJUST(nbytes);
	uLong	     z_src_nbytes = (uLong)nbytes;
        int          aggression;     /* Compression aggression setting */

        /* Set the compression aggression level */
	aggression = (int)cd_values[0];

        /* Allocate output (compressed) buffer */
	if(NULL == (outbuf = malloc(z_dst_nbytes))) {
	    fprintf(stderr,"deflate: unable to allocate deflate destination buffer\n");
	    goto done;
	}
        z_dst = (Bytef *)outbuf;

        /* Perform compression from the source to the destination buffer */
	status = compress2(z_dst, &z_dst_nbytes, z_src, z_src_nbytes, aggression);

        /* Check for various zlib errors */
	if(Z_BUF_ERROR == status) {
	    fprintf(stderr,"deflate: overflow");
	    goto done;
        }
	else if(Z_MEM_ERROR == status) {
	    fprintf(stderr,"deflate: deflate memory error\n");
	    goto done;
        }
	else if(Z_OK != status) {
	    fprintf(stderr,"deflate: other deflate error\n");
	    goto done;
        }
        /* Successfully uncompressed the buffer */
        else {
            /* Free the input buffer */
	    if(buf && *buf) free(*buf);

            /* Set return values */
	    *buf = outbuf;
	    outbuf = NULL;
	    *buf_size = nbytes;
	    ret_value = z_dst_nbytes;
	} /* end else */
    } /* end else */

done:
    if(outbuf)
        free(outbuf);
    return ret_value;
}

/**************************************************/
/* NCZarr Filter Objects */

/* Forward */
static int NCZ_deflate_codec_to_hdf5(void*, const char* codec, int* nparamsp, unsigned** paramsp);
static int NCZ_deflate_hdf5_to_codec(void*, int nparams, const unsigned* params, char** codecp);

/* Structure for NCZ_PLUGIN_CODEC */
static NCZ_codec_t NCZ_zlib_codec = {/* NCZ_codec_t  codec fields */ 
  NCZ_CODEC_CLASS_VER,	/* Struct version number */
  NCZ_CODEC_HDF5,	/* Struct sort */
  "zlib",	        /* Standard name/id of the codec */
  H5Z_FILTER_DEFLATE,   /* HDF5 alias for zlib */
  NCZ_deflate_codec_to_hdf5,
  NCZ_deflate_hdf5_to_codec,
  NULL,			/* setup function */
  NULL,			/* reset function */
  NULL,			/* finalize function */
};

/* External Export API */
DLLEXPORT
const void*
NCZ_get_plugin_info(void)
{
    return (void*)&NCZ_zlib_codec;
}

/* NCZarr Interface Functions */

static int
NCZ_deflate_codec_to_hdf5(void* context, const char* codec_json, int* nparamsp, unsigned** paramsp)
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
NCZ_deflate_hdf5_to_codec(void* context, int nparams, const unsigned* params, char** codecp)
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


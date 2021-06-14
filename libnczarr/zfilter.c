/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/hdf5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/**
 * @file @internal Internal functions for filters
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4 filters
 *
 * @author Dennis Heimbigner
 *
 * This file is very similar to libhdf5/hdf5filters.c, so changes
 * should be propagated if needed.
 *
 */

#include "config.h"
#include <stdlib.h>

#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif

#ifdef _WIN32
#include <windows.h>
#endif

#include "zincludes.h"
#include "zfilter.h"
#include "ncpathmgr.h"
#include "ncpoco.h"
#include "netcdf_filter.h"
#include "netcdf_filter_build.h"
#include "netcdf_aux.h"

#undef DEBUG

#undef TFILTERS

/* MNEMONIC */
#define FORCEUNLOAD 1

/* Hold the loaded filter plugin information */
typedef struct NCZ_plugin {
    struct HDF5API {
        const H5Z_class2_t* filter;
        NCPSharedLib* hdf5lib; /* source of the filter */
    } hdf5;
    struct CodecAPI {
	const NCZ_codec_t* codec;
	NCPSharedLib* codeclib; /* of the source codec; null if same as hdf5 */
    } codec;
    int refcount;
} NCZ_plugin;

/* The NC_VAR_INFO_T->filters field is an NClist of this struct */
/*
Each filter can have two parts: HDF5 and Codec.
There are cases:
1. HDF5 defined (via def_var_filter) and Codec defined (via NCZ_hdf5_to_codec)
2. HDF5 not defined and Codec defined (from .zarray)
Only case 1 allows a filter to be used to read/write a variable.
If nc_def_var_filter is called and no matching codec can be found,
then the call will fail.
Case 2 can occur when reading an existing Zarr file that uses codecs. However,
if a codec is used for which no HDF5 filter implementation is available, then
a variable using that codec is unreadable.
It is still desirable for a user to be able to see what filters and codecs are defined
for a variable. This is accommodated by providing two special attributes:
1, "_Filters" attribute shows the HDF5 filters defined on the variable; if a filter is missing,
   then this attribute is undefined.
2, "_Codecs" attribute shows the codecs defined on the variable.
*/
struct NCZ_Filter {
    int flags;             /**< Flags describing state of this filter. */
    struct {
        unsigned id;           /**< HDF5 id corresponding to filterid. */
        size_t nparams;        /**< nparams for arbitrary filter. */
        unsigned int* params;  /**< Params for arbitrary filter. */
    } hdf5;
    struct {
	char* id;	       /**< The NumCodecs ID */
	/* Note that we always reconstruct the actual codec from the hdf5 params */
    } codec;
    NCZ_plugin* plugin;    /**< Implementation of this filter. */
};

/* All possible HDF5 filter plugins */
static NCZ_plugin* loaded_filters[H5Z_FILTER_MAX];
static int loaded_filters_max = -1;

static int NCZ_filter_initialized = 0;

/**************************************************/
#ifdef TFILTERS
static void printfilter1(struct NCZ_Filter* nfs);
static void printfilter(struct NCZ_Filter* nfs, const char* tag, int line);
static void printfilterlist(NC_VAR_INFO_T* var, const char* tag, int line);
#define PRINTFILTER(nfs, tag) printfilter(nfs,tag,__LINE__)
#define PRINTFILTERLIST(var,tag) printfilterlist(var,tag,__LINE__)
#else
#define PRINTFILTER(nfs, tag)
#define PRINTFILTERLIST(var,tag)
#endif /*TFILTERS*/

/* Forward */
static int NCZ_load_all_plugins(void);
static int NCZ_load_plugin_dir(const char* path);
static int NCZ_load_plugin(const char* path, NCZ_plugin** plugp);
static int NCZ_unload_plugin(NCZ_plugin* plugin, int forceunload);
static int NCZ_plugin_loaded(int filterid, NCZ_plugin** pp);

static int getentries(const char* path, NClist* contents);

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf nczarr filter functions.
 *
 * This file contains functions internal to the libnczarr library.
 * None of the functions in this file are exposed in the exernal API. These
 * functions all relate to the manipulation of netcdf-4's var->filters list.
 *
 * @author Dennis Heimbigner
 */

int
NCZ_filter_freelist(NC_VAR_INFO_T* var)
{
    int i, stat=NC_NOERR;
    NClist* filters = (NClist*)var->filters;

    if(filters == NULL) goto done;
PRINTFILTERLIST(var,"free: before");
    /* Free the filter list backward */
    for(i=nclistlength(filters)-1;i>=0;i--) {
	struct NCZ_Filter* spec = nclistremove(filters,i);
	/* Reclaim the plugin (only has an effect if refcount == 1) */
        NCZ_unload_plugin(spec->plugin,!FORCEUNLOAD);
	if((stat = NCZ_filter_free(spec))) goto done;
    }
PRINTFILTERLIST(var,"free: after");
    nclistfree(filters);
    var->filters = NULL;
done:
    return stat;
}

int
NCZ_filter_free(struct NCZ_Filter* spec)
{
    if(spec == NULL) goto done;
PRINTFILTER(spec,"free");
    nullfree(spec->hdf5.params);
    nullfree(spec->codec.id);
    NCJreclaim(spec->codec.codec);
    free(spec);
done:
    return NC_NOERR;
}

int
NCZ_addfilter(NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    struct NCZ_Filter* fi = NULL;
    char* codec = NULL;
    
    if(nparams > 0 && params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if(var->filters == NULL) var->filters = (void*)nclistnew();

    if((stat=NCZ_filter_lookup(var,id,&fi))==NC_NOERR) {
	assert(fi != NULL);
        /* already exists */
    } else {
        NClist* flist = (NClist*)var->filters;
	stat = NC_NOERR;
        if((fi = calloc(1,sizeof(struct NCZ_Filter))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        fi->hdf5.id = id;
	nclistpush(flist,fi);
    }    
    fi->hdf5.nparams = nparams;
    if(fi->hdf5.params != NULL) {
	nullfree(fi->hdf5.params);
	fi->hdf5.params = NULL;
    }
    assert(fi->hdf5.params == NULL);
    if(fi->hdf5.nparams > 0) {
	if((fi->hdf5.params = (unsigned int*)malloc(sizeof(unsigned int)*fi->hdf5.nparams)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        memcpy(fi->hdf5.params,params,sizeof(unsigned int)*fi->hdf5.nparams);
    }
    if(fi->plugin == NULL) {
        /* Now see if we have the codec */
        if((stat = NCZ_plugin_loaded(id,&fi->plugin))) goto done;    
    }
    if(fi->plugin == NULL) {stat = NC_ENOFILTER; goto done;} /* no implementation */
    /* Extract the codec info */
    {
	fi->codec.id = strdup(fi->plugin->codec.codec->codecid);
	if((stat = fi->plugin->codec.codec->NCZ_hdf5_to_codec(nparams,params,&codec))) goto done;
	if((stat = NCJunparse(codec,&fi->codec.codec))) goto done;
    }
    
PRINTFILTERLIST(var,"add");
    fi = NULL; /* either way,its in the var->filters list */

done:
    nullfree(codec);
    if(fi) NCZ_filter_free(fi);    
    return THROW(stat);
}

int
NCZ_filter_remove(NC_VAR_INFO_T* var, unsigned int id)
{
    int k;
    NClist* flist = (NClist*)var->filters;

    /* Walk backwards */
    for(k=nclistlength(flist)-1;k>=0;k--) {
	struct NCZ_Filter* f = (struct NCZ_Filter*)nclistget(flist,k);
        if(f->hdf5.id == id) {
	    /* Remove from variable */
    	    nclistremove(flist,k);
#ifdef TFILTERS
PRINTFILTERLIST(var,"remove");
fprintf(stderr,"\tid=%s\n",id);
#endif
	    /* Reclaim */
	    NCZ_filter_free(f);
	    return NC_NOERR;
	}
    }
    return NC_ENOFILTER;
}

int
NCZ_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NCZ_Filter** specp)
{
    int i;
    NClist* flist = (NClist*)var->filters;
    
    if(flist == NULL) {
	if((flist = nclistnew())==NULL)
	    return NC_ENOMEM;
	var->filters = (void*)flist;
    }
    for(i=0;i<nclistlength(flist);i++) {
	NCZ_Filter* spec = nclistget(flist,i);
	assert(spec->plugin != NULL);
	if(spec->hdf5.id == id) {
	    if(specp) *specp = spec;
	    return NC_NOERR;
	}
    }
    return NC_ENOFILTER;
}

#if 0
/**
 * @internal Remove a filter from filter list for a variable
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param id filter id to remove
 *
 * @returns ::NC_NOERR No error.
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNC4 Attempting netcdf-4 operation on file that is
 * not netCDF-4/NCZARR.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_ENOTINDEFINE Not in define mode.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner
 */
int
nc_var_filter_remove(int ncid, int varid, unsigned int filterid)
{
    NC_VAR_INFO_T *var = NULL;
    int stat;

    /* Get pointer to the var. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, NULL, NULL, &var)))
        return stat;
    assert(var);

    stat = NC4_nczarr_filter_remove(var,filterid);

    return stat;
}
#endif

int
NCZ_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams,
                   const unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct NCZ_Filter* oldspec = NULL;
    int havedeflate = 0;
    int haveszip = 0;

    ZTRACE(1,"ncid=%d varid=%d id=%u nparams=%u params=%s",ncid,varid,id,(unsigned)nparams,nczprint_paramvector(nparams,params));

    if((stat = NCZ_filter_initialize())) goto done;
    
    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* If the NCZARR dataset has already been created, then it is too
     * late to set all the extra stuff. */
    if (!(h5->flags & NC_INDEF))
	{stat = THROW(NC_EINDEFINE); goto done;}
    if (!var->ndims)
	{stat = NC_EINVAL; goto done;} /* For scalars, complain */
    if (var->created)
        {stat = THROW(NC_ELATEDEF); goto done;}

    /* Lookup incoming id to see if already defined */
    switch((stat=NCZ_filter_lookup(var,id,&oldspec))) {
    case NC_NOERR: break; /* already defined */
    case NC_ENOFILTER: break; /*not defined*/
    default: goto done;
    }

    /* See if deflate &/or szip is defined */
    switch ((stat = NCZ_filter_lookup(var,H5Z_FILTER_DEFLATE,NULL))) {
    case NC_NOERR: havedeflate = 1; break;
    case NC_ENOFILTER: havedeflate = 0; break;	
    default: goto done;
    }
    switch ((stat = NCZ_filter_lookup(var,H5Z_FILTER_SZIP,NULL))) {
    case NC_NOERR: haveszip = 1; break;
    case NC_ENOFILTER: haveszip = 0; break;	
    default: goto done;
    }
    /* If incoming filter not already defined, then check for conflicts */
    if(oldspec == NULL) {
        if(id == H5Z_FILTER_DEFLATE) {
		int level;
                if(nparams != 1)
                    {stat = THROW(NC_EFILTER); goto done;}/* incorrect no. of parameters */
   	        level = (int)params[0];
                if (level < NC_MIN_DEFLATE_LEVEL || level > NC_MAX_DEFLATE_LEVEL)
                    {stat = THROW(NC_EINVAL); goto done;}
                /* If szip compression is already applied, return error. */
	        if(haveszip) {stat = THROW(NC_EINVAL); goto done;}
        }
        if(id == H5Z_FILTER_SZIP) { /* Do error checking */
                if(nparams != 2)
                    {stat = THROW(NC_EFILTER); goto done;}/* incorrect no. of parameters */
                /* Pixels per block must be an even number, < 32. */
                if (params[1] % 2 || params[1] > NC_MAX_PIXELS_PER_BLOCK)
                    {stat = THROW(NC_EINVAL); goto done;}
                /* If zlib compression is already applied, return error. */
	        if(havedeflate) {stat = THROW(NC_EINVAL); goto done;}
        }
        /* Filter => chunking */
        var->storage = NC_CHUNKED;
        /* Determine default chunksizes for this variable unless already specified */
        if(var->chunksizes && !var->chunksizes[0]) {
	        /* Should this throw error? */
                if((stat = nc4_find_default_chunksizes2(grp, var)))
	            goto done;
                /* Adjust the cache. */
                if ((stat = NCZ_adjust_var_cache(var)))
                    goto done;
        }
     }
     /* More error checking */
    if(id == H5Z_FILTER_SZIP) { /* szip X chunking error checking */
	    /* For szip, the pixels_per_block parameter must not be greater
	     * than the number of elements in a chunk of data. */
            size_t num_elem = 1;
            int d;
            for (d = 0; d < var->ndims; d++)
                if (var->dim[d]->len)
		    num_elem *= var->dim[d]->len;
            /* Pixels per block must be <= number of elements. */
            if (params[1] > num_elem)
                {stat = THROW(NC_EINVAL); goto done;}
    }
    /* addfilter can handle case where filter is already defined, and will just replace parameters */
    if((stat = NCZ_addfilter(var,id,nparams,params)))
        goto done;
    if (h5->parallel)
        {stat = THROW(NC_EINVAL); goto done;}

done:
    return ZUNTRACE(stat);
}

int
NCZ_inq_var_filter_ids(int ncid, int varid, size_t* nfiltersp, unsigned int* ids)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NClist* flist = NULL;
    size_t nfilters;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    flist = var->filters;

    nfilters = nclistlength(flist);
    if(nfilters > 0 && ids != NULL) {
	int k;
	for(k=0;k<nfilters;k++) {
	    struct NCZ_Filter* f = (struct NCZ_Filter*)nclistget(flist,k);
	    ids[k] = f->hdf5.id;
	}
    }
    if(nfiltersp) *nfiltersp = nfilters;
 
done:
    return stat;

}

int
NCZ_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparamsp, unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct NCZ_Filter* spec = NULL;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));

    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    if((stat = NCZ_filter_lookup(var,id,&spec))) goto done;
    if(nparamsp) *nparamsp = spec->hdf5.nparams;
    if(params && spec->hdf5.nparams > 0) {
	memcpy(params,spec->hdf5.params,sizeof(unsigned int)*spec->hdf5.nparams);
    }
 
done:
    return stat;

}


/**************************************************/
/* Debug functions */

#ifdef TFILTERS
static void
printfilter1(struct NCZ_Filter* nfs)
{
    int i;
    if(nfs == NULL) {
	fprintf(stderr,"{null}");
	return;
    }
    fprintf(stderr,"{%u,(%u)",nfs->filterid,(int)nfs->nparams);
    for(i=0;i<nfs->nparams;i++) {
      fprintf(stderr," %s",nfs->params[i]);
    }
    fprintf(stderr,"}");
}

static void
printfilter(struct NCZ_Filter* nfs, const char* tag, int line)
{
    fprintf(stderr,"%s: line=%d: ",tag,line);
    printfilter1(nfs);
    fprintf(stderr,"\n");
}

static void
printfilterlist(NC_VAR_INFO_T* var, const char* tag, int line)
{
    int i;
    const char* name;
    if(var == NULL) name = "null";
    else if(var->hdr.name == NULL) name = "?";
    else name = var->hdr.name;
    fprintf(stderr,"%s: line=%d: var=%s filters=",tag,line,name);
    if(var != NULL) {
        for(i=0;i<nclistlength((NClist*)var->filters);i++) {
	    struct NCZ_Filter* nfs = nclistget((NClist*)var->filters,i);
	    fprintf(stderr,"[%d]",i);
	    printfilter1(nfs);
	}
    }
    fprintf(stderr,"\n");
}
#endif /*TFILTERS*/


/**************************************************/
/* Filter application functions */

int
NCZ_filter_initialize(void)
{
    int stat = NC_NOERR;
    if(NCZ_filter_initialized) return stat;
    {
        NCZ_filter_initialized = 1;
        memset(loaded_filters,0,sizeof(loaded_filters));
        if((stat = NCZ_load_all_plugins())) goto done;
    }
done:
    return stat;
}

int
NCZ_filter_finalize(void)
{
    int stat = NC_NOERR;
    return stat;
}

static int
NCZ_plugin_loaded(int filterid, NCZ_plugin** pp)
{
    int stat = NC_NOERR;
    struct NCZ_plugin* plug = NULL;
    if(filterid <= 0 || filterid >= H5Z_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    if(filterid <= loaded_filters_max) 
        plug = loaded_filters[filterid];
    if(pp) *pp = plug;
done:
    return stat;
}

int
NCZ_applyfilterchain(NClist* chain, size_t inlen, void* indata, size_t* outlenp, void** outdatap, int encode)
{
    int i, stat = NC_NOERR;
    void* lastbuffer = NULL; /* if not null, then last allocated buffer */

    /* Make sure all the filters are loaded */
    for(i=0;i<nclistlength(chain);i++) {
	struct NCZ_Filter* f = (struct NCZ_Filter*)nclistget(chain,i);
	assert(f != NULL && f->hdf5.id > 0);
	if(f->plugin == NULL) {
	    NCZ_plugin* np = NULL;
	    /* attach to the filter code */
	    stat = NCZ_plugin_loaded(f->hdf5.id,&np);
	    if(stat) {stat = NC_ENOFILTER; goto done;}
	    f->plugin = np;
	    np->refcount++;
	}
    }

    {
	struct NCZ_Filter* f = NULL;
	const H5Z_class2_t* ff = NULL;
	size_t current_alloc = inlen;
	void* current_buf = indata;
	size_t current_used = inlen;
	size_t next_alloc = 0;
	void* next_buf = NULL;
	size_t next_used = 0;


#ifdef DEBUG
fprintf(stderr,"current: alloc=%u used=%u buf=%p\n",(unsigned)current_alloc,(unsigned)current_used,current_buf);
#endif
        /* Apply in proper order */
        if(encode) {
            for(i=0;i<nclistlength(chain);i++) {
	        f = (struct NCZ_Filter*)nclistget(chain,i);	
	        ff = f->plugin->hdf5.filter;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(0,f->hdf5.nparams,f->hdf5.params,current_used,&next_alloc,&next_buf);
#ifdef DEBUG
fprintf(stderr,"next: alloc=%u used=%u buf=%p\n",(unsigned)next_alloc,(unsigned)next_used,next_buf);
#endif
		if(next_used == 0) {stat = NC_EFILTER; lastbuffer = next_buf; goto done; }
	        current_buf = next_buf;
	        current_alloc = next_alloc;
	        current_used = next_used;
	    }
	} else {
	    /* Apply in reverse order */
            for(i=nclistlength(chain)-1;i>=0;i--) {
	        f = (struct NCZ_Filter*)nclistget(chain,i);	
	        ff = f->plugin->hdf5.filter;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(H5Z_FLAG_REVERSE,f->hdf5.nparams,f->hdf5.params,current_used,&next_alloc,&next_buf);
#ifdef DEBUG
fprintf(stderr,"next: alloc=%u used=%u buf=%p\n",(unsigned)next_alloc,(unsigned)next_used,next_buf);
#endif
		if(next_used == 0) {stat = NC_EFILTER; lastbuffer = next_buf; goto done;}
	        current_buf = next_buf;
	        current_alloc = next_alloc;
	        current_used = next_used;
	    }
	}
#ifdef DEBUG
fprintf(stderr,"current: alloc=%u used=%u buf=%p\n",(unsigned)current_alloc,(unsigned)current_used,current_buf);
#endif
	/* return results */
	if(outlenp) {*outlenp = current_alloc;} /* or should it be current_used? */
	if(outdatap) {*outdatap = current_buf;}
    }

done:
    if(lastbuffer != NULL && lastbuffer != indata) nullfree(lastbuffer); /* cleanup */
    return stat;
}

/**************************************************/
/* JSON Parse/unparse of filters */

int
NCZ_filter_jsonize(NC_VAR_INFO_T* var, NCZ_Filter* filter, NCjson** jfilterp)
{
    int i,stat = NC_NOERR;
    NCjson* jfilter = NULL;
    char value[64];
    char* codec = NULL;
    
    /* Convert the HDF5 id + parameters to the codec form */
    assert(filter->codec.id != NULL && filter->plugin != NULL);
    if((stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(filter->hdf5.nparams,filter->hdf5.params,&codec))) goto done;
    /* Parse it */
    if((stat = NCJunparse(codec,&jfilter))) goto done;    
    if(jfilterp) {*jfilterp = jfilter; jfilter = NULL;}

done:
    if(codec) free(codec);
    if(jfilter) NCJreclaim(jfilter);
    return stat;
}

int
NCZ_filter_build(const NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter** filterp)
{
    int i,stat = NC_NOERR;
    NCZ_Filter* filter = NULL;
    NCjson* jvalue = NULL;
    char* codectext = NULL;


    /* Find the plugin for this filter */




    if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}
    if((stat = NCJdictget(jfilter,"id",&jvalue))) goto done;    
    if(NCJsort(jvalue) != NCJ_STRING) {stat = NC_ENOFILTER; goto done;}
    filter->codec.id = strdup(NCJstring(jvalue));

    /* Stringify jfilter and convert to HDF5 form */
    if((stat = NCJunparse(jfilter,&codectext))) goto done;
    if((stat = 


    if((1 != sscanf(NCJstring(jvalue),"%u",&filter->id)))
        {stat = NC_EFILTER; goto done;}
    /* get the parameters */
    if((stat = NCJdictget(jfilter,"parameters",&jvalue))) goto done;
    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
    /* Figure out the max # of parameters */
    filter->nparams = NCJlength(jvalue);
    if((filter->hdf5.params = calloc(sizeof(unsigned int),filter->hdf5.nparams))==NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<filter->hdf5.nparams;i++) {
	NCjson* jparam;
	jparam = NCJith(jvalue,(size_t)i);
        /* parse it */
	sscanf(NCJstring(jparam),"%u",&filter->hdf5.params[i]);
    }
#if DEBUG > 0
    fprintf(stderr,"build filter: id=%u nparams=%u params=%s\n",filter->filterid,(unsigned)filter->nparams,nczprint_paramvector(filter->nparams,filter->params));
#endif
    if(filterp) {*filterp = filter; filter = NULL;}
done:
    NCZ_filter_free(filter);
    return stat;
}

/**************************************************/
/* Filter loading */

/*
Get entries in a path that is assumed to be a directory.
*/

#ifdef _WIN32

static int
getentries(const char* path, NClist* contents)
{
    /* Iterate over the entries in the directory */
    int ret = NC_NOERR;
    errno = 0;
    WIN32_FIND_DATA FindFileData;
    HANDLE dir = NULL;
    char* ffpath = NULL;
    char* lpath = NULL;
    size_t len;
    char* d = NULL;

    ZTRACE(10,"path=%s",path);

    /* We need to process the path to make it work with FindFirstFile */
    len = strlen(path);
    /* Need to terminate path with '/''*' */
    ffpath = (char*)malloc(len+2+1);
    memcpy(ffpath,path,len);
    if(path[len-1] != '/') {
	ffpath[len] = '/';	
	len++;
    }
    ffpath[len] = '*'; len++;
    ffpath[len] = '\0';

    /* localize it */
    if((ret = nczm_localize(ffpath,&lpath,LOCALIZE))) goto done;
    dir = FindFirstFile(lpath, &FindFileData);
    if(dir == INVALID_HANDLE_VALUE) {
	/* Distinquish not-a-directory from no-matching-file */
        switch (GetLastError()) {
	case ERROR_FILE_NOT_FOUND: /* No matching files */ /* fall thru */
	    ret = NC_NOERR;
	    goto done;
	case ERROR_DIRECTORY: /* not a directory */
	default:
            ret = NC_EEMPTY;
	    goto done;
	}
    }
    do {
	char* p = NULL;
	const char* name = NULL;
        name = FindFileData.cFileName;
	if(strcmp(name,".")==0 || strcmp(name,"..")==0)
	    continue;
	nclistpush(contents,strdup(name));
    } while(FindNextFile(dir, &FindFileData));

done:
    if(dir) FindClose(dir);
    nullfree(lpath);
    nullfree(ffpath);
    nullfree(d);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}

#else /* !_WIN32 */

int
getentries(const char* path, NClist* contents)
{
    int ret = NC_NOERR;
    errno = 0;
    DIR* dir = NULL;

    ZTRACE(10,"path=%s",path);

    dir = NCopendir(path);
    if(dir == NULL)
        {ret = (errno); goto done;}
    for(;;) {
	const char* name = NULL;
	struct dirent* de = NULL;
	errno = 0;
        de = readdir(dir);
        if(de == NULL)
	    {ret = (errno); goto done;}
	if(strcmp(de->d_name,".")==0 || strcmp(de->d_name,"..")==0)
	    continue;
	name = de->d_name;
	nclistpush(contents,strdup(name));
    }
done:
    if(dir) NCclosedir(dir);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}
#endif /*_WIN32*/

static int
NCZ_load_all_plugins(void)
{
    int ret = NC_NOERR;
    const char* pluginroot = NULL;
    struct stat buf;
#ifdef _WIN32
    char pluginpath32[4096];
#endif
    
    /* Find the plugin directory root(s) */
    pluginroot = getenv(plugin_env);
    if(pluginroot == NULL || strlen(pluginroot) == 0) {
#ifdef _WIN32
	const char* win32_root;
	win32_root = getenv(win32_root_env);
	if(win32_root != NULL && strlen(win32_root) > 0) {
	    snprintf(pluginpath32,sizeof(pluginpath32),plugin_dir_win,win32_root);
	    pluginroot = pluginpath32;
	} else
	    pluginroot = NULL;
#else /*!_WIN32*/
	pluginroot = plugin_dir_unix;
#endif
    }
    if(pluginroot == NULL) {ret = NC_EINVAL; goto done;}

    /* Make sure the root is actually a directory */
    errno = 0;
    ret = NCstat(pluginroot, &buf);
//    ZTRACEMORE(6,"stat: local=%s ret=%d, errno=%d st_mode=%d",local,ret,errno,buf.st_mode);
    if(ret < 0) {
	ret = (errno);
    } else if(! S_ISDIR(buf.st_mode))
        ret = NC_EINVAL;
    if(ret) goto done;

    /* Try to load plugins from this directory */
    if((ret = NCZ_load_plugin_dir(pluginroot))) goto done;

    /* Expunge all plugins for which we do not have both HDF5 and codec */
    {
        int i;
	NCZ_plugin* p;
	for(i=0;i<loaded_filters_max;i++) {
	    if((p = loaded_filters[i]) != NULL) {
		if(p->hdf5.filter == NULL || p->codec.codec == NULL) {
		    /* expunge this entry */
		    (void)NCZ_unload_plugin(p,FORCEUNLOAD);
		    nullfree(p);
		}
	    }
	}
    }
    
done:
    return ret;
}

/* Load all the filters within a specified directory */
static int
NCZ_load_plugin_dir(const char* path)
{
    int i,stat = NC_NOERR;
    size_t pathlen;
    NClist* contents = nclistnew();
    char* file = NULL;

    if(path == NULL) {stat = NC_EINVAL; goto done;}
    pathlen = strlen(path);
    if(pathlen == 0) {stat = NC_EINVAL; goto done;}

    if((stat = getentries(path,contents))) goto done;
    for(i=0;i<nclistlength(contents);i++) {
        const char* name = (const char*)nclistget(contents,i);
	size_t nmlen = strlen(name);
	size_t flen = pathlen+1+nmlen+1;
	int id;
	NCZ_plugin* plugin = NULL;

	assert(nmlen > 0);
	nullfree(file); file = NULL;
	if((file = (char*)malloc(flen))==NULL) {stat = NC_ENOMEM; goto done;}
	file[0] = '\0';
	strlcat(file,path,flen);
	strlcat(file,"/",flen);
	strlcat(file,name,flen);
	/* See if can load the file */
	switch ((stat = NCZ_load_plugin(file,&plugin))) {
	case NC_NOERR: break;
	case NC_ENOTFOUND: break; /* will cause it to be ignored */
	default: goto done;
	}
	if(plugin != NULL) {
	    id = plugin->hdf5.filter->id;
	    if(loaded_filters[id] == NULL) {
	        plugin->refcount = 1;
	        loaded_filters[id] = plugin;
		if(id > loaded_filters_max) loaded_filters_max = id;
#if DEBUG > 1
		fprintf(stderr,"plugin loaded: id=%u, name=%s\n",id,(plugin.filter->name?plugin.filter->name:"unknown"));
#endif
	    } else {
#if DEBUG > 1
		fprintf(stderr,"plugin duplicate: id=%u, name=%s\n",id,(plugin.filter->name?plugin.filter->name:"unknown"));
#endif
	        NCZ_unload_plugin(plugin,FORCEUNLOAD); /* its a duplicate */
	    }
	} else
	    stat = NC_NOERR; /*ignore failure */
    }	

done:
    nullfree(file);
    nclistfreeall(contents);
    return stat;
}

static int
NCZ_load_plugin(const char* path, struct NCZ_plugin** plugp)
{
    int stat = NC_NOERR;
    NCZ_plugin* plugin = NULL;
    const H5Z_class2_t* h5class = NULL;
    const NCZ_codec_t** codecs = NULL;
    const NCZ_codec_t** list;
    const NCZ_codec_t** codec;
    NCPSharedLib* lib = NULL;
    int flags = NCP_GLOBAL;
    
    assert(path != NULL && strlen(path) > 0 && plugp != NULL);

    /* load the shared library */
    if((stat = ncpsharedlibnew(&lib))) goto done;
    if((stat = ncpload(lib,path,flags))) goto done;

    /* See what we have */
    {
	H5PL_get_plugin_type_proto gpt =  (H5PL_get_plugin_type_proto)ncpgetsymbol(lib,"H5PLget_plugin_type");
	H5PL_get_plugin_info_proto gpi =  (H5PL_get_plugin_info_proto)ncpgetsymbol(lib,"H5PLget_plugin_info");
	NCZ_get_plugin_info_proto  npi =  (NCZ_get_plugin_info_proto)ncpgetsymbol(lib,"NCZ_get_plugin_info");

	if(gpt != NULL && gpi != NULL) {
	    /* get HDF5 info */
	    H5PL_type_t h5type = gpt();
	    h5class = gpi();	    
	    /* Verify */
	    if(h5type != H5PL_TYPE_FILTER) {stat = NC_EPLUGIN; goto done;}
	    if(h5class->version != H5Z_CLASS_T_VERS) {stat = NC_EFILTER; goto done;}
	}
	
	if(npi != NULL) {
	    /* get Codec info */
	    codecs = npi();
	    /* Verify */
	    for(list=codecs;*list;list++) {
		codec = *list;
	        if(codec->version != NCZ_CODEC_CLASS_VER) {stat = NC_EPLUGIN; goto done;}
	        if(codec->sort != NCZ_CODEC_HDF5) {stat = NC_EPLUGIN; goto done;}
	    }
	}
    }

    /* Now, it may be that we previously loaded an HDF5|Codec only plugin,
       so we need to re-use it if we find the other half
    */
    if(h5class != NULL) {
	/* See if plugin with this id already exists */
	if((stat = NCZ_plugin_loaded(h5class->id,&plugin))) goto done;
	if(plugin != NULL) {
	    /* Check for duplicates */
	    if(plugin->hdf5.filter != NULL) {
		/* we have two plugins apparently implementing the same filter */
		/* we ignore the second one, but report it */
		nclog(NCLOGWARN,"Duplicate implementation of HDF5 filter id=%d; ignored", h5class->id);
	    } else
		plugin->hdf5.filter = h5class;
	    assert(plugin->codec.codec != NULL);
	    plugin->hdf5.hdf5lib = lib; lib = NULL;
	}
    } 

    /* Walk the set of codecs in this library */
    for(list=codecs;*list;list++) {
        codec = *list;
        if(codec != NULL) {
	    /* See if hdf5 plugin with this id already exists */
    	    if((stat = NCZ_plugin_loaded(codec->hdf5id,&plugin))) goto done;
	    if(plugin != NULL) {
	        /* Check for duplicates */
	        if(plugin->codec.codec != NULL) {
		    /* we have two plugins apparently implementing the same filter */
		    /* we ignore the second one, but report it */
	  	    nclog(NCLOGWARN,"Duplicate implementation of Codec id=%d; ignored", codec->hdf5id);
	        } else
		    plugin->codec.codec = codec;
	        assert(plugin->hdf5.filter != NULL);
  	        plugin->codec.codeclib = lib; lib = NULL;
	    }
	}
        if((codec != NULL || h5class != NULL) && plugin == NULL) { /* First encounter of this filter */
            if((plugin = (NCZ_plugin*)calloc(1,sizeof(NCZ_plugin)))==NULL)
                {stat = NC_ENOMEM; goto done;}
            if(h5class != NULL) {plugin->hdf5.filter = h5class; h5class = NULL;}
            if(codec != NULL) {plugin->codec.codec = codec; codec = NULL;}
            plugin->hdf5.hdf5lib = lib; lib = NULL;
	    plugin->codec.codeclib = NULL;
            if(plugp) {*plugp = plugin; plugin = NULL;}
        }
        /* else (codec == NULL && hdf5class == NULL) => ignore this library */
     }

done:
    if(lib) {
#if DEBUG > 1
	const char* errmsg = ncpgeterrmsg(lib);
	if(errmsg) fprintf(stderr,"err: %s\n", errmsg);
#endif	
        (void)ncpsharedlibfree(lib);
    }
    if(plugin)
        NCZ_unload_plugin(plugin,FORCEUNLOAD);
    return stat;
}

static int
NCZ_unload_plugin(NCZ_plugin* plugin, int forceunload)
{
    if(plugin && (plugin->refcount == 0 || forceunload)) {
	if(plugin->hdf5.hdf5lib != NULL) (void)ncpsharedlibfree(plugin->hdf5.hdf5lib);
	if(plugin->codec.codeclib != NULL) (void)ncpsharedlibfree(plugin->codec.codeclib);
	memset(plugin,0,sizeof(NCZ_plugin));
    } else
	plugin->refcount--;
    return NC_NOERR;
}


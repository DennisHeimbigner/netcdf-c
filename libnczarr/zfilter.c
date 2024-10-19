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
#include <stddef.h>
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
#include "netcdf_aux.h"
#include "netcdf_filter.h"
#include "netcdf_filter_build.h"
#include "zfilter.h"
#include "zplugins.h"

#if 0
#define DEBUG
#define DEBUGF
#define DEBUGL
#endif

/* If set, then triage potential shared libraries based on extension */
#define NAMEOPT

#define NULLIFY(x) ((x)?(x):"NULL")

#define H5Z_FILTER_RAW 0
#define H5Z_CODEC_RAW "hdf5raw"

NCZ_Codec NCZ_codec_empty = {NULL, NULL, 0};
NCZ_HDF5 NCZ_hdf5_empty = {0, {0,NULL}, {0, NULL}};

static void
ncz_codec_clear(NCZ_Codec* codec) {
    nullfree(codec->id); nullfree(codec->codec);
    *codec = NCZ_codec_empty;
}

static void
ncz_hdf5_clear(NCZ_HDF5* h) {
    nullfree(h->visible.params);
    nullfree(h->working.params);
    *h = NCZ_hdf5_empty;
}

#define FILTERINCOMPLETE(f) ((f)->flags & FLAG_INCOMPLETE?1:0)

/* WARNING: GLOBAL DATA */
/* TODO: move to common global state */

static int NCZ_filter_initialized = 0;

/**************************************************/

#ifdef ZTRACING
static const char*
NCJtrace(const NCjson* j)
{
    static char jstat[4096];
    char* js = NULL;
    jstat[0] = '\0';
    if(j) {
        (void)NCJunparse(j,0,&js);
	if(js) strlcat(jstat,js,sizeof(jstat));
	nullfree(js);
    }
    return jstat;
}

#define IEXISTS(x,p) (((x) && *(x)? (*(x))-> p : 0xffffffff))
#define SEXISTS(x,p) (((x) && *(x)? (*(x))-> p : "null"))
#endif

#if defined(DEBUGF) || defined(DEBUGL)
static const char*
printfilter(const NCZ_Filter* f)
{
    static char pfbuf[4096];

    if(f == NULL) return "NULL";
    snprintf(pfbuf,sizeof(pfbuf),"{flags=%d hdf5=%s codec=%s plugin=%p}",
		f->flags, printhdf5(f->hdf5),printcodec(f->codec),f->plugin);
    return pfbuf;
}

#endif


/* Forward */
static int ensure_working(const NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int paramnczclone(const NCZ_Params* src, NCZ_Params* dst);
static int paramclone(size_t nparams, const unsigned* src, unsigned** dstp);

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
NCZ_filter_freelists(NC_VAR_INFO_T* var)
{
    int stat=NC_NOERR;

    ZTRACE(6,"var=%s",var->hdr.name);
    NCZ_filter_freelist1((NClist*)var->filters);
    var->filters = NULL;
    return ZUNTRACE(stat);
}

int
NCZ_filter_freelist1(NClist* filters)
{
    int stat=NC_NOERR;
    size_t i;
    NCZ_Filter* spec = NULL;
    
    ZTRACE(6,"|filters|=%zu",nclistlength(filters));
    if(filters == NULL) goto done;
    /* Free the filter list elements */
    for(i=0;i<nclistlength(filters);i++) {
	spec = nclistget(filters,i);
	if((stat = NCZ_filter_free(spec))) goto done;
    }
    nclistfree(filters);
done:
    return ZUNTRACE(stat);
}

int
NCZ_filter_free(NCZ_Filter* spec)
{
    if(spec == NULL) return NC_NOERR;
    NCZ_filter_hdf5_clear(&spec->hdf5);
    NCZ_filter_codec_clear(&spec->codec);
    free(spec);
    return NC_NOERR;
}

int
NCZ_filter_hdf5_clear(NCZ_HDF5* spec)
{
    ZTRACE(6,"spec=%d",spec->id);
    if(spec == NULL) goto done;
    nullfree(spec->visible.params); spec->visible.params = NULL;
    nullfree(spec->working.params); spec->working.params = NULL;
done:
    return ZUNTRACE(NC_NOERR);
}

int
NCZ_filter_codec_clear(NCZ_Codec* spec)
{
    ZTRACE(6,"spec=%d",(spec?spec->id:"null"));
    if(spec == NULL) goto done;
    nullfree(spec->id); spec->id = NULL;
    nullfree(spec->codec); spec->codec = NULL;
done:
    return ZUNTRACE(NC_NOERR);
}

/* From NCZ_def_var_filter */
int
NCZ_addfilter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    struct NCZ_Filter* fi = NULL;
    NCZ_Plugin* plugin = NULL;

    ZTRACE(6,"file=%s var=%s id=%u nparams=%u params=%p",file->hdr.name,var->hdr.name,id,nparams,params);
    
    if(nparams > 0 && params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if(var->filters == NULL) var->filters = (void*)nclistnew();

    /* Before anything else, find the matching plugin */
    if((stat = NCZ_plugin_loaded((size_t)id,&plugin))) goto done;
    if(plugin == NULL) {
	stat = THROW(NC_ENOFILTER);
	goto done;
    }

    /* Find the NCZ_Filter */
    if((stat=NCZ_filter_lookup(var,id,&fi))) goto done;
    if(fi != NULL) {
	if(fi->plugin != plugin)
	    {stat = NC_EINTERNAL; goto done;}
    } else {
	stat = NC_NOERR;
        if((fi = calloc(1,sizeof(struct NCZ_Filter))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        fi->plugin = plugin;
	if(plugin->incomplete) {
	    fi->flags |= (FLAG_INCOMPLETE);
	}
	nclistpush((NClist*)var->filters, fi);
    }

    /* If this variable is not fixed size, mark filter as suppressed */
    if(var->type_info->varsized) {
	fi->flags |= FLAG_SUPPRESS;
	nclog(NCLOGWARN,"Filters cannot be applied to variable length data types; ignored");
    }

    if(!FILTERINCOMPLETE(fi)) {
        /* (over)write the HDF5 parameters */
        nullfree(fi->hdf5.visible.params);
	nullfree(fi->hdf5.working.params);
        /* Fill in the hdf5 */
        fi->hdf5 = NCZ_hdf5_empty; /* struct copy */
	fi->hdf5.id = id;
        /* Capture the visible parameters */
        fi->hdf5.visible.nparams = nparams;
        if(nparams > 0) {
	    if((stat = paramclone(nparams,params,&fi->hdf5.visible.params))) goto done;
	}
	fi->hdf5.working.nparams = 0;
	fi->hdf5.working.params = NULL;
	fi->flags |= FLAG_VISIBLE;
    }

    fi = NULL; /* either way,its in a filters list */

done:
    if(fi) NCZ_filter_free(fi);    
    return ZUNTRACE(stat);
}

int
NCZ_filter_remove(NC_VAR_INFO_T* var, unsigned int id)
{
    int stat = NC_NOERR;
    NClist* flist = (NClist*)var->filters;

    ZTRACE(6,"var=%s id=%u",var->hdr.name,id);
    /* Walk backwards */
    for(size_t k = nclistlength(flist); k-->0;) {
	struct NCZ_Filter* f = (struct NCZ_Filter*)nclistget(flist,k);
        if(f->hdf5.id == id) {
	    /* Remove from variable */
    	    nclistremove(flist,k);
	    /* Reclaim */
	    NCZ_filter_free(f); f = NULL;
	    goto done;
	}
    }
    stat = THROW(NC_ENOFILTER);
done:
    return ZUNTRACE(stat);
}

int
NCZ_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, NCZ_Filter** specp)
{
    size_t i;
    NClist* flist = (NClist*)var->filters;
    
    ZTRACE(6,"var=%s id=%u",var->hdr.name,id);

    if(specp) *specp = NULL;
    if(flist == NULL) {
	if((flist = nclistnew())==NULL)
	    return NC_ENOMEM;
	var->filters = (void*)flist;
    }
    for(i=0;i<nclistlength(flist);i++) {
	NCZ_Filter* spec = nclistget(flist,i);
	assert(spec != NULL);
	if((spec->hdf5.id == id) && !spec->incomplete) {
	    if(specp) *specp = spec;
	    break;
	}
    }
    return ZUNTRACEX(NC_NOERR,"spec=%d",IEXISTS(specp,hdf5.id));
}

int
NCZ_plugin_lookup(const char* codecid, NCZ_Plugin** pluginp)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_Plugin* plugin = NULL;
    char digits[64];
    const char* trueid = NULL;
    
    if(pluginp == NULL) return NC_NOERR;

    /* Find the plugin for this codecid */
    for(i=1;i<=plugins.loaded_plugins_max;i++) {
	NCZ_Plugin* p = plugins.loaded_plugins[i];
        if(p == NULL) continue;
        if(p == NULL|| p->codec.codec == NULL) continue; /* no plugin or no codec */
	if(p->codec.ishdf5raw) {
	    /* get true id */
	    snprintf(digits,sizeof(digits),"%d",p->hdf5.filter->id);
	    trueid = digits;
	} else {
	    trueid = p->codec.codec->codecid;
	}
	if(strcmp(codecid, trueid) == 0)
	    {plugin = p; break;}
    }
    if(pluginp) *pluginp = plugin;
    return stat;
}

#if 0
static int
NCZ_codec_lookup(NClist* codecs, const char* id, NCZ_Codec** codecp)
{
    int i;
    
    ZTRACE(6,"|codecs|=%u id=%u", (unsigned)nclistlength(codecs), id);
    if(codecp) *codecp = NULL;

    if(codecs == NULL) return NC_NOERR;
    for(i=0;i<nclistlength(codecs);i++) {
	NCZ_Codec* spec = nclistget(codecs,i);
	assert(spec != NULL);
	if(strcmp(spec->id,id)==0) {
	    if(codecp) *codecp = spec;
	    break;
	}
    }
    return ZUNTRACEX(NC_NOERR,"codec=%s",SEXISTS(codecp,id));
}
#endif

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
int
NCZ_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams,
                   const unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_Filter* oldspec = NULL;
    NCZ_Filter* tmp = NULL;
    int havedeflate = 0;
    int haveszip = 0;

    ZTRACE(1,"ncid=%d varid=%d id=%u nparams=%u params=%s",ncid,varid,id,(unsigned)nparams,nczprint_paramvector(nparams,params));

    if((stat = NCZ_filter_initialize())) goto done;
    
    if((stat = NC_check_id(ncid,&nc))) return stat;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
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
    if((stat=NCZ_filter_lookup(var,id,&oldspec))) goto done;

    /* See if deflate &/or szip is defined */
    if((stat = NCZ_filter_lookup(var,H5Z_FILTER_DEFLATE,&tmp))) goto done;
    havedeflate = (tmp == NULL ? 0 : 1);
    stat = NC_NOERR; /* reset */

    if((stat = NCZ_filter_lookup(var,H5Z_FILTER_SZIP,&tmp))) goto done;
    haveszip = (tmp == NULL ? 0 : 1);
    stat = NC_NOERR; /* reset */
    
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
        }
     }
     /* More error checking */
    if(id == H5Z_FILTER_SZIP) { /* szip X chunking error checking */
	    /* For szip, the pixels_per_block parameter must not be greater
	     * than the number of elements in a chunk of data. */
            size_t num_elem = 1;
            size_t d;
            for (d = 0; d < var->ndims; d++)
                if (var->dim[d]->len)
		    num_elem *= var->dim[d]->len;
            /* Pixels per block must be <= number of elements. */
            if (params[1] > num_elem)
                {stat = THROW(NC_EINVAL); goto done;}
    }
    /* addfilter can handle case where filter is already defined, and will just replace parameters */
    if((stat = NCZ_addfilter(h5,var,id,nparams,params)))
        goto done;
    if (h5->parallel)
        {stat = THROW(NC_EINVAL); goto done;}

done:
    return ZUNTRACE(stat);
}

/* From NCZ_def_var_filter */
static int
NCZ_addfilter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    int exists = 0;
    NCZ_Filter* fi = NULL;
    NCZ_HDF5 hdf5 = NCZ_hdf5_empty;

    ZTRACE(6,"file=%s var=%s id=%u nparams=%u params=%p",file->hdr.name,var->hdr.name,id,nparams,params);
    
    if(nparams > 0 && params == NULL) {stat = NC_EINVAL; goto done;}

    /* Warning if filter already exists, fi will be changed to be that filter and old fi will be reclaimed */
    /* If it already exists, then overwrite the parameters */
    if((stat=NCZ_filter_lookup(var,id,&fi))) goto done;
    if(fi != NULL) {
	exists = 1;
    } else {
        if((fi = calloc(1,sizeof(NCZ_Filter))) == NULL) {stat = NC_ENOMEM; goto done;}
    }

    hdf5.id = id;
    hdf5.visible.nparams = nparams;
    hdf5.visible.params = (unsigned*)params;
    if((stat = NCZ_insert_filter(file,(NClist*)var->filters,&hdf5,NULL,fi,exists))) goto done;

done:
    return ZUNTRACE(stat);
}

/* Use this instead of NCZ_addfilter when filter has already been constructed */
int
NCZ_insert_filter(NC_FILE_INFO_T* file, NClist* filterlist, NCZ_HDF5* hdf5, NCZ_Codec* codec, NCZ_Filter* fi, int exists)
{
    int stat = NC_NOERR;
    NCZ_Plugin* plugin = NULL;

    ZTRACE(6,"file=%s",file->hdr.name);
    
    assert(filterlist != NULL);
    assert(fi != NULL);
    assert(hdf5 != NULL);

    /* cleanup */
    if((stat=NCZ_filter_hdf5_clear(&fi->hdf5))) goto done;
    if((stat=NCZ_filter_codec_clear(&fi->codec))) goto done;

    /* Find the matching plugin, if any */
    if((stat = NCZ_plugin_loaded(hdf5->id,&plugin))) goto done;
    fi->plugin = plugin;
    if(fi->plugin == NULL || plugin->incomplete) fi->incomplete = 1;

    if((stat = NCZ_fillin_filter(file,fi,hdf5,(codec!=NULL?codec:NULL)))) goto done;
    
    /* Add to filters list  */
    if(!exists)
	nclistpush(filterlist, fi);

done:
    return ZUNTRACE(stat);
}

int
NCZ_fillin_filter(NC_FILE_INFO_T* file,
				NCZ_Filter* filter,
				NCZ_HDF5* hdf5,
				NCZ_Codec* codec)
{
    int stat = NC_NOERR;

    /* (over)write the HDF5 parameters */
    NCZ_filter_hdf5_clear(&filter->hdf5);
    NCZ_filter_codec_clear(&filter->codec);
    /* Fill in the hdf5 and codec*/
    filter->hdf5 = *hdf5; /* get non-pointer fields */
    /* Avoid taking control of params */
    if((stat = paramclone(hdf5->visible.nparams,hdf5->visible.params,&filter->hdf5.visible.params))) goto done;
    assert(hdf5->working.nparams == 0 && hdf5->working.params == NULL);
    if(codec != NULL) {
        filter->codec = *codec; /* get non-pointer fields */
        /* Avoid taking control of fields */    
        filter->codec.id = nulldup(codec->id);
        filter->codec.codec = nulldup(codec->codec);
    } else
        filter->codec = NCZ_codec_empty;
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

    ZTRACE(1,"ncid=%d varid=%d",ncid,varid);

    if((stat = NC_check_id(ncid,&nc))) goto done;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* Make sure all the filters are defined */
    if((stat = NCZ_filter_initialize())) goto done;

    flist = var->filters;

    nfilters = nclistlength(flist); /* including incomplete filters */
    if(nfilters > 0 && ids != NULL) {
	size_t k;
	for(k=0;k<nfilters;k++) {
	    NCZ_Filter* f = (NCZ_Filter*)nclistget(flist,k);
	    if(f->hdf5.id == 0) continue; /* No way to show filter without hdf5 id */
            ids[k] = (unsigned int)(f->hdf5.id);
	}
    }
    if(nfiltersp) *nfiltersp = nfilters;
 
done:
    return ZUNTRACEX(stat, "nfilters=%u", nfilters);
}

int
NCZ_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparamsp, unsigned int* params)
{
    int stat = NC_NOERR;
    NC *nc;
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_Filter* spec = NULL;

    ZTRACE(1,"ncid=%d varid=%d id=%u",ncid,varid,id);
    
    if((stat = NC_check_id(ncid,&nc))) goto done;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* Make sure all the plugins are defined */
    if((stat = NCZ_filter_initialize())) goto done;

    if((stat = NCZ_filter_lookup(var,id,&spec))) goto done;
    if(spec != NULL) {
	/* return the current visible parameters */
        if(nparamsp) *nparamsp = spec->hdf5.visible.nparams;
        if(params && spec->hdf5.visible.nparams > 0)
	    memcpy(params,spec->hdf5.visible.params,sizeof(unsigned int)*spec->hdf5.visible.nparams);
    } else {
        stat = THROW(NC_ENOFILTER);
    } 
done:
    return ZUNTRACEX(stat,"nparams=%u",(unsigned)(nparamsp?*nparamsp:0));
}

/* Test if a specific filter is available.
   @param file for which use of a filter is desired
   @param id the filter id of interest   
   @return NC_NOERR if the filter is available
   @return NC_ENOFILTER if the filter is not available
   @return NC_EBADID if ncid is invalid
   @return NC_EFILTER if ncid format does not support filters
*/
int
NCZ_inq_filter_avail(int ncid, unsigned id)
{
    int stat = NC_NOERR;
    struct NCZ_Plugin* plug = NULL;

    NC_UNUSED(ncid);
    ZTRACE(1,"ncid=%d id=%u",ncid,id);
    if((stat = NCZ_filter_initialize())) goto done;
    /* Check the available filters list */
    if((stat = NCZ_plugin_loaded((size_t)id, &plug))) goto done;
    if(plug == NULL || plug->incomplete)
        stat = THROW(NC_ENOFILTER);
done:
    return ZUNTRACE(stat);
}

#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/

/**************************************************/
/* Filter application functions */

int
NCZ_filter_initialize(void)
{
    int stat = NC_NOERR;

    ZTRACE(6,"");

    if(NCZ_filter_initialized) goto done;

    NCZ_filter_initialized = 1;

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if((stat = NCZ_load_all_plugins())) goto done;
#endif
done:
    return ZUNTRACE(stat);
}

int
NCZ_filter_finalize(void)
{
    int stat = NC_NOERR;
    if(!NCZ_filter_initialized) goto done;
    NCZ_filter_initialized = 0;

done:
    return ZUNTRACE(stat);
}

int
NCZ_applyfilterchain(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NClist* chain, size_t inlen, void* indata, size_t* outlenp, void** outdatap, int encode)
{
    size_t i;
    int stat = NC_NOERR;
    void* lastbuffer = NULL; /* if not null, then last allocated buffer */
    
    ZTRACE(6,"|chain|=%u inlen=%u indata=%p encode=%d", (unsigned)nclistlength(chain), (unsigned)inlen, indata, encode);

    /* Make sure all the filters are loaded && setup */
    for(i=0;i<nclistlength(chain);i++) {
	NCZ_Filter* f = (NCZ_Filter*)nclistget(chain,i);
	assert(f != NULL);
	if(f->incomplete || f->suppress) continue; /* ignore bad filters and vlen variable filters */
	assert(f->hdf5.id > 0 && f->plugin != NULL);
	if(!(f->flags & FLAG_WORKING)) {/* working not yet available */
	    if((stat = ensure_working(file, var,f))) goto done;
	}
    }

    {
	NCZ_Filter* f = NULL;
	const H5Z_class2_t* ff = NULL;
	size_t current_alloc = inlen;
	void* current_buf = indata;
	size_t current_used = inlen;
	size_t next_alloc = 0;
	void* next_buf = NULL;
	size_t next_used = 0;

#ifdef DEBUG
fprintf(stderr,">>> current: alloc=%u used=%u buf=%p\n",(unsigned)current_alloc,(unsigned)current_used,current_buf);
#endif
        /* Apply in proper order */
        if(encode) {
            for(i=0;i<nclistlength(chain);i++) {
	        f = (NCZ_Filter*)nclistget(chain,i);	
		if(f->incomplete || f->suppress) continue; /* this filter should not be applied */		
	        ff = f->plugin->hdf5.filter;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(0,f->hdf5.working.nparams,f->hdf5.working.params,current_used,&next_alloc,&next_buf);
#ifdef DEBUG
fprintf(stderr,">>> next: alloc=%u used=%u buf=%p\n",(unsigned)next_alloc,(unsigned)next_used,next_buf);
#endif
		if(next_used == 0) {stat = NC_EFILTER; lastbuffer = next_buf; goto done; }
		/* If the filter did not need to create a new buffer, then next == current else current was reclaimed */
	        current_buf = next_buf;
	        current_alloc = next_alloc;
	        current_used = next_used;
	    }
	} else {
	    /* Apply in reverse order */
            for(size_t k=nclistlength(chain); k-->0;) {
                f = (struct NCZ_Filter*)nclistget(chain, k);
		if(f->suppress) continue; /* this filter should not be applied */
	        ff = f->plugin->hdf5.filter;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(H5Z_FLAG_REVERSE,f->hdf5.working.nparams,f->hdf5.working.params,current_used,&next_alloc,&next_buf);
#ifdef DEBUG
fprintf(stderr,">>> next: alloc=%u used=%u buf=%p\n",(unsigned)next_alloc,(unsigned)next_used,next_buf);
#endif
		if(next_used == 0) {stat = NC_EFILTER; lastbuffer = next_buf; goto done;}
		/* If the filter did not need to create a new buffer, then next == current else current was reclaimed */
	        current_buf = next_buf;
	        current_alloc = next_alloc;
	        current_used = next_used;
	    }
	}
#ifdef DEBUG
fprintf(stderr,">>> current: alloc=%u used=%u buf=%p\n",(unsigned)current_alloc,(unsigned)current_used,current_buf);
#endif
	/* return results */
	if(outlenp) {*outlenp = current_used;} /* or should it be current_alloc? */
	if(outdatap) {*outdatap = current_buf;}
    }

done:
    if(lastbuffer != NULL && lastbuffer != indata) nullfree(lastbuffer); /* cleanup */
    return ZUNTRACEX(stat,"outlen=%u outdata=%p",(unsigned)*outlenp,*outdatap);
}

/**************************************************/
/* JSON Parse/unparse of filters */
int
NCZ_filter_jsonize(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter, NCjson** jfilterp)
{
    int stat = NC_NOERR;
    NCjson* jfilter = NULL;
    
    ZTRACE(6,"var=%s filter=%s",var->hdr.name,(filter != NULL && filter->codec.id != NULL?filter->codec.id:"null"));

    /* assumptions */
    assert(filter->flags & FLAG_WORKING);

    /* Convert the HDF5 id + parameters to the codec form */

    /* We need to ensure the the current visible parameters are defined and had the opportunity to come
       from the working parameters */
    assert((filter->flags & (FLAG_VISIBLE | FLAG_WORKING)) == (FLAG_VISIBLE | FLAG_WORKING));
#if 0
    if((stat = rebuild_visible(var,filter))) goto done;
#endif

    /* Convert the visible parameters back to codec */
    /* Clear any previous codec */
    nullfree(filter->codec.id); filter->codec.id = NULL;
    nullfree(filter->codec.codec); filter->codec.codec = NULL;
    filter->codec.id = strdup(filter->plugin->codec.codec->codecid);
    if(filter->plugin->codec.codec->NCZ_hdf5_to_codec) {
	stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(filter->hdf5.visible.nparams,filter->hdf5.visible.params,&filter->codec.codec);
#ifdef DEBUGF
	fprintf(stderr,">>> DEBUGF: NCZ_hdf5_to_codec: visible=%s codec=%s\n",printnczparams(filter->hdf5.visible),filter->codec.codec);
#endif
        if(stat) goto done;
    } else
        {stat = NC_EFILTER; goto done;}

    /* Parse the codec as the return */
    if(NCJparse(filter->codec.codec,0,&jfilter) < 0) {stat = NC_EFILTER; goto done;}
    if(jfilterp) {*jfilterp = jfilter; jfilter = NULL;}

done:
    NCJreclaim(jfilter);
    return ZUNTRACEX(stat,"codec=%s",NULLIFY(filter->codec.codec));
}

/* Build filter from parsed Zarr metadata */
int
NCZ_filter_build(const NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jfilter, int chainindex)
{
    int stat = NC_NOERR;
    NCZ_Filter* filter = NULL;
    const NCjson* jvalue = NULL;
    NCZ_Plugin* plugin = NULL;
    NCZ_Codec codec = NCZ_codec_empty;
    NCZ_HDF5 hdf5 = NCZ_hdf5_empty;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    ZTRACE(6,"file=%s var=%s jfilter=%s",file->hdr.name,var->hdr.name,NCJtrace(jfilter));

    if(var->filters == NULL) var->filters = nclistnew();

    /* Get the id of this codec filter */
    if(NCJdictget(jfilter,"id",&jvalue)<0) {stat = NC_EFILTER; goto done;}
    if(NCJsort(jvalue) != NCJ_STRING) {
	stat = THROW(NC_ENOFILTER); goto done;
    }

    /* Build the codec */
    if((codec.id = strdup(NCJstring(jvalue)))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if(NCJunparse(jfilter,0,&codec.codec)<0) {stat = NC_EFILTER; goto done;}

    /* Find the plugin for this filter */
    if((stat = NCZ_plugin_loaded_byname(NCJstring(jvalue),&plugin))) goto done;
    /* Will always have a filter; possibly unknown */ 
    if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}		
    filter->chainindex = chainindex;

    if(plugin != NULL) {
	/* Save the hdf5 id */
	hdf5.id = plugin->codec.codec->hdf5id;
	/* Convert the codec to hdf5 form visible parameters */
        if(plugin->codec.codec->NCZ_codec_to_hdf5) {
            stat = plugin->codec.codec->NCZ_codec_to_hdf5(codec.codec,&hdf5.visible.nparams,&hdf5.visible.params);
#ifdef DEBUGF
	    fprintf(stderr,">>> DEBUGF: NCZ_codec_to_hdf5: codec=%s, hdf5=%s\n",printcodec(codec),printhdf5(hdf5));
#endif
	    if(stat) goto done;
	}
	filter->flags |= FLAG_VISIBLE;
	filter->hdf5 = hdf5; hdf5 = NCZ_hdf5_empty;
	filter->codec = codec; codec = NCZ_codec_empty;
	filter->flags |= FLAG_CODEC;
        filter->plugin = plugin; plugin = NULL;
    } else {
        /* Create a fake filter so we do not forget about this codec */
	filter->hdf5 = NCZ_hdf5_empty;
	filter->codec = codec; codec = NCZ_codec_empty;
	filter->flags |= (FLAG_INCOMPLETE|FLAG_CODEC);
    }

    if(filter != NULL) {
        NClist* filterlist = (NClist*)var->filters;
        nclistpush(filterlist,filter);
        filter = NULL;
    }
    
done:
    ncz_hdf5_clear(&hdf5);
    ncz_codec_clear(&codec);
    NCZ_filter_free(filter);
    return ZUNTRACE(stat);
}

>>>>>>> minpluginx.dmh
/**************************************************/
/* _Codecs attribute */

int
NCZ_codec_attr(NC_VAR_INFO_T* var, size_t* lenp, void* data)
{
    size_t i;
    int stat = NC_NOERR;
    size_t len;
    char* contents = NULL;
    NCbytes* buf = NULL;
    NClist* filters = (NClist*)var->filters;
    size_t nfilters;
    
    ZTRACE(6,"var=%s",var->hdr.name);

    nfilters = nclistlength(filters);

    if(nfilters == 0)
        {stat = NC_ENOTATT; goto done;}

    buf = ncbytesnew(); ncbytessetalloc(buf,1024);
    ncbytescat(buf,"[");
    for(i=0;i<nfilters;i++) {
       	NCZ_Filter* spec = (NCZ_Filter*)nclistget(filters,i);
        if(i > 0) ncbytescat(buf,",");
	ncbytescat(buf,spec->codec.codec);
    }
    ncbytescat(buf,"]");

    len = ncbyteslength(buf);
    contents = nclistcontents(buf);
    if(lenp) *lenp = len;
    if(data) strncpy((char*)data,contents,len+1);
done:
    ncbytesfree(buf);
    return ZUNTRACEX(stat,"len=%u data=%p",(unsigned)len,data);
}

static int
ensure_working(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;

    if(filter->incomplete) {stat = THROW(NC_ENOFILTER); goto done;}
    if(!(filter->flags & FLAG_WORKING)) {
	const size_t oldnparams = filter->hdf5.visible.nparams;
	const unsigned* oldparams = filter->hdf5.visible.params;

	assert(filter->flags & FLAG_VISIBLE);
        /* Convert the visible parameters to working parameters; may also modify the visible params */
#ifdef DEBUGF
        fprintf(stderr,">>> DEBUGF: NCZ_modify_parameters: before: visible=%s working=%s\n",
	    	printparams(filter->hdf5.visible.nparams,filter->hdf5.visible.params),
	    	printparams(filter->hdf5.working.nparams,filter->hdf5.working.params));
#endif
        if(filter->plugin && filter->plugin->codec.codec->NCZ_modify_parameters) {
	    NCproplist* props = ncproplistnew();
	    if((stat=ncproplistclone(zfile->zarr.zarr_format==2?NCplistzarrv2:NCplistzarrv3,props))) goto done;
    	    ncproplistadd(props,"fileid",(size_t)ncidfor(var));
    	    ncproplistadd(props,"varid",(uintptr_t)var->hdr.id);
	    stat = filter->plugin->codec.codec->NCZ_modify_parameters(props,&filter->hdf5.id,
				&filter->hdf5.visible.nparams, &filter->hdf5.visible.params,
				&filter->hdf5.working.nparams, &filter->hdf5.working.params);
	    ncproplistfree(props);
#ifdef DEBUGF
	    fprintf(stderr,">>> DEBUGF: NCZ_modify_parameters: stat=%d ncid=%d varid=%d filter=%s\n",stat, (int)ncidfor(var),(int)var->hdr.id,
			printfilter(filter));
#endif
	    if(stat) goto done;
	} else {
	    /* assume visible are unchanged */
	    assert(oldnparams == filter->hdf5.visible.nparams && oldparams == filter->hdf5.visible.params); /* unchanged */
	    /* Just copy the visible parameters */
	    nullfree(filter->hdf5.working.params);
	    if((stat = paramnczclone(&filter->hdf5.visible,&filter->hdf5.working))) goto done;
	}
#ifdef DEBUGF
        fprintf(stderr,">>> DEBUGF: NCZ_modify_parameters: after: visible=%s working=%s\n",
	    	printparams(filter->hdf5.visible.nparams,filter->hdf5.visible.params),
	    	printparams(filter->hdf5.working.nparams,filter->hdf5.working.params));
#endif
	filter->flags |= FLAG_WORKING;
    }
#ifdef DEBUGF
    fprintf(stderr,">>> DEBUGF: ensure_working_parameters: ncid=%lu varid=%u filter=%s\n", ncidfor(var), (unsigned)var->hdr.id,printfilter(filter));
#endif
done:
    return THROW(stat);
}


/* Called by NCZ_enddef to ensure that the working parameters are defined */
int
NCZ_filter_setup(NC_VAR_INFO_T* var)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* filters = NULL;
    NC_FILE_INFO_T* file = var->container->nc4_info;

    ZTRACE(6,"var=%s",var->hdr.name);

    filters = (NClist*)var->filters;
    for(i=0;i<nclistlength(filters);i++) {    
	NCZ_Filter* filter = (NCZ_Filter*)nclistget(filters,i);
        assert(filter != NULL);
 	/* verify */
        if((stat=NCZ_filter_verify(filter,var->type_info->varsized))) goto done;
        if(filter->incomplete) continue; /* ignore these */
	/* Initialize the working parameters */
	if((stat = ensure_working(file,var,filter))) goto done;
#ifdef DEBUGF
	fprintf(stderr,">>> DEBUGF: NCZ_filter_setup: ncid=%d varid=%d filter=%s\n", (int)ncidfor(var),(int)var->hdr.id,
			printfilter(filter));
#endif
    }

done:
    return ZUNTRACE(stat);
}

/*
Check for filter incompleteness and suppression, etc/
*/
int
NCZ_filter_verify(NCZ_Filter* filter, int varsized)
{
    /* If this variable is not fixed size, mark filter as suppressed */
    if(varsized) {
	filter->suppress = 1;
	nclog(NCLOGWARN,"Filters cannot be applied to variable length data types; filter will be ignored");
    }
    if(filter->plugin == NULL) filter->incomplete = 1;
    else if(filter->plugin->incomplete) filter->incomplete = 1;
    if(!filter->incomplete)
        assert(filter->hdf5.id > 0 && (filter->hdf5.visible.nparams == 0 || filter->hdf5.visible.params != NULL));
    assert(filter->flags == 0);
    if(filter->hdf5.visible.nparams == 0 || filter->hdf5.visible.params != NULL) filter->flags |= FLAG_VISIBLE;
    return NC_NOERR;
}

/**************************************************/

/* Clone an hdf5 parameter set */
static int
paramclone(size_t nparams, const unsigned* src, unsigned** dstp)
{
    unsigned* dst = NULL;
    if(nparams > 0) {
	if(src == NULL) return NC_EINVAL;
	if((dst = (unsigned*)malloc(sizeof(unsigned) * nparams))==NULL)
	    return NC_ENOMEM;
	memcpy(dst,src,sizeof(unsigned) * nparams);
    }
    if(dstp) *dstp = dst;
    return NC_NOERR;
}

static int
paramnczclone(const NCZ_Params* src, NCZ_Params* dst)
{
    assert(src != NULL && dst != NULL && dst->params == NULL);
    *dst = *src;
    return paramclone(src->nparams,src->params,&dst->params);
}

/* Encode filter to parsed Zarr metadata */
int
NCZ_filters_encode(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NClist* jfilters)
{
    int stat = NC_NOERR;
    size_t i;
    NClist* filters = NULL;
    NCZ_Filter* filter = NULL;
    NCjson* jfilter = NULL;

    filters = (NClist*)var->filters;
    if(filters != NULL) {
	for(i=0;i<nclistlength(filters);i++) {
	    filter = (NCZ_Filter*)nclistget(filters,i);
	    if((stat = NCZF_encode_filter(file,var,filter,&jfilter))) goto done;
	    nclistpush(jfilters,jfilter); jfilter = NULL;
	}
    }
    
done:
#if 0
    ncz_hdf5_clear(&hdf5);
    ncz_codec_clear(&codec);
#endif /*0*/
    NCZ_reclaim_json(jfilter);
    return ZUNTRACE(stat);
}

/* Decode Zarr filter metadata to  filters and attach to var*/
int
NCZ_filters_decode(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NClist* jfilters)
{
    int stat = NC_NOERR;
    size_t i,j;
    NCZ_Filter* filter = NULL;
    NClist* filters = NULL;

    if(var->filters == NULL) var->filters = nclistnew();
    filters = (NClist*)var->filters;

    for(i=0;i<nclistlength(jfilters);i++) {
        const NCjson* jfilter = (NCjson*)nclistget(jfilters,i);
        /* Will always have a filter; possibly unknown */ 
	if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}		
        /* Decode the JSON filter */
        if((stat=NCZF_decode_filter(file,var,jfilter,filter))) goto done;
	nclistpush(filters,filter);
    }
#if 0
    memset(&codec,0,sizeof(codec));
    /* Get the id of this codec filter */
    if(NCJdictget(jfilter,"id",&jvalue)<0) {stat = NC_EFILTER; goto done;}
    if(NCJsort(jvalue) != NCJ_STRING) {
	stat = THROW(NC_ENOFILTER); goto done;
    }

    /* Build the codec */
    if((codec.id = strdup(NCJstring(jvalue)))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if(NCJunparse(jfilter,0,&codec.codec)<0) {stat = NC_EFILTER; goto done;}
#endif

    /* Find the plugin for each filter */
    for(i=0;i<=plugins.loaded_plugins_max;i++) {
        if (!plugins.loaded_plugins[i]) continue;
	if(!plugins.loaded_plugins[i] || !plugins.loaded_plugins[i]->codec.codec) continue; /* no plugin or no codec */
	/* See if this plugin matches any filter */
	for(j=0;j<nclistlength(filters);j++) {
	    filter = (NCZ_Filter*)nclistget(filters,i);
            if(strcmp(filter->codec.id, plugins.loaded_plugins[i]->codec.codec->codecid) == 0)
	        {filter->plugin = plugins.loaded_plugins[i];}
	}
	filter = NULL;
    }
#if 0
	if((stat = NCZF_codec2hdf(file,var,filter))) goto done;
	filter->flags |= FLAG_VISIBLE;
	filter->codec = codec; codec = NCZ_codec_empty;
    } else {
        /* Create a fake filter so we do not forget about this codec */
	filter->hdf5 = hdf5_empty;
	filter->codec = codec; codec = NCZ_codec_empty;
    }
#endif
    
    /* Finalize the filters */
    if((stat = NCZ_filter_setup(var))) goto done;

done:
#if 0
    ncz_hdf5_clear(&hdf5);
    ncz_codec_clear(&codec);
#endif /*0*/
    NCZ_filter_free(filter);
    return ZUNTRACE(stat);
}

#if 0
/* Encode filter to parsed Zarr metadata */
int
NCZ_filter_encode(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCZ_Filter* filter, NCjson** jfilterp)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(6,"file=%s var=%s filter=%s",file->hdr.name,var->hdr.name,filter->codec.id);
    if((stat = NCZF_encode_filter(file,var,filter,jfilterp))) goto done;
    
done:
    return ZUNTRACE(stat);
}
#endif /*0*/

#if 0
/* Create filter from parsed Zarr metadata */
int
NCZ_filter_decode(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jfilter)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_Filter* filter = NULL;
    const NCjson* jvalue = NULL;
    NCZ_Plugin* plugin = NULL;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    ZTRACE(6,"file=%s var=%s jfilter=%s",file->hdr.name,var->hdr.name,NCJtrace(jfilter));

    memset(&codec,0,sizeof(codec));

    if(var->filters == NULL) var->filters = nclistnew();

    /* Will always have a filter; possibly unknown */ 
    if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}		

    /* Decode the JSON filter */
    if((stat=NCZF_decode_filter(file,var,jfilter,filter))) goto done;

#if 0
    /* Get the id of this codec filter */
    if(NCJdictget(jfilter,"id",&jvalue)<0) {stat = NC_EFILTER; goto done;}
    if(NCJsort(jvalue) != NCJ_STRING) {
	stat = THROW(NC_ENOFILTER); goto done;
    }

    /* Build the codec */
    if((codec.id = strdup(NCJstring(jvalue)))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if(NCJunparse(jfilter,0,&codec.codec)<0) {stat = NC_EFILTER; goto done;}
#endif

    /* Find the plugin for this filter */
    for(i=0;i<=plugins.loaded_plugins_max;i++) {
        if (!plugins.loaded_plugins[i]) continue;
        if(!plugins.loaded_plugins[i] || !plugins.loaded_plugins[i]->codec.codec) continue; /* no plugin or no codec */
        if(strcmp(filter->codec.id, plugins.loaded_plugins[i]->codec.codec->codecid) == 0)
	    {plugin = plugins.loaded_plugins[i]; break;}
    }

    filter->plugin = plugin; plugin = NULL;
#if 0
	if((stat = NCZF_codec2hdf(file,var,filter))) goto done;
	filter->flags |= FLAG_VISIBLE;
	filter->codec = codec; codec = NCZ_codec_empty;
    } else {
        /* Create a fake filter so we do not forget about this codec */
	filter->hdf5 = hdf5_empty;
	filter->codec = codec; codec = NCZ_codec_empty;
    }
#endif

    if(filter != NULL) {
        NClist* filterlist = (NClist*)var->filters;
        nclistpush(filterlist,filter);
        filter = NULL;
    }
    
done:
#if 0
    ncz_hdf5_clear(&hdf5);
    ncz_codec_clear(&codec);
#endif /*0*/
    NCZ_filter_free(filter);
    return ZUNTRACE(stat);
}
#endif /*0*/

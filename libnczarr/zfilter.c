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

/* Hold the loaded filter information for all possible filter ids */
static struct NCZ_plugin {
    const NCZ_filter_class* filter;
    NCPSharedLib* library;
    int refcount;
} loaded_filters[NCZ_FILTER_MAX];

#define BLOSC_FILTERID 32001

/* Define the .rc file tag; value is <name>=<int> <name>=<int>... */
#define BLOSCRC_TAG "ZARR.FILTERS.BLOSC.COMPRESSORS"

static const struct SpecialFilter {const char* name; unsigned id; size_t nparams;} specialfilters[] = {
{"blosc",BLOSC_FILTERID,7},
{NULL,0,0}
};

/* Table of BLOSC internal filters in sorted order, can be extended */
static const char* defalt_blosc_filters = "reset,blosclz=0,lz4=1,lz4hc=2,snappy=3,zlib=4,zstd=5";

static NClist* bloscfilters = NULL;

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
static int NCZ_load_plugin(const char* path, struct NCZ_plugin* plug);
static int NCZ_unload_plugin(struct NCZ_plugin* plugin, int forceunload);
static int NCZ_filter_loaded(int filterid, const NCZ_filter_class** fp);

static int parse_special_filter(const NC_VAR_INFO_T*, const struct SpecialFilter*, const NCjson*, NCZ_Filter* filterp);
static int getentries(const char* path, NClist* contents);
static int jsonize_special_filter(const NC_VAR_INFO_T*, const struct SpecialFilter*, const NCZ_Filter*, NCjson*);
static int buildblosclist(NClist* list, const char* txt);

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
        NCZ_unload_plugin((struct NCZ_plugin*)spec->code,!FORCEUNLOAD);
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
    if(spec->params != NULL) nullfree(spec->params)
    free(spec);
done:
    return NC_NOERR;
}

int
NCZ_addfilter(NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    struct NCZ_Filter* fi = NULL;
    
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
        fi->filterid = id;
	nclistpush(flist,fi);
    }    
    fi->nparams = nparams;
    if(fi->params != NULL) {
	nullfree(fi->params);
	fi->params = NULL;
    }
    assert(fi->params == NULL);
    if(fi->nparams > 0) {
	if((fi->params = (unsigned int*)malloc(sizeof(unsigned int)*fi->nparams)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        memcpy(fi->params,params,sizeof(unsigned int)*fi->nparams);
    }
PRINTFILTERLIST(var,"add");
    fi = NULL; /* either way,its in the var->filters list */

done:
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
        if(f->filterid == id) {
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
	struct NCZ_Filter* spec = nclistget(flist,i);
	if(id == spec->filterid) {
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

    ZTRACE(1,"ncid=%d varid=%d id=%u nparams=%u params=%s",ncid,varid,(unsigned)nparams,nczprint_paramvector(nparams,params));

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
	    ids[k] = f->filterid;
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
    if(nparamsp) *nparamsp = spec->nparams;
    if(params && spec->nparams > 0) {
	memcpy(params,spec->params,sizeof(unsigned int)*spec->nparams);
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
    /* Setup the blosc filter set */
    {
        const char* rctag;
	bloscfilters = nclistnew();
	if((stat = buildblosclist(bloscfilters,defalt_blosc_filters))) goto done;
        rctag = NC_rclookup(BLOSCRC_TAG,NULL);
	if(rctag != NULL) {
	    if((stat = buildblosclist(bloscfilters,rctag))) goto done;
	}
    }
done:
    return stat;
}

int
NCZ_filter_finalize(void)
{
    int stat = NC_NOERR;
    nclistfreeall(bloscfilters);
    bloscfilters = NULL;
    return stat;
}

static int
buildblosclist(NClist* list, const char* txt)
{
    int i,stat = NC_NOERR;
    NClist* tmp = nclistnew();

    stat = NCZ_comma_parse(txt,tmp);
    if(nclistlength(tmp) > 0 && strcasecmp("reset",(const char*)nclistget(tmp,0))==0)
	nclistclearall(list);
    for(i=0;i<nclistlength(tmp);i++) {
	int index = -1;
	char* pair = (char*)nclistget(tmp,i);
	char* p = strchr(pair,'=');
	if(p != NULL) {
	    *p++ = '\0';
	    sscanf(p,"%d",&index);
	    if(index < 0) {stat = NC_EINVAL; goto done;}
	    /* allow a null name */
	    if(strcasecmp(pair,"null")==0) pair = NULL;
	    nclistinsert(list,index,nulldup(pair));
	}
    }
done:
    nclistfreeall(tmp);
    return stat;
}

static int
NCZ_filter_loaded(int filterid, const NCZ_filter_class** fp)
{
    int stat = NC_NOERR;
    struct NCZ_plugin* plug = NULL;
    if(filterid <= 0 || filterid >= NCZ_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    plug = &loaded_filters[filterid];
    if(plug == NULL) stat = NC_ENOFILTER;
    if(fp) *fp = plug->filter;
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
	assert(f != NULL && f->filterid > 0);
	if(f->code == NULL) {
	    const NCZ_filter_class* ff = NULL;
	    /* attach to the filter code */
	    if((stat = NCZ_filter_loaded(f->filterid,&ff))) goto done;
	    f->code = ff;
	}
    }

    {
	struct NCZ_Filter* f = NULL;
	NCZ_filter_class* ff = NULL;
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
	        ff = (NCZ_filter_class*)f->code;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(0,f->nparams,f->params,current_used,&next_alloc,&next_buf);
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
	        ff = (NCZ_filter_class*)f->code;
	        /* code can be simplified */
	        next_alloc = current_alloc;
	        next_buf = current_buf;
	        next_used = 0;
	        next_used = ff->filter(NCZ_FILTER_REVERSE,f->nparams,f->params,current_used,&next_alloc,&next_buf);
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
    NCjson* jparams = NULL;
    char value[64];
    const struct SpecialFilter* p;
    
    if((stat = NCJnew(NCJ_DICT,&jfilter))) goto done;

    /* See if this is a special case */
    for(p=specialfilters;p->name;p++) {
        if(filter->filterid == p->id) {
	    if((stat = jsonize_special_filter(var,p,filter,jfilter))) goto done;
	    goto done;
	}
    }
    /* insert id */
    if((stat = NCJaddstring(jfilter,NCJ_STRING,"id"))) goto done;
    snprintf(value,sizeof(value),"%u",filter->filterid);
    if((stat = NCJaddstring(jfilter,NCJ_INT,value))) goto done;    
    if((stat = NCJaddstring(jfilter,NCJ_STRING,"parameters"))) goto done;
    /* Compute set of parameters, empty array if none */
    if((stat = NCJnew(NCJ_ARRAY,&jparams))) goto done;
    for(i=0;i<filter->nparams;i++) {
        snprintf(value,sizeof(value),"%u",filter->params[i]);
	if((stat = NCJaddstring(jparams,NCJ_INT,value))) goto done;    
    }
    if((stat = NCJappend(jfilter,jparams))) goto done;
    jparams = NULL;
    
    if(jfilterp) {*jfilterp = jfilter; jfilter = NULL;}

done:
    if(jparams) NCJreclaim(jparams);
    if(jfilter) NCJreclaim(jfilter);
    return stat;
}

int
NCZ_filter_build(NC_VAR_INFO_T* var, NCjson* jfilter, NCZ_Filter** filterp)
{
    int i,stat = NC_NOERR;
    NCZ_Filter* filter = NULL;
    NCjson* jvalue = NULL;
    const struct SpecialFilter* p;

    if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}
    if((stat = NCJdictget(jfilter,"id",&jvalue))) goto done;    
    /* See if this is a special case */
    for(p=specialfilters;p->name;p++) {
        if(strcasecmp(p->name,jvalue->value)==0) {
	    stat = parse_special_filter(var,p,jfilter,filter);
	    if(filterp) {*filterp = filter; filter = NULL;}
	    goto done;
	}
    }
    /* Try to scanf the id value */
    if((1 != sscanf(jvalue->value,"%u",&filter->filterid)))
        {stat = NC_EFILTER; goto done;}
    /* get the parameters */
    if((stat = NCJdictget(jfilter,"parameters",&jvalue))) goto done;
    if(jvalue->sort != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
    /* Figure out the max # of parameters */
    filter->nparams = NCJlength(jvalue);
    if((filter->params = calloc(sizeof(unsigned int),filter->nparams))==NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<filter->nparams;i++) {
	NCjson* jparam;
	if((stat = NCJarrayith(jvalue,(size_t)i,&jparam))) goto done;
        /* parse it */
	sscanf(jparam->value,"%u",&filter->params[i]);
    }
#if DEBUG > 0
    fprintf(stderr,"build filter: id=%u nparams=%u params=%s\n",filter->filterid,(unsigned)filter->nparams,nczprint_paramvector(filter->nparams,filter->params));
#endif
    if(filterp) {*filterp = filter; filter = NULL;}
done:
    NCZ_filter_free(filter);
    return stat;
}

static int
jsonize_special_filter(const NC_VAR_INFO_T* var, const struct SpecialFilter* special, const NCZ_Filter* filter, NCjson* jfilter)
{
    int stat = NC_NOERR;
    NCjson* jtmp = NULL;
    char tmp[64];
    const char* p;
    
    switch (filter->filterid) {

    case BLOSC_FILTERID:
	if(filter->nparams != special->nparams) {stat = NC_EFILTER; goto done;}
	snprintf(tmp,sizeof(tmp),"%u",filter->filterid);
	if((stat = NCJnewstring(NCJ_INT,tmp,&jtmp))) goto done;
	if((stat = NCJinsert(jfilter,"id",jtmp))) goto done;
	jtmp = NULL;
	snprintf(tmp,sizeof(tmp),"%u",filter->params[0]);
	if((stat = NCJnewstring(NCJ_INT,tmp,&jtmp))) goto done;
	if((stat = NCJinsert(jfilter,"clevel",jtmp))) goto done;
	jtmp = NULL;
	if((stat = NCJnewstring(NCJ_BOOLEAN,(filter->params[1]?"true":"false"),&jtmp))) goto done;
	if((stat = NCJinsert(jfilter,"shuffle",jtmp))) goto done;
	jtmp = NULL;
#if 0
	/* This is non-standard */
	snprintf(tmp,sizeof(tmp),"%u",filter->params[2]);
	if((stat = NCJnewstring(NCJ_INT,tmp,&jtmp))) goto done;
	if((stat = NCJinsert(jfilter,"typesize",jtmp))) goto done;
	jtmp = NULL;
#endif
	if(filter->params[3] < 0 || filter->params[3] >= nclistlength(bloscfilters))
	    {stat = NC_EFILTER; goto done;}
	/* Convert the compressor id to a name */
	p = (const char*)nclistget(bloscfilters,filter->params[3]);
	if((stat = NCJnewstring(NCJ_STRING,p,&jtmp))) goto done;
	if((stat = NCJinsert(jfilter,"cname",jtmp))) goto done;
	jtmp = NULL;
	break;
    default: stat = NC_ENOFILTER; goto done;
    };

done:
    NCJreclaim(jtmp);
    return stat;
}

static int
parse_special_filter(const NC_VAR_INFO_T* var, const struct SpecialFilter* special, const NCjson* jfilter, NCZ_Filter* filter)
{
    int i,stat = NC_NOERR;
    NCjson* jvalue = NULL;
    unsigned filterid = 0;
    size_t nparams;
    unsigned params[32]; /* max across all supported special filters */

    if((stat = NCJdictget(jfilter,"id",&jvalue))) goto done;    
    if(strcasecmp(jvalue->value,"blosc")==0) {
	/* c-blosc appears to take four arguments (aside from buffers)
	   1. compression level -- int 0<=level<=10
   	   2. doshuffle -- int 1=>shuffle
      	   3. typesize -- in bytes
	   4. compressor used by blosc -- e.g. "blosclz", "lz4", "lz4hc", "snappy", "zlib" or "zstd"
	   Technically type size is also a parameter, but can be computed from var.
	*/
	struct NCJconst njc;
	size_t typsize;
	int found;

	filterid = special->id;
	nparams = special->nparams;
        if((stat = NCJdictget(jfilter,"clevel",&jvalue))) goto done;    
	sscanf(jvalue->value,"%u",&params[0]);
        if((stat = NCJdictget(jfilter,"shuffle",&jvalue))) goto done;
	if((stat = NCJcvt(jvalue,NCJ_BOOLEAN,&njc))) goto done;
	params[1] = njc.bval;
	/* Figure out the typesize */
	if((stat = NC4_inq_atomic_type(var->type_info->hdr.id,NULL,&typsize))) goto done;
	params[2] = (unsigned)typsize;

	/* Get sub-compressor name */
        if((stat = NCJdictget(jfilter,"cname",&jvalue))) goto done;
	if(jvalue->sort != NCJ_STRING) {stat = NC_ENOFILTER; goto done;}
	/* Search for the name */
	found = 0;
	for(i=0;i<nclistlength(bloscfilters);i++) {
	    const char* name = (const char*)nclistget(bloscfilters,i);
	    if(strcasecmp(name,jvalue->value)==0) {
		params[3] = i;
		found = 1;
		break;
	    }
	}	
	if(!found) {stat = NC_ENOFILTER; goto done;}
    } else
        {stat = NC_ENOFILTER; goto done;}
    filter->filterid = filterid;
    filter->nparams = nparams;
    filter->params = (unsigned*)malloc(sizeof(unsigned)*nparams);
    if(filter->params == NULL) {stat = NC_ENOMEM; goto done;}
    memcpy(filter->params,params,sizeof(unsigned)*nparams);

done:
    return stat;
}

/**************************************************/
/* Filter loading */

/*
Get entries in a path that is assumed to be a directory.
*/

#ifdef WIN32

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

#else /* !WIN32 */

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
	    snprintf(pluginpath32,sizeof(pluginpath32),plugindir_win,win32_root);
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
	struct NCZ_plugin plugin = {NULL,NULL};
	int id;

	assert(nmlen > 0);
	nullfree(file); file = NULL;
	if((file = (char*)malloc(flen))==NULL) {stat = NC_ENOMEM; goto done;}
	file[0] = '\0';
	strlcat(file,path,flen);
	strlcat(file,"/",flen);
	strlcat(file,name,flen);
	/* See if can load the file */
	if((stat = NCZ_load_plugin(file,&plugin)) == NC_NOERR) {
	    id = plugin.filter->id;
	    if(loaded_filters[id].filter == NULL) {
	        plugin.refcount = 1;
	        loaded_filters[id] = plugin;
#if DEBUG > 1
		fprintf(stderr,"plugin loaded: id=%u, name=%s\n",id,(plugin.filter->name?plugin.filter->name:"unknown"));
#endif
	    } else {
#if DEBUG > 1
		fprintf(stderr,"plugin duplicate: id=%u, name=%s\n",id,(plugin.filter->name?plugin.filter->name:"unknown"));
#endif
	        NCZ_unload_plugin(&plugin,FORCEUNLOAD); /* its a duplicate */
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
NCZ_load_plugin(const char* path, struct NCZ_plugin* plug)
{
    int stat = NC_NOERR;
    const NCZ_filter_class* fclass = NULL;
    NCPSharedLib* lib = NULL;
    int flags = NCP_GLOBAL;
    
    assert(path != NULL && strlen(path) > 0 && plug != NULL);

    /* load the shared library */
    if((stat = ncpsharedlibnew(&lib))) goto done;
    if((stat = ncpload(lib,path,flags))) goto done;

    /* See if this is a filter plugin */
    {
	NCZ_get_plugin_type_proto gpt =  (NCZ_get_plugin_type_proto)ncpgetsymbol(lib,"H5PLget_plugin_type");
	NCZ_get_plugin_info_proto gpi =  (NCZ_get_plugin_info_proto)ncpgetsymbol(lib,"H5PLget_plugin_info");
	if(gpt == NULL) {stat = NC_ENOTFOUND; goto done;}
	if(gpi == NULL) {stat = NC_ENOTFOUND; goto done;}
	if(gpt() != NCZP_TYPE_FILTER) {stat = NC_ENOTFOUND; goto done;}
	fclass = gpi();
	if(fclass->version != NCZ_FILTER_CLASS_VER) {stat = NC_EFILTER; goto done;}
    }
    plug->filter = fclass;
    plug->library = lib;
    lib = NULL;

done:
    if(lib) {
#if DEBUG > 1
	const char* errmsg = ncpgeterrmsg(lib);
	if(errmsg) fprintf(stderr,"err: %s\n", errmsg);
#endif	
        (void)ncpsharedlibfree(lib);
    }
    return stat;
}

static int
NCZ_unload_plugin(struct NCZ_plugin* plugin, int forceunload)
{
    if(plugin && (plugin->refcount == 0 || forceunload)) {
	(void)ncpsharedlibfree(plugin->library);
	plugin->filter = NULL;
	plugin->library = NULL;
    }
    return NC_NOERR;
}

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
#undef DEBUGF

#undef TFILTERS

/* Hold the loaded filter plugin information */
typedef struct NCZ_Plugin {
    struct HDF5API {
        const H5Z_class2_t* filter;
        NCPSharedLib* hdf5lib; /* source of the filter */
    } hdf5;
    struct CodecAPI {
	const NCZ_codec_t* codec;
	NCPSharedLib* codeclib; /* of the source codec; null if same as hdf5 */
    } codec;
} NCZ_Plugin;

/* The NC_VAR_INFO_T->filters field is an NClist of this struct */
/*
Each filter can have two parts: HDF5 and Codec.
The NC_VAR_INFO_T.filters list only holds entries where both the HDF5 info
and the codec info are defined.
The NCZ_VAR_INFO_T.codecs list holds the codec info when reading a Zarr file.
Note that it is not possible to have an entry on the filters list that does not
have both HDF5 and codec. This is because nc_def_var_filter will fail if the codec
part is not available. If a codec is read from a file and there is no available
corresponding HDF5 implementation, then that codec will not appear in the filters list.
It is possible that some subset of the codecs do have a corresponding HDF5, but we
enforce the rule that no entries go into the filters list unless all are defined.
It is still desirable for a user to be able to see what filters and codecs are defined
for a variable. This is accommodated by providing two special attributes:
1, "_Filters" attribute shows the HDF5 filters defined on the variable, if any.
2, "_Codecs" attribute shows the codecs defined on the variable; for zarr, this list
   should always be defined.
*/

/* Codec Info */
typedef struct NCZ_Codec {
    char* id;              /**< The NumCodecs ID */
    NCjson* codec;         /**< The Codec from the file; NULL if creating */
} NCZ_Codec;

/* HDF5 Info */
typedef struct NCZ_HDF5 {
    unsigned id;           /**< HDF5 id corresponding to filterid. */
    int nparams;           /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
} NCZ_HDF5;

typedef struct NCZ_Filter {
    int flags;             	/**< Flags describing state of this filter. */
    NCZ_HDF5 hdf5;
    NCZ_Codec* codec;  		/**< Points to an entry also in Codecs list */
    void* codec_context;        /**< From setup(). */
    struct NCZ_Plugin* plugin;  /**< Implementation of this filter. */
} NCZ_Filter;


/* All possible HDF5 filter plugins */
/* Convert to linked list or hash table since very sparse */
NCZ_Plugin* loaded_plugins[H5Z_FILTER_MAX];
int loaded_plugins_max = -1;

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


/* Forward */
static int NCZ_load_all_plugins(void);
static int NCZ_load_plugin_dir(const char* path);
static int NCZ_load_plugin(const char* path, NCZ_Plugin** plugp);
static int NCZ_unload_plugin(NCZ_Plugin* plugin);
static int NCZ_plugin_loaded(int filterid, NCZ_Plugin** pp);
static int NCZ_plugin_save(int filterid, NCZ_Plugin* p);
static int NCZ_filter_free(NCZ_Filter* spec);
static int NCZ_h5filter_clear(NCZ_HDF5* spec);
static int NCZ_codec_free(NCZ_Codec* spec);
static int NCZ_codec_lookup(NClist* codecs, const char* id, NCZ_Codec** codecp);
static int NCZ_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NCZ_Filter** specp);

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

    ZTRACE(6,"var=%s",var->hdr.name);
    if(filters == NULL) goto done;
PRINTFILTERLIST(var,"free: before");
    /* Free the filter list elements */
    for(i=0;i<nclistlength(filters);i++) {
	struct NCZ_Filter* spec = nclistget(filters,i);
	if(spec->plugin->codec.codec->NCZ_codec_reset)
            (void)spec->plugin->codec.codec->NCZ_codec_reset(spec->codec_context);
	if((stat = NCZ_filter_free(spec))) goto done;
    }
PRINTFILTERLIST(var,"free: after");
    nclistfree(filters);
    var->filters = NULL;
done:
    return ZUNTRACE(stat);
}

int
NCZ_codec_freelist(NCZ_VAR_INFO_T* zvar)
{
    int i, stat=NC_NOERR;
    NClist* codecs = zvar->codecs;

    ZTRACE(6,"zvar=%p",zvar);
    if(codecs == NULL) goto done;
    /* Free the codec list elements */
    for(i=0;i<nclistlength(codecs);i++) {
	NCZ_Codec* spec = nclistget(codecs,i);
	if((stat = NCZ_codec_free(spec))) goto done;
    }
    nclistfree(codecs);
    zvar->codecs = NULL;
done:
    return ZUNTRACE(stat);
}

static int
NCZ_filter_free(NCZ_Filter* spec)
{
    if(spec == NULL) return NC_NOERR;
PRINTFILTER(spec,"free");
    NCZ_h5filter_clear(&spec->hdf5);
    free(spec);
    return NC_NOERR;
}

static int
NCZ_h5filter_clear(NCZ_HDF5* spec)
{
    ZTRACE(6,"spec=%d",spec->id);
    if(spec == NULL) goto done;
    nullfree(spec->params);
done:
    return ZUNTRACE(NC_NOERR);
}

static int
NCZ_codec_free(NCZ_Codec* spec)
{
    ZTRACE(6,"spec=%d",(spec?spec->id:"null"));
    if(spec == NULL) goto done;
    nullfree(spec->id);
    NCJreclaim(spec->codec);
    free(spec);
done:
    return ZUNTRACE(NC_NOERR);
}

int
NCZ_addfilter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params)
{
    int stat = NC_NOERR;
    struct NCZ_Filter* fi = NULL;
    char* codectext = NULL;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    NCZ_Plugin* plugin = NULL;
    NCZ_Codec* codec = NULL;
    NCjson* jcodec = NULL;
    NCjson* jid = NULL;
    int codecexists = 0;
    unsigned* cloneparams = NULL;
    int clonenparams = (int)nparams;
    void* context = NULL;

    ZTRACE(6,"file=%s var=%s id=%u nparams=%u params=%p",file->hdr.name,var->hdr.name,id,nparams,params);
    
    if(nparams > 0 && params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if(var->filters == NULL) var->filters = (void*)nclistnew();
    if(zvar->codecs == NULL) zvar->codecs = nclistnew();

    /* Before anything else, find the matching plugin */
    if((stat = NCZ_plugin_loaded(id,&plugin))) goto done;
    if(plugin == NULL || plugin->codec.codec == NULL) { /* fail */
	stat = NC_ENOFILTER;
	goto done;
    }

    /* Setup the codec wrt to var */
    if(nparams > 0) {
	if((cloneparams = (unsigned*)calloc(nparams,sizeof(unsigned)))==NULL) goto done;
	memcpy(cloneparams,params,nparams*sizeof(unsigned));
    } else cloneparams = NULL;
    
    if(plugin && context && plugin->codec.codec->NCZ_codec_setup)
        {if((stat = plugin->codec.codec->NCZ_codec_setup(ncidfor(file,var->container->hdr.id),var->hdr.id,&context))) goto done;}
    /* Attempt to convert hdf5 to codec */
    if((stat = plugin->codec.codec->NCZ_hdf5_to_codec(context,clonenparams,cloneparams,&codectext))) goto done;

    /* Parse result */
    if((stat = NCJparse(codectext,0,&jcodec))) goto done;
    /* Get the id */
    if((stat = NCJdictget(jcodec,"id",&jid))) goto done;
    
    /* See if already defined */
    if((stat = NCZ_codec_lookup(zvar->codecs,NCJstring(jid),&codec))) goto done;
    if(codec == NULL) {
        /* Create the codec */
        if((codec = calloc(1,sizeof(struct NCZ_Codec))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
    } else
        codecexists = 1;
    /* Save/overwrite the codec JSON */
    nullfree(codec->id);
    NCJreclaim(codec->codec);
    if((codec->id = strdup(NCJstring(jid)))==NULL)
	{stat = NC_ENOMEM; goto done;}
    codec->codec = jcodec; jcodec = NULL;
    if(plugin && context && plugin->codec.codec->NCZ_codec_reset)
        (void)plugin->codec.codec->NCZ_codec_reset(context);

    /* Build/find the NCZ_Filter */
    if((stat=NCZ_filter_lookup(var,id,&fi))) goto done;
    if(fi != NULL) {
        /* already exists */
    } else {
        NClist* flist = (NClist*)var->filters;
	stat = NC_NOERR;
        if((fi = calloc(1,sizeof(struct NCZ_Filter))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        fi->hdf5.id = id;
	nclistpush(flist,fi);
    }    
    fi->hdf5.nparams = clonenparams;
    if(fi->hdf5.params != NULL) {
	nullfree(fi->hdf5.params);
	fi->hdf5.params = NULL;
    }
    assert(fi->hdf5.params == NULL);
    if(fi->hdf5.nparams > 0) {
	fi->hdf5.params = cloneparams; cloneparams = NULL;
    }
    fi->codec = codec;
    fi->codec_context = context; context = NULL;
    if(!codecexists) {
        /* Add codec to the codecs list */
        nclistpush(zvar->codecs,codec);
        /* remember where this came from */
    }
    fi->plugin = plugin;
    codec = NULL;
    
PRINTFILTERLIST(var,"add");
    fi = NULL; /* either way,its in the var->filters list */

done:
    if(plugin && context && plugin->codec.codec->NCZ_codec_reset)
        (void)plugin->codec.codec->NCZ_codec_reset(context);
    nullfree(cloneparams);
    nullfree(codectext);
    NCJreclaim(jcodec);
    NCZ_codec_free(codec);
    if(fi) NCZ_filter_free(fi);    
    return ZUNTRACE(stat);
}

int
NCZ_filter_remove(NC_VAR_INFO_T* var, unsigned int id)
{
    int k, stat = NC_NOERR;
    NClist* flist = (NClist*)var->filters;

    ZTRACE(6,"var=%s id=%u",var->hdr.name,id);
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
	    goto done;
	}
    }
    stat = NC_ENOFILTER;
done:
    return ZUNTRACE(stat);
}

static int
NCZ_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NCZ_Filter** specp)
{
    int i;
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
	if(spec->hdf5.id == id) {
	    if(specp) *specp = spec;
	    break;
	}
    }
    return ZUNTRACEX(NC_NOERR,"spec=%d",IEXISTS(specp,hdf5.id));
}

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
    NCZ_Filter* oldspec = NULL;
    NCZ_Filter* tmp = NULL;
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
    if((stat=NCZ_filter_lookup(var,id,&oldspec))) goto done;

    /* See if deflate &/or szip is defined */
    if((stat = NCZ_filter_lookup(var,H5Z_FILTER_DEFLATE,&tmp))) goto done;
    havedeflate = (tmp == NULL ? 0 : 1);

    if((stat = NCZ_filter_lookup(var,H5Z_FILTER_SZIP,&tmp))) goto done;
    haveszip = (tmp == NULL ? 0 : 1);

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
    if((stat = NCZ_addfilter(h5,var,id,nparams,params)))
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
    size_t nfilters = 0;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));
    ZTRACE(1,"ncid=%d varid=%d",ncid,varid);

    if((stat = NC_check_id(ncid,&nc))) goto done;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* Make sure all the filters are defined */
    if((stat = NCZ_filter_initialize())) goto done;

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
    struct NCZ_Filter* spec = NULL;

    LOG((2, "%s: ncid 0x%x varid %d", __func__, ncid, varid));
    ZTRACE(1,"ncid=%d varid=%d id=%u",ncid,varid,id);
    
    if((stat = NC_check_id(ncid,&nc))) goto done;
    assert(nc);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = ncz_find_grp_file_var(ncid, varid, &h5, &grp, &var)))
	{stat = THROW(stat); goto done;}

    assert(h5 && var && var->hdr.id == varid);

    /* Make sure all the plugins are defined */
    if((stat = NCZ_filter_initialize())) goto done;

    if((stat = NCZ_filter_lookup(var,id,&spec))) goto done;
    if(spec != NULL) {
        if(nparamsp) *nparamsp = spec->hdf5.nparams;
        if(params && spec->hdf5.nparams > 0)
	    memcpy(params,spec->hdf5.params,sizeof(unsigned int)*spec->hdf5.nparams);
    } else
        stat = NC_ENOFILTER;
 
done:
    return ZUNTRACEX(stat,"nparams=%u",(unsigned)*nparamsp);
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
    ZTRACE(6,"");
    if(NCZ_filter_initialized) goto done;
    {
        NCZ_filter_initialized = 1;
        memset(loaded_plugins,0,sizeof(loaded_plugins));
        if((stat = NCZ_load_all_plugins())) goto done;
    }
done:
    return ZUNTRACE(stat);
}

int
NCZ_filter_finalize(void)
{
    int stat = NC_NOERR;
    int i;
    ZTRACE(6,"");
    /* Reclaim all loaded filters */
    for(i=0;i<=loaded_plugins_max;i++) {
        NCZ_unload_plugin(loaded_plugins[i]);
	loaded_plugins[i] = NULL;
    }
    return ZUNTRACE(stat);
}

static int
NCZ_plugin_save(int filterid, NCZ_Plugin* p)
{
    int stat = NC_NOERR;
    ZTRACE(6,"filterid=%d p=%p",filterid,p);
    if(filterid <= 0 || filterid >= H5Z_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    if(filterid > loaded_plugins_max) loaded_plugins_max = filterid;
    loaded_plugins[filterid] = p;
done:
    return ZUNTRACE(stat);
}

static int
NCZ_plugin_loaded(int filterid, NCZ_Plugin** pp)
{
    int stat = NC_NOERR;
    struct NCZ_Plugin* plug = NULL;
    ZTRACE(6,"filterid=%d",filterid);
    if(filterid <= 0 || filterid >= H5Z_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    if(filterid <= loaded_plugins_max) 
        plug = loaded_plugins[filterid];
    if(pp) *pp = plug;
done:
    return ZUNTRACEX(stat,"plugin=%p",*pp);
}

int
NCZ_applyfilterchain(NClist* chain, size_t inlen, void* indata, size_t* outlenp, void** outdatap, int encode)
{
    int i, stat = NC_NOERR;
    void* lastbuffer = NULL; /* if not null, then last allocated buffer */

    ZTRACE(6,"|chain|=%u inlen=%u indata=%p encode=%d", (unsigned)nclistlength(chain), (unsigned)inlen, indata, encode);

    /* Make sure all the filters are loaded */
    for(i=0;i<nclistlength(chain);i++) {
	struct NCZ_Filter* f = (struct NCZ_Filter*)nclistget(chain,i);
	assert(f != NULL && f->hdf5.id > 0);
	if(f->plugin == NULL) {
	    NCZ_Plugin* np = NULL;
	    /* attach to the filter code */
	    stat = NCZ_plugin_loaded(f->hdf5.id,&np);
	    if(stat) {stat = NC_ENOFILTER; goto done;}
	    f->plugin = np;
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
NCZ_filter_jsonize(const NC_VAR_INFO_T* var, const NCZ_Filter* filter, NCjson** jfilterp)
{
    int stat = NC_NOERR;
    NCjson* jfilter = NULL;
    char* codec = NULL;
    
    ZTRACE(6,"var=%s filter=%s",var->hdr.name,filter->codec->id);
    /* Convert the HDF5 id + parameters to the codec form */
    assert(filter->codec->id != NULL && filter->plugin != NULL);
    if((stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(filter->codec_context,filter->hdf5.nparams,filter->hdf5.params,&codec))) goto done;
    /* Parse it */
    if((stat = NCJparse(codec,0,&jfilter))) goto done;    
fprintf(stderr,"zfilter.parse.1: %p\n",jfilter);
    if(jfilterp) {*jfilterp = jfilter; jfilter = NULL;}

done:
    if(codec) free(codec);
    if(jfilter) NCJreclaim(jfilter);
    return ZUNTRACEX(stat,"json=%s",(!jfilterp || !*jfilterp ? "null":NCJtrace(*jfilterp)));
}

int
NCZ_filter_build(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jfilter)
{
    int i,stat = NC_NOERR;
    NCZ_Filter* filter = NULL;
    NCjson* jvalue = NULL;
    NCZ_Plugin* plugin = NULL;
    char* text = NULL;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    NCZ_Codec* codec = NULL;
    NCZ_HDF5 hdf5filter = {0,0,NULL};
    void* context = NULL;

    ZTRACE(6,"file=%s var=%s jfilter=%s",file->hdr.name,var->hdr.name,NCJtrace(jfilter));

    if(var->filters == NULL) var->filters = nclistnew();
    if(zvar->codecs == NULL) zvar->codecs = nclistnew();

    /* Get the id of this codec filter */
    if((stat = NCJdictget(jfilter,"id",&jvalue))) goto done;
    if(NCJsort(jvalue) != NCJ_STRING) {stat = NC_ENOFILTER; goto done;}

    /* Add to the list of codecs */
    if((codec = (NCZ_Codec*)calloc(1,sizeof(NCZ_Codec)))==NULL)
        {stat = NC_ENOMEM; goto done;}
    if((stat = NCJclone(jfilter,&codec->codec))) goto done;
    if((codec->id = strdup(NCJstring(jvalue)))==NULL)
        {stat = NC_ENOMEM; goto done;}

    /* Find the plugin for this filter */
    for(i=0;i<=loaded_plugins_max;i++) {
	if(loaded_plugins[i] && strcmp(NCJstring(jvalue),loaded_plugins[i]->codec.codec->codecid)==0) 
	    {plugin = loaded_plugins[i]; break;}
    }

    if(plugin != NULL) {
        /* Get codec context */
        if(plugin->codec.codec->NCZ_codec_setup)
            {if((stat = plugin->codec.codec->NCZ_codec_setup(ncidfor(file,var->container->hdr.id),var->hdr.id,&context))) goto done;}
        /* Convert to HDF5 form */
        hdf5filter.id = plugin->hdf5.filter->id;
	if((stat = NCJunparse(jfilter,0,&text))) goto done;
        if((stat = plugin->codec.codec->NCZ_codec_to_hdf5(context,text,&hdf5filter.nparams,&hdf5filter.params))) goto done;
	/* Since we have both halves, create the filter */
        if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}		
	filter->hdf5 = hdf5filter; hdf5filter.params = NULL;
	filter->codec = codec;
        filter->plugin = plugin; plugin = NULL;
	filter->codec_context = context; context = NULL;
    }

    /* Add codec to the codecs list */
    if(codec != NULL) {
        nclistpush(zvar->codecs,codec);
        codec = NULL;
    }
    if(filter != NULL) {
        NClist* filterlist = (NClist*)var->filters;
        nclistpush(filterlist,filter);
        filter = NULL;
    }
    
done:
    if(plugin && context && plugin->codec.codec->NCZ_codec_reset)
        (void)plugin->codec.codec->NCZ_codec_reset(context);
    NCZ_codec_free(codec);
    NCZ_h5filter_clear(&hdf5filter);
    if(plugin) NCZ_unload_plugin(plugin);
    nullfree(text);
    NCZ_filter_free(filter);
    return ZUNTRACE(stat);
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

    ZTRACE(6,"path=%s",path);

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

    ZTRACE(6,"path=%s",path);

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
    
   ZTRACE(6,"");

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
    ZTRACEMORE(6,"pluginroot=%s",(pluginroot?pluginroot:"null"));
    if(pluginroot == NULL) {ret = NC_EINVAL; goto done;}

    /* Make sure the root is actually a directory */
    errno = 0;
    ret = NCstat(pluginroot, &buf);
#if 1
    ZTRACEMORE(6,"stat: ret=%d, errno=%d st_mode=%d",ret,errno,buf.st_mode);
#endif
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
	NCZ_Plugin* p;
	for(i=0;i<loaded_plugins_max;i++) {
	    if((p = loaded_plugins[i]) != NULL) {
		if(p->hdf5.filter == NULL || p->codec.codec == NULL) {
		    /* expunge this entry */
		    (void)NCZ_unload_plugin(p);
		    loaded_plugins[i] = NULL;
		}
	    }
	}
    }
    
done:
    errno = 0;
    return ZUNTRACE(ret);
}

/* Load all the filters within a specified directory */
static int
NCZ_load_plugin_dir(const char* path)
{
    int i,stat = NC_NOERR;
    size_t pathlen;
    NClist* contents = nclistnew();
    char* file = NULL;

    ZTRACE(7,"path=%s",path);

    if(path == NULL) {stat = NC_EINVAL; goto done;}
    pathlen = strlen(path);
    if(pathlen == 0) {stat = NC_EINVAL; goto done;}

    if((stat = getentries(path,contents))) goto done;
    for(i=0;i<nclistlength(contents);i++) {
        const char* name = (const char*)nclistget(contents,i);
	size_t nmlen = strlen(name);
	size_t flen = pathlen+1+nmlen+1;
	int id;
	NCZ_Plugin* plugin = NULL;

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
	    if(loaded_plugins[id] == NULL) {
	        loaded_plugins[id] = plugin;
		if(id > loaded_plugins_max) loaded_plugins_max = id;
#ifdef DEBUGF
		fprintf(stderr,"plugin loaded: id=%u, name=%s\n",id,(plugin->hdf5.filter->name?plugin->hdf5.filter->name:"unknown"));
#endif
	    } else {
#ifdef DEBUGF
		fprintf(stderr,"plugin duplicate: id=%u, name=%s\n",id,(plugin->hdf5.filter->name?plugin->hdf5.filter->name:"unknown"));
#endif
	        NCZ_unload_plugin(plugin); /* its a duplicate */
	    }
	} else
	    stat = NC_NOERR; /*ignore failure */
    }	

done:
    nullfree(file);
    nclistfreeall(contents);
    return ZUNTRACE(stat);
}

static int
NCZ_load_plugin(const char* path, struct NCZ_Plugin** plugp)
{
    int stat = NC_NOERR;
    NCZ_Plugin* plugin = NULL;
    const H5Z_class2_t* h5class = NULL;
    const NCZ_codec_t* codec = NULL;
    NCPSharedLib* lib = NULL;
    int flags = NCP_GLOBAL;
    int h5id = -1;
    
    assert(path != NULL && strlen(path) > 0 && plugp != NULL);

    ZTRACE(8,"path=%s",path);

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
	    codec = npi();
	    /* Verify */
            if(codec->version != NCZ_CODEC_CLASS_VER) {stat = NC_EPLUGIN; goto done;}
	    if(codec->sort != NCZ_CODEC_HDF5) {stat = NC_EPLUGIN; goto done;}
	}
    }

    /* Ignore this library if neither h5class nor codec are defined */
    if(h5class == NULL && codec == NULL) goto done;

#ifdef DEBUGF
fprintf(stderr,"load: %s:",path);
if(h5class) fprintf(stderr," %u",(unsigned)h5class->id);
if(codec) fprintf(stderr," %u/%s",codec->hdf5id,codec->codecid);
fprintf(stderr,"\n");
#endif

    if(h5class != NULL && codec != NULL) {
	/* Verify consistency of the HDF5 and the Codec */
	if(h5class->id != codec->hdf5id) goto done; /* ignore */
    } 

    /* There are several cases to consider:
    1. This library has both HDF5 API and Codec API => merge
    2. This library has HDF5 API only and Codec API was already found in another library => merge
    3. This library has Codec API only and HDF5 API was already found in another library => merge    
    */

    /* Get any previous plugin entry for this id; may be NULL */
    if(h5class != NULL) {
	h5id = h5class->id;
	if((stat = NCZ_plugin_loaded(h5class->id,&plugin))) goto done;
    } else if(codec != NULL) {
	h5id = codec->hdf5id;
	if((stat = NCZ_plugin_loaded(codec->hdf5id,&plugin))) goto done;
    }
    if(plugin == NULL) {
	/* create new entry */
	if((plugin = (NCZ_Plugin*)calloc(1,sizeof(NCZ_Plugin)))==NULL) {stat = NC_ENOMEM; goto done;}
    } 

    /* Fill in the plugin */
    if(plugin->hdf5.filter == NULL) {
	plugin->hdf5.filter = h5class;
	plugin->hdf5.hdf5lib = lib;
	lib = NULL;
    }
    if(plugin->codec.codec == NULL) {
	plugin->codec.codec = codec;
	plugin->codec.codeclib = lib;
	lib = NULL;
    }

    /* Cleanup */
    if(plugin->hdf5.hdf5lib == plugin->codec.codeclib)
	    plugin->codec.codeclib = NULL;
    if((stat=NCZ_plugin_save(h5id,plugin))) goto done;
    plugin = NULL;

done:
    if(lib) {
        (void)ncpsharedlibfree(lib);
    }
    if(plugin) NCZ_unload_plugin(plugin);
    return ZUNTRACEX(stat,"plug=%p",*plugp);
}

static int
NCZ_unload_plugin(NCZ_Plugin* plugin)
{
    ZTRACE(9,"plugin=%p",plugin);

    if(plugin) {
#ifdef DEBUGF
fprintf(stderr,"unload: %s\n",
	(plugin->hdf5.hdf5lib?plugin->hdf5.hdf5lib->path
			     : (plugin->codec.codeclib?plugin->codec.codeclib->path:"null")));
#endif
	if(plugin->codec.codec && plugin->codec.codec->NCZ_codec_finalize)
		plugin->codec.codec->NCZ_codec_finalize();
        if(plugin->hdf5.filter != NULL) loaded_plugins[plugin->hdf5.filter->id] = NULL;
	if(plugin->hdf5.hdf5lib != NULL) (void)ncpsharedlibfree(plugin->hdf5.hdf5lib);
	if(plugin->codec.codeclib != NULL) (void)ncpsharedlibfree(plugin->codec.codeclib);
	memset(plugin,0,sizeof(NCZ_Plugin));
	free(plugin);
    }
    return ZUNTRACE(NC_NOERR);
}

/**************************************************/
/* _Codecs attribute */

int
NCZ_codec_attr(const NC_VAR_INFO_T* var, size_t* lenp, void* data)
{
    int i,stat = NC_NOERR;
    size_t len;
    char* contents = NULL;
    NCbytes* buf = NULL;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    ZTRACE(6,"var=%s",var->hdr.name);
    if(nclistlength(zvar->codecs) == 0) {stat = NC_ENOTATT; goto done;}
    buf = ncbytesnew(); ncbytessetalloc(buf,1024);
    ncbytescat(buf,"[");
    for(i=0;i<nclistlength(zvar->codecs);i++) {
	char* text = NULL;
       	NCZ_Codec* spec = nclistget(zvar->codecs,i);
        if(i > 0) ncbytescat(buf,",");
	if((stat = NCJunparse(spec->codec,0,&text))) goto done;
	ncbytescat(buf,text);
	nullfree(text);
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

/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file @internal Internal functions for filters
 *
 * This file contains functions internal to the netcdf4 library. None of
 * the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4 filters
 *
 * @author Dennis Heimbigner
 */

#include "config.h"
#include <stdlib.h>
#include "zincludes.h"

/* Mnemonic */
#define FILTERACTIVE 1

/* WARNING: GLOBAL VARIABLE */

#ifdef ENABLE_CLIENT_FILTERS
/* Define list of registered filters */
static NClist* NCZ_registeredfilters = NULL; /** List<NC_FILTER_CLIENT_ZARR*> */

/**************************************************/
/* Filter registration support */

static int
filterlookup(conbst char* id)
{
    int i;
    if(NCZ_registeredfilters == NULL)
	NCZ_registeredfilters = nclistnew();
    for(i=0;i<nclistlength(NCZ_registeredfilters);i++) {
	NCX_FILTER_CLIENT* x = nclistget(NCZ_registeredfilters,i);
	if(x != NULL && x->id == id) return i; /* return position */
    }
    return -1;
}

static void
reclaiminfo(NCX_FILTER_CLIENT* info)
{
    nullfree(info);
}

static int
filterremove(int pos)
{
    NCX_FILTER_CLIENT* info = NULL;
    if(NCZ_registeredfilters == NULL)
	return THROW(NC_EINVAL);
    if(pos < 0 || pos >= nclistlength(NCZ_registeredfilters))
	return THROW(NC_EINVAL);
    info = nclistget(NCZ_registeredfilters,pos);
    reclaiminfo(info);
    nclistremove(NCZ_registeredfilters,pos);
    return NC_NOERR;
}

static NCX_FILTER_CLIENT*
dupfilterinfo(NCX_FILTER_CLIENT* info)
{
    NCX_FILTER_CLIENT* dup = NULL;
    if(info == NULL) goto fail;
    if((dup = calloc(1,sizeof(NCX_FILTER_CLIENT))) == NULL) goto fail;
    *dup = *info;
    return dup;
fail:
    reclaiminfo(dup);
    return NULL;
}

int
NCZ_global_filter_action(int op, unsigned int id, NC_FILTER_OBJ_HDF5* infop)
{
    int stat = NC_NOERR;
    H5Z_class2_t* h5filterinfo = NULL;
    herr_t herr;
    int pos = -1;
    NCX_FILTER_CLIENT* dup = NULL;
    NCX_FILTER_CLIENT* elem = NULL;
    NCX_FILTER_CLIENT ncf;

    switch (op) {
    case NCFILTER_CLIENT_REG: /* Ignore id argument */
        if(infop == NULL) {stat = NC_EINVAL; goto done;}
	assert(NC_FILTER_FORMAT_HDF5 == infop->hdr.format);
	assert(NC_FILTER_SORT_CLIENT == infop->sort);
	elem = (NCX_FILTER_CLIENT*)&infop->u.client;
        h5filterinfo = elem->info;
        /* Another sanity check */
        if(id != h5filterinfo->id)
	    {stat = NC_EINVAL; goto done;}
	/* See if this filter is already defined */
	if((pos = filterlookup(id)) >= 0)
	    {stat = NC_ENAMEINUSE; goto done;} /* Already defined */
	if((herr = H5Zregister(h5filterinfo)) < 0)
	    {stat = NC_EFILTER; goto done;}
	/* Save a copy of the passed in info */
	ncf.id = id;
	ncf.info = elem->info;
	if((dup=dupfilterinfo(&ncf)) == NULL)
	    {stat = NC_ENOMEM; goto done;}		
	nclistpush(NCZ_registeredfilters,dup);	
	break;
    case NCFILTER_CLIENT_UNREG:
	if(id <= 0)
	    {stat = NC_EFILTER; goto done;}
	/* See if this filter is already defined */
	if((pos = filterlookup(id)) < 0)
	    {stat = NC_ENOFILTER; goto done;} /* Not defined */
	if((herr = H5Zunregister(id)) < 0)
	    {stat = NC_EFILTER; goto done;}
	if((stat=filterremove(pos))) goto done;
	break;
    case NCFILTER_CLIENT_INQ:
	if(infop == NULL) goto done;
        /* Look up the id in our local table */
   	if((pos = filterlookup(id)) < 0)
	    {stat = NC_ENOFILTER; goto done;} /* Not defined */
        elem = (NCX_FILTER_CLIENT*)nclistget(NCZ_registeredfilters,pos);
	if(elem == NULL) {stat = NC_EINTERNAL; goto done;}
	if(infop != NULL) {
	    infop->u.client = *elem;
	}
	break;
    default:
	{stat = NC_EINTERNAL; goto done;}	
    }
done:
    return THROW(stat);
} 
#endif /*ENABLE_CLIENT_FILTERS*/

/**************************************************/

int
NCZ_addfilter(NC_VAR_INFO_T* var, int active, const char* id, const char* inparams)
{
    int stat = NC_NOERR;
    NCX_FILTER_SPEC* fi = NULL;

    if(var->filters == NULL) {
	if((var->filters = nclistnew())==NULL) return THROW(NC_ENOMEM);
    }

    if(id == NULL || inparams == NULL)
        return THROW(NC_EINVAL);
    
    if((fi = calloc(1,sizeof(NC_FILTER_SPEC_HDF5))) == NULL)
    	return THROW(NC_ENOMEM);

    fi->active = active;
    fi->filterid = strdup(id);
    fi->params = strdup(inparams);
    nclistpush(var->filters,fi);
    return THROW(stat);
}

/**
 * @internal Define filter settings. Called by nc_def_var_filter().
 *
 * @param ncid File ID.
 * @param varid Variable ID.
 * @param id Filter ID
 * @param nparams Number of parameters for filter.
 * @param parms Filter parameters.
 *
 * @returns ::NC_NOERR for success
 * @returns ::NC_EBADID Bad ncid.
 * @returns ::NC_ENOTVAR Invalid variable ID.
 * @returns ::NC_ENOTNCZ Attempting netcdf-4 operation on file that is
 * not netCDF-4/HDF5.
 * @returns ::NC_ELATEDEF Too late to change settings for this variable.
 * @returns ::NC_EFILTER Filter error.
 * @returns ::NC_EINVAL Invalid input
 * @author Dennis Heimbigner
 */
int
NCZ_filter_actions(int ncid, int varid, int op, NC_Filterobject* args)
{
    int stat = NC_NOERR;
    NC_GRP_INFO_T *grp = NULL;
    NC_FILE_INFO_T *h5 = NULL;
    NC_VAR_INFO_T *var = NULL;
    NCX_FILTER_OBJ* obj = (NCX_FILTER_OBJ*)args;
    size_t nfilters = 0;
    size_t* nfiltersp = 0;
    char** filterids = NULL;
    char* id = NULL;

    LOG((2, "%s: ncid 0x%x varid %d op=%d", __func__, ncid, varid, op));

    if(args == NULL) return THROW(NC_EINVAL);
    if(args->format != NC_FILTER_FORMAT_HDF5) return THROW(NC_EFILTER);

    /* Find info for this file and group and var, and set pointer to each. */
    if ((stat = nc4_find_grp_h5_var(ncid, varid, &h5, &grp, &var)))
	return THROW(stat);

    assert(h5 && var && var->hdr.id == varid);

    nfilters = nclistlength(var->filters);

    switch (op) {
    case NCFILTER_DEF: {
        NCX_FILTER_SPEC* spec;
        if(obj->sort != NC_FILTER_SORT_SPEC) return THROW(NC_EFILTER);
        /* If the dataset has already been created, then it is too
         * late to set all the extra stuff. */
        if (!(h5->flags & NC_INDEF)) return THROW(NC_EINDEFINE);
        if (!var->ndims) return NC_NOERR; /* For scalars, ignore */
        if (var->created)
             return THROW(NC_ELATEDEF);
        /* Filter => chunking */
        var->storage = NC_CHUNKED;
        /* Determine default chunksizes for this variable unless already specified */
        if(var->chunksizes && !var->chunksizes[0]) {
	    /* Should this throw error? */
	    if((stat = ncz_find_default_chunksizes2(grp, var)))
	        goto done;
        }
        spec = &obj->u.spec;
	if((stat = NCZ_addfilter(var,!FILTERACTIVE,spec->filterid,spec->params)))
            goto done;
    } break;
    case NCFILTER_INQ:
	return THROW(NC_ENOTBUILT);
    case NCFILTER_FILTERIDS: {
        if(obj->sort != NC_FILTER_SORT_IDS) return THROW(NC_EFILTER);
	nfiltersp = &obj->u.ids.nfilters;
	filterids = obj->u.ids.filterids;
        if(nfiltersp) *nfiltersp = nfilters;
	if(filterids) filterids[0] = NULL;
        if(nfilters > 0 && filterids != NULL) {
	    int k;
	    for(k=0;k<nfilters;k++) {
		NCX_FILTER_SPEC* f = (NCX_FILTER_SPEC*)nclistget(var->filters,k);
		filterids[k] = f->filterid;
	    }
	}
	} break;
    case NCFILTER_INFO: {
	int k,found;
        if(obj->sort != NC_FILTER_SORT_SPEC) return THROW(NC_EFILTER);
	id = obj->u.spec.filterid;
        for(found=0,k=0;k<nfilters;k++) {
	    NCX_FILTER_SPEC* f = (NCX_FILTER_SPEC*)nclistget(var->filters,k);
	    if(strcmp(f->filterid,id)==0) {
		if(obj->u.spec.params != NULL && f->params != NULL)
		    f->params = strdup(obj->u.spec.params);
		found = 1;
		break;
	    }
	}
	if(!found) {stat = NC_ENOFILTER; goto done;}
	} break;
    case NCFILTER_REMOVE: {
	int k;
        if (!(h5->flags & NC_INDEF)) return THROW(NC_EINDEFINE);
        if(obj->sort != NC_FILTER_SORT_SPEC) return THROW(NC_EFILTER);
	id = obj->u.spec.filterid;
	/* Walk backwards */
        for(k=nfilters-1;k>=0;k--) {
	    NCX_FILTER_SPEC* f = (NCX_FILTER_SPEC*)nclistget(var->filters,k);
	    if(strcmp(f->filterid,id)==0) {
		if(f->active) {
		    /* Remove from variable */
#ifdef LOOK
		    if((stat = NCZ_nczarr_remove_filter(var,id))) {stat = NC_ENOFILTER; goto done;}
#endif
		}
		nclistremove(var->filters,k);
		NCX_freefilterspec(f);
	    }
	}
	} break;
    default:
	{stat = NC_EINTERNAL; goto done;}	
    }

done:
    return THROW(stat);
}

void
NCX_freefilterspec(NCX_FILTER_SPEC* f)
{
    if(f) {
        if(f->params != NULL) {free(f->params);}
        if(f->filterid != NULL) {free(f->filterid);}
	free(f);
    }
}

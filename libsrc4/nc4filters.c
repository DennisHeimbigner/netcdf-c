/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file
 * @internal
 * Internal netcdf-4 filter functions.
 *
 * This file contains functions internal to the netcdf4 library.
 * None of the functions in this file are exposed in the exetnal API. These
 * functions all relate to the manipulation of netcdf-4's var->filters list
 *
 * @author Dennis Heimbigner
 */
#include "config.h"
#include "netcdf.h"
#include "nc4internal.h"
#include "netcdf_filter.h"
#include "ncfilter.h"

int
NC4_filterx_freelist(NC_VAR_INFO_T* var)
{
    int i, stat=NC_NOERR;

    if(var->filters == NULL) goto done;
    /* Free the filter list */
    for(i=0;i<nclistlength(var->filters);i++) {
	NC_FILTERX_SPEC* spec = nclistget(var->filters,i);
	if((stat = NC4_filterx_free(spec))) return stat;
    }
    nclistfree(var->filters);
    var->filters = NULL;
done:
    return stat;
}

int
NC4_filterx_free(NC_FILTERX_SPEC* spec)
{
    if(spec == NULL) goto done;
    nullfree(spec->filterid);
    if(spec->params) {
	NC_filterx_freestringvec(spec->nparams,spec->params);
    }
    free(spec);
done:
    return NC_NOERR;
}

int
NC4_filterx_add(NC_VAR_INFO_T* var, int active, NC_FILTERX_SPEC* spec)
{
    int stat = NC_NOERR;
    NC_FILTERX_SPEC* fi = NULL;

    if(var->filters == NULL) {
	if((var->filters = nclistnew())==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }

    if(spec->nparams > 0 && spec->params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if((fi = calloc(1,sizeof(NC_FILTERX_SPEC))) == NULL)
    	{stat = NC_ENOMEM; goto done;}

    fi->active = active;
    fi->nparams = spec->nparams;
    if((fi->filterid = strdup(spec->filterid)) == NULL)
        {stat = NC_ENOMEM; goto done;}
    if(spec->params != NULL) {
	if((stat = NC_filterx_copy(spec->nparams,(const char**)spec->params,&fi->params))) goto done;
    }
    nclistpush(var->filters,fi);
    fi = NULL;
done:
    NC4_filterx_free(fi);    
    return stat;
}

int
NC4_filterx_remove(NC_VAR_INFO_T* var, const char*xid)
{
    int k;
    /* Walk backwards */
    for(k=nclistlength(var->filters)-1;k>=0;k--) {
	NC_FILTERX_SPEC* f = (NC_FILTERX_SPEC*)nclistget(var->filters,k);
        if(strcasecmp(f->filterid,xid)==0) {
	    if(f->active) {
		/* Remove from variable */
		nclistremove(var->filters,k);
		return NC_NOERR;
	    }
	}
    }
    return NC_ENOFILTER;
}

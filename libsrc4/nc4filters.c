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

static void
printfilter1(NC_FILTERX_SPEC* nfs)
{
    int i;
    if(nfs == NULL) {
	fprintf(stderr,"{null}");
	return;
    }
    fprintf(stderr,"{%s,(%d)",nfs->filterid,(int)nfs->nparams);
    for(i=0;i<nfs->nparams;i++) {
      fprintf(stderr," %s",nfs->params[i]);
    }
    fprintf(stderr,"}");
}

static void
printfilter(NC_FILTERX_SPEC* nfs, const char* tag)
{
    fprintf(stderr,"%s: ",tag);
    printfilter1(nfs);
    fprintf(stderr,"\n");
}

static void
printfilterlist(NC_VAR_INFO_T* var, const char* tag)
{
    int i;
    const char* name;
    if(var == NULL) name = "null";
    else if(var->hdr.name == NULL) name = "?";
    else name = var->hdr.name;
    fprintf(stderr,"%s: var=%s filters=",tag,name);
    if(var != NULL) {
        for(i=0;i<nclistlength(var->filters);i++) {
	    NC_FILTERX_SPEC* nfs = nclistget(var->filters,i);
	    fprintf(stderr,"[%d]",i);
	    printfilter1(nfs);
	}
    }
    fprintf(stderr,"\n");
}

int
NC4_filterx_freelist(NC_VAR_INFO_T* var)
{
    int i, stat=NC_NOERR;

    if(var->filters == NULL) goto done;
printfilterlist(var,"free: before");
    /* Free the filter list backward */
    for(i=nclistlength(var->filters)-1;i>=0;i--) {
	NC_FILTERX_SPEC* spec = nclistremove(var->filters,i);
	if((stat = NC4_filterx_free(spec))) return stat;
    }
printfilterlist(var,"free: after");
    nclistfree(var->filters);
    var->filters = NULL;
done:
    return stat;
}

int
NC4_filterx_free(NC_FILTERX_SPEC* spec)
{
    if(spec == NULL) goto done;
printfilter(spec,"free");
    nullfree(spec->filterid);
    if(spec->params) {
	NC_filterx_freestringvec(spec->nparams,spec->params);
    }
    free(spec);
done:
    return NC_NOERR;
}

int
NC4_filterx_lookup(NC_VAR_INFO_T* var, const char* id, NC_FILTERX_SPEC** specp)
{
    int i;
    if(var->filters == NULL) {
	if((var->filters = nclistnew())==NULL)
	    return NC_ENOMEM;
    }
    for(i=0;i<nclistlength(var->filters);i++) {
	NC_FILTERX_SPEC* spec = nclistget(var->filters,i);
	if(strcasecmp(id,spec->filterid)==0) {
	    if(specp) *specp = spec;
	    return NC_NOERR;
	}
    }
    return NC_ENOFILTER;
}

int
NC4_filterx_add(NC_VAR_INFO_T* var, int active, const char* id, int nparams, const char** params)
{
    int stat = NC_NOERR;
    NC_FILTERX_SPEC* fi = NULL;
    int olddef = 0; /* 1=>already defined */

    if(nparams > 0 && params == NULL)
	{stat = NC_EINVAL; goto done;}
    
    if((stat=NC4_filterx_lookup(var,id,&fi))==NC_NOERR) {
        /* already exists */
	NC_filterx_freestringvec(fi->nparams,fi->params); /* reclaim old values */
	fi->params = NULL;
	olddef = 1;	
    } else {
        if((fi = calloc(1,sizeof(NC_FILTERX_SPEC))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        if((fi->filterid = strdup(id)) == NULL)
            {stat = NC_ENOMEM; goto done;}
	olddef = 0;
    }    
    fi->active = active;
    fi->nparams = nparams;
    if(params != NULL) {
	if((stat = NC_filterx_copy(nparams,(const char**)params,&fi->params))) goto done;
    }
    if(!olddef) nclistpush(var->filters,fi);
    fi = NULL;
printfilterlist(var,"add");

done:
    if(fi) NC4_filterx_free(fi);    
    return stat;
}

int
NC4_filterx_remove(NC_VAR_INFO_T* var, const char* xid)
{
    int k;
    /* Walk backwards */
    for(k=nclistlength(var->filters)-1;k>=0;k--) {
	NC_FILTERX_SPEC* f = (NC_FILTERX_SPEC*)nclistget(var->filters,k);
        if(strcasecmp(f->filterid,xid)==0) {
	    if(!f->active) {
		/* Remove from variable */
		nclistremove(var->filters,k);
printfilterlist(var,"remove");
fprintf(stderr,"\tid=%s\n",xid);
		return NC_NOERR;
	    }
	}
    }
    return NC_ENOFILTER;
}

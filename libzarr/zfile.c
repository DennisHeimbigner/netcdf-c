/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"

/* Implementation table */
static struct NCZM_PROTOLIST {
    const char* prototag; /* value of "protocol=" */
    NCZM_IMPL impl;
} protolist[] = {
{"netcdf4", NCZM_NC4},
{NULL,NCZM_UNDEF}
};


/*Forward*/
static NCZM_IMPL computemapimpl(NCURI* uri, int cmode);

int
NCZ_open(const char *path, int omode,
	    int basepe, size_t *chunksizehintp,
	    void* parameters, const NC_Dispatch* dispatch, NC* nc)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO* zinfo = NULL;
    size64_t flags = 0;
    NC_FILE_INFO_T* file = NULL;
    NCURI* uri = NULL;

    ZTRACE();

    if(path == NULL)
	return THROW(NC_EDAPURL);

    if(dispatch == NULL)
	return THROW(NC_EINTERNAL);

    /* Setup our per-dataset state */

    /* Record and fill the NC_FILE_INFO_T object; will also
       create the root group */
    if((stat=nc4_nc4f_list_add(nc, nc->path, nc->mode))) goto done;
    file = (NC_FILE_INFO_T*)nc->dispatchdata;

    nc->int_ncid = nc__pseudofd(); /* create a unique id */
    nc->dispatch = dispatch;

    /* Parse url and params */
    if(ncuriparse(nc->path,&uri))
	{stat = NC_EDAPURL; goto done;}

    /* Annotate the NC_FILE_INFO_T object */
    if((zinfo=malloc(sizeof(NCZ_FILE_INFO))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Cross link */
    zinfo->dataset = file;
    file->format_file_info = zinfo;

    /* initialize map handle*/
    if((stat = nczmap_open(path,omode,flags,parameters,&zinfo->map)))
	goto done;

    /* Load the Zarr/NCZarr meta-data */
    if((stat=NCZ_open_dataset(zinfo))) goto done;

done:
    ncurifree(uri);
    return stat;
}

int
NCZ_create(const char *path, int cmode,
	  size_t initialsz, int basepe, size_t *chunksizehintp,
	  void* parameters, const NC_Dispatch* dispatch, NC* nc)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO* zinfo = NULL;
    size64_t flags = 0;
    NC_FILE_INFO_T* file = NULL;
    NCURI* uri = NULL;
    NCZM_IMPL mapimpl = NCZM_UNDEF;

    ZTRACE();

    if(path == NULL)
	return THROW(NC_EDAPURL);

    if(dispatch == NULL)
	return THROW(NC_EINTERNAL);

    /* Parse url and params */
    if(ncuriparse(nc->path,&uri))
	{stat = NC_EDAPURL; goto done;}

    /* Figure out the map implementation */
    if((mapimpl = computemapimpl(uri,cmode)) == NCZM_UNDEF)
	mapimpl = NCZM_DEFAULT;

    /* Setup our per-dataset state */

    /* Record and fill the NC_FILE_INFO_T object. */
    if((stat=nc4_nc4f_list_add(nc, nc->path, nc->mode))) goto done;
    file = (NC_FILE_INFO_T*)nc->dispatchdata;

    nc->int_ncid = nc__pseudofd(); /* create a unique id */
    nc->dispatch = dispatch;

    /* Annotate the NC_FILE_INFO_T object */
    if((zinfo=malloc(sizeof(NCZ_FILE_INFO))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Cross link */
    zinfo->dataset = file;
    file->format_file_info = zinfo;

    /* Parse url and params */
    if(ncuriparse(nc->path,&uri))
	{stat = NC_EDAPURL; goto done;}

    /* initialize map handle*/
    if((stat = nczmap_create(mapimpl,path,cmode,flags,parameters,&zinfo->map)))
	goto done;

    /* Record and fill the NC_FILE_INFO_T object. */
    if((stat=nc4_nc4f_list_add(nc, nc->path, nc->mode))) goto done;
    file = (NC_FILE_INFO_T*)nc->dispatchdata;

    /* create the Zarr/NCZarr meta-data */
    if((stat=NCZ_create_dataset(zinfo))) goto done;

done:
    ncurifree(uri);
    return stat;
}

/**************************************************/
/* Utilities */

static NCZM_IMPL
computemapimpl(NCURI* uri, int cmode)
{
    if(uri == NULL) {
	goto done;
    } else if(strcasecmp(uri->protocol,"file")==0) {
	/* It will be one of the file-based implementations */
	/* See what is in the fragment list */
	const char* proto = ncurilookup(uri,"protocol");
	if(proto == NULL)
	    proto = ncurilookup(uri,"proto");
	if(proto == NULL) goto done; 
	struct NCZM_PROTOLIST* p = protolist;
	for(;p->prototag;p++) {
	    if(strcmp(p->prototag,proto)==0)
		return p->impl;
	}
	goto done; /* no match */	
    } else if(strcasecmp(uri->protocol,"s3")==0) {
	ncurisetprotocol(uri,"https");
	return NCZM_S3;
    }
done:
    return NCZM_UNDEF; /* could not determine protocol */
}

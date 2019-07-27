/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"

int
NCZ_open(const char *path, int omode,
	    int basepe, size_t *chunksizehintp,
	    void* parameters, const NC_Dispatch* dispatch, NC* nc)
{
    int stat = NC_NOERR;
    NCZINFO* zinfo = NULL;
    size64_t flags = 0;

    ZTRACE();

    if(path == NULL)
	return THROW(NC_EDAPURL);

    if(dispatch == NULL)
	return THROW(NC_EINTERNAL);

    /* Setup our per-dataset state */

    zinfo = (NCZINFO*)calloc(1,sizeof(NCZINFO));
    if(zinfo == NULL) {stat = NC_ENOMEM; goto done;}

    nc->dispatchdata = zinfo;
    nc->int_ncid = nc__pseudofd(); /* create a unique id */
    zinfo->controller = (NC*)nc;
    nc->dispatch = dispatch;

    /* Parse url and params */
    if(ncuriparse(nc->path,&zinfo->uri))
	{stat = NC_EDAPURL; goto done;}

    /* initialize map handle*/
    if((stat = nczmap_open(path,omode,flags,parameters,&zinfo->map)))
	goto done;

    /* Load the Zarr/NCZarr meta-data */
    if((stat=NCZ_loadmetadata(zinfo))) goto done;

done:
    return stat;
}

int
NCZ_create(const char *path, int cmode,
	  size_t initialsz, int basepe, size_t *chunksizehintp,
	  void* parameters, const NC_Dispatch* dispatch, NC* nc)
{
    int stat = NC_NOERR;
    NCZINFO* zinfo = NULL;
    size64_t flags = 0;

    ZTRACE();

    if(path == NULL)
	return THROW(NC_EDAPURL);

    if(dispatch == NULL)
	return THROW(NC_EINTERNAL);

    /* Setup our per-dataset state */

    zinfo = (NCZINFO*)calloc(1,sizeof(NCZINFO));
    if(zinfo == NULL) {stat = NC_ENOMEM; goto done;}

    nc->dispatchdata = zinfo;
    nc->int_ncid = nc__pseudofd(); /* create a unique id */
    zinfo->controller = (NC*)nc;
    nc->dispatch = dispatch;

    /* Parse url and params */
    if(ncuriparse(nc->path,&zinfo->uri))
	{stat = NC_EDAPURL; goto done;}

    /* initialize map handle*/
    if((stat = nczmap_create(NCZM_NC4,path,cmode,flags,parameters,&zinfo->map)))
	goto done;

done:
    return stat;
}

/**************************************************/

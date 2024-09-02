/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file @internal netcdf-4 functions for the plugin list.
 *
 * @author Dennis Heimbigner
 */

#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include "netcdf.h"
#include "ncbytes.h"
#include "hdf5internal.h"
#include "hdf5debug.h"
#include "ncplugins.h"

#undef TPLUGINS

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf hdf5 plugin path functions.
 *
 * @author Dennis Heimbigner
 */
/**************************************************/

EXTERNL int
NC4_hdf5_plugin_path_sync(int formatx)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;  
    struct NCglobalstate* gs = NC_getglobalstate();

    assert(formatx == NC_FORMATX_NC_HDF5);
    {
	/* Clear the current path list */
        if((hstat = H5PLsize(&npaths))<0) goto done;
        if(npaths > 0) {
	    size_t i;
	    for(i=0;i<npaths;i++) {
		/* Always remove the first element */
		if((hstat = H5PLremove(0))<0) {stat = NC_EINVAL; goto done;}
	    }
	}
    }

    if(nclistlength(gs->pluginpaths) > 0) {
	size_t i;
	/* Insert the new path list */
#ifdef TPLUGINS
        if((hstat = H5PLsize(&npaths))<0) goto done;
	assert(npaths == 0)
#endif
        for(i=0;i<nclistlength(gs->pluginpaths);i++) {
	    const char* dir = (const char*)nclistget(gs->pluginpaths,i);
	    /* Always append */
	    if((hstat = H5PLappend(dir))<0)
		{stat = NC_EINVAL; goto done;}
	}
    }

done:
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

int
NC4_hdf5_plugin_path_initialize(void)
{
    return NC_NOERR;
}

int
NC4_hdf5_plugin_path_finalize(void)
{
    return NC_NOERR;
}

/**************************************************/
/* Debug printer for HDF5 plugin paths */

EXTERNL NCbytes* NC_pluginpaths_buf;
#define buf NC_pluginpaths_buf
const char*
NC4_hdf5_plugin_path_tostring(void)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;
    char* dir = NULL;

    if(buf == NULL) buf = ncbytesnew();
    ncbytesclear(buf);

    if((hstat = H5PLsize(&npaths))<0) {stat = NC_EINVAL; goto done;}
    if(npaths > 0) {
	ssize_t dirlen = 0;
	unsigned i;
	for(i=0;i<npaths;i++) {
	    dirlen = H5PLget(i,NULL,0);
	    if(dirlen < 0) {stat = NC_EINVAL; goto done;}
	    if((dir = (char*)malloc(1+(size_t)dirlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
	    /* returned dirlen does not include the nul terminator, but the length argument must include it */
	    dirlen = H5PLget(i,dir,(size_t)(dirlen+1));
	    dir[dirlen] = '\0';
	    if(i > 0) ncbytescat(buf,";");
	    ncbytescat(buf,dir);
	    nullfree(dir); dir = NULL;
	}
    }

done:
    nullfree(dir);
    if(stat != NC_NOERR) ncbytesclear(buf);
    ncbytesnull(buf);
    return ncbytescontents(buf);
}
#undef NC_pluginpaths_buf

/**************************************************/
#ifdef TPLUGINS

static void
printplugin1(struct NC_HDF5_Plugin* nfs)
{
    int i;
    if(nfs == NULL) {
	fprintf(stderr,"{null}");
	return;
    }
    fprintf(stderr,"{%u,(%u)",nfs->pluginid,(int)nfs->nparams);
    for(i=0;i<nfs->nparams;i++) {
      fprintf(stderr," %s",nfs->params[i]);
    }
    fprintf(stderr,"}");
}

static void
printplugin(struct NC_HDF5_Plugin* nfs, const char* tag, int line)
{
    fprintf(stderr,"%s: line=%d: ",tag,line);
    printplugin1(nfs);
    fprintf(stderr,"\n");
}

static void
printpluginlist(NC_VAR_INFO_T* var, const char* tag, int line)
{
    int i;
    const char* name;
    if(var == NULL) name = "null";
    else if(var->hdr.name == NULL) name = "?";
    else name = var->hdr.name;
    fprintf(stderr,"%s: line=%d: var=%s plugins=",tag,line,name);
    if(var != NULL) {
	NClist* plugins = (NClist*)var->plugins;
        for(i=0;i<nclistlength(plugins);i++) {
	    struct NC_HDF5_Plugin* nfs = (struct NC_HDF5_Plugin*)nclistget(plugins,i);
	    fprintf(stderr,"[%d]",i);
	    printplugin1(nfs);
	}
    }
    fprintf(stderr,"\n");
}
#endif /*TPLUGINS*/

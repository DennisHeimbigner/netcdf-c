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
#include "hdf5internal.h"
#include "hdf5debug.h"
#include "netcdf.h"
#include "netcdf_filter.h"

#undef TPLUGINS

EXTERNL int nc_parse_plugin_pathlist(const char* path0, NClist* dirlist);

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf hdf5 plugin path functions.
 *
 * @author Dennis Heimbigner
 */
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

#define PRINTPLUGIN(nfs, tag) printplugin(nfs,tag,__LINE__)
#define PRINTPLUGINLIST(var,tag) printpluginlist(var,tag,__LINE__)
#else
#define PRINTPLUGIN(nfs, tag)
#define PRINTPLUGINLIST(var,tag)
#endif /*TPLUGINS*/

/**
 * Return the current sequence of directories in the internal plugin path list.
 *
 * @param ncid ID ignored
 * @param npaths return the number of paths in the path list
 * @param pathlist return the sequence of directies in the path list
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.
 * The first time with npaths not NULL and pathlist set to NULL
 *     to get the size of the path list.
 * The second time with pathlist not NULL to get the actual sequence of paths.
*/

EXTERNL int
NC4_hdf5_plugin_path_list(int ncid, size_t* npathsp, char** pathlist)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;
    char* buf = NULL;

    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(npathsp) *npathsp = npaths;
    if(npaths > 0 && pathlist != NULL) {
	ssize_t pathlen = 0;
	unsigned i;
	for(i=0;i<npaths;i++) {
	    pathlen = H5PLget(i,NULL,0);
	    if(pathlen < 0) {stat = NC_EINVAL; goto done;}
	    if((buf = (char*)malloc(1+(size_t)pathlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
	    pathlen = H5PLget(i,buf,(size_t)pathlen);
	    buf[pathlen] = '\0';
	    pathlist[i] = buf; buf = NULL;
	}
    }
done:
    nullfree(buf);
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

/**
 * Append a directory to the end of the current internal path list.
 *
 * @param ncid ID ignored
 * @param dir directory to append.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
NC4_hdf5_plugin_path_append(int ncid, const char* path)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    if((hstat = H5PLappend(path))<0) goto done;
done:
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

/**
 * Prepend a directory to the front of the current internal path list.
 *
 * @param ncid ID ignored
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
NC4_hdf5_plugin_path_prepend(int ncid, const char* path)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    if((hstat = H5PLprepend(path))<0) goto done;
done:
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

/**
 * Remove all occurrences of a directory from the internal path sequence
 *
 * @param ncid ID ignored
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
NC4_hdf5_plugin_path_remove(int ncid, const char* dir)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;
    char* buf = NULL;

    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(npaths > 0 && dir != NULL) {
	ssize_t pathlen = 0;
	unsigned i;
	/* Walk backward to avoid more complex logic */
	for(i=npaths-1;i>=0;i--) {
	    pathlen = H5PLget(i,NULL,0);
	    if(pathlen < 0) {stat = NC_EINVAL; goto done;}
	    if((buf = (char*)malloc(1+(size_t)pathlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
	    if((pathlen = H5PLget(i,buf,(size_t)pathlen))<0)
		{stat = NC_EINVAL; goto done;}
	    buf[pathlen] = '\0';
	    if(strcmp(buf,dir)==0) {
	        if((hstat = H5PLremove(i))<0) {stat = NC_EINVAL; goto done;}
	    }
	}
    }
done:
    nullfree(buf);
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories
 * parsed from the paths argument.
 * The path argument has the following syntax:
 *    paths := <empty> | dirlist
 *    dirlist := dir | dirlist separator dir
 *    separator := ';' | ':'
 *    dir := <OS specific directory path>
 * Note that the ':' separator is not legal on Windows machines.
 * The ';' separator is legal on all machines.
 *
 * @param ncid ID ignored
 * @param paths to overwrite the current internal path list
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
 *
 * In effect, this is sort of bulk loader for the path list.
*/

EXTERNL int
NC4_hdf5_plugin_path_load(int ncid, const char* paths)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    NClist* newpaths = nclistnew();
    unsigned npaths = 0;

    /* Parse the paths */
    if((stat = nc_parse_plugin_pathlist(paths,newpaths))) goto done;

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

    if(nclistlength(newpaths) > 0) {
	size_t i;
	/* Insert the new path list */
#ifdef TPLUGINS
        if((hstat = H5PLsize(&npaths))<0) goto done;
	assert(npaths == 0)
#endif
        for(i=0;i<nclistlength(newpaths);i++) {
	    /* Always append */
	    if((hstat = H5PLappend(nclistget(newpaths,i)))<0)
		{stat = NC_EINVAL; goto done;}
	}
    }

done:
    nclistfreeall(newpaths);
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

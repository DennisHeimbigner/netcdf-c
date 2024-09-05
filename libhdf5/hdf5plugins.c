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
/* Forward */

static int NC4_hdf5_plugin_path_initialize(void** statep, const NClist* initialpaths);
static int NC4_hdf5_plugin_path_finalize(void** statep);
static int NC4_hdf5_plugin_path_getall(void* state, size_t* npathsp, char** pathlist);
static int NC4_hdf5_plugin_path_getith(void* state, size_t index, char** entryp);
static int NC4_hdf5_plugin_path_load(void* state, const char* paths);
static int NC4_hdf5_plugin_path_append(void* state, const char* path);
static int NC4_hdf5_plugin_path_prepend(void* state, const char* path);
static int NC4_hdf5_plugin_path_remove(void* state, const char* dir);

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf hdf5 plugin path functions.
 *
 * @author Dennis Heimbigner
 */
/**************************************************/
/* The HDF5 Plugin Path Dispatch table and functions */

const NC_PluginPathDispatch NC4_hdf5_pluginpathtable = {
    NC_FORMATX_NC_HDF5,
    NC_PLUGINPATH_DISPATCH_VERSION,
    NC4_hdf5_plugin_path_initialize,
    NC4_hdf5_plugin_path_finalize,
    NC4_hdf5_plugin_path_getall,
    NC4_hdf5_plugin_path_getith,
    NC4_hdf5_plugin_path_load,
    NC4_hdf5_plugin_path_append,
    NC4_hdf5_plugin_path_prepend,
    NC4_hdf5_plugin_path_remove
};

/**
 * This function is called as part of nc_initialize.
 * Its purpose is to initialize the plugin paths state.
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/
static int
NC4_hdf5_plugin_path_initialize(void** statep, const NClist* initialpaths)
{
    int stat = NC_NOERR;
    GlobalHDF5* g5 = NULL;

    NC_UNUSED(initialpaths); /* Let HDF5 do its own thing */
    
    assert(statep != NULL);
    if(*statep != NULL) goto done; /* already initialized */
    if((g5 = (GlobalHDF5*)calloc(1,sizeof(GlobalHDF5)))==NULL) {stat = NC_ENOMEM; goto done;}
    *statep = (void*)g5; g5 = NULL;
done:
    nullfree(g5);
    return THROW(stat);
}

/**
 * This function is called as part of nc_finalize()
 * Its purpose is to clean-up plugin path state.
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/
static int
NC4_hdf5_plugin_path_finalize(void** statep)
{
    int stat = NC_NOERR;
    GlobalHDF5* g5 = NULL;

    assert(statep != NULL);
    if(*statep == NULL) goto done; /* already finalized */
    g5 = (GlobalHDF5*)(*statep);
    *statep = NULL;

done:
    nullfree(g5);
    return THROW(stat);
}

/**
 * Return the current sequence of directories in the internal plugin path list.
 * Since this function does not modify the plugin path, it can be called at any time.
 * @param npaths return the number of paths in the path list
 * @param pathlist return the sequence of directies in the path list
 * @return NC_NOERR
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.
 * The first time with npaths not NULL and pathlist set to NULL
 *     to get the size of the path list.
 * The second time with pathlist not NULL to get the actual sequence of paths.
*/
static int
NC4_hdf5_plugin_path_getall(void* state, size_t* npathsp, char** pathlist)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;  
    char* dir = NULL;

    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(npathsp) *npathsp = npaths;
    if(npaths > 0 && pathlist != NULL) {
	unsigned i;
	ssize_t dirlen = 0;
	for(i=0;i<npaths;i++) {
	    /* Get length of the ith element */
	    if((dirlen=H5PLget(i,NULL,0))<0) {hstat = dirlen; goto done;}
	    /* Alloc space for the dir name */
	    if((dir = (char*)malloc((size_t)dirlen+1))==NULL) {stat = NC_ENOMEM; goto done;}
	    if((hstat=H5PLget(i,dir,(size_t)dirlen+1))<0) goto done;
	    dir[dirlen] = '\0';
	    pathlist[i] = dir; dir = NULL;
	}
    }
done:
    nullfree(dir);
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}

/**
 * Get ith directory the internal path sequence.
 * The index starts at 0 (zero).
 * Caller frees the returned string.
 * @param index of path to return
 * @entryp store copy of the index'th dir from the internal path or NULL if out of range.
 * @return NC_NOERR||NC_ERANGE if out of range
 * @author Dennis Heimbigner
*/

static int
NC4_hdf5_plugin_path_getith(void* state, size_t index, char** entryp)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;  
    char* dir = NULL;

    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(index >= (size_t)npaths) {stat = NC_EINVAL; goto done;}
    if(entryp) {
	ssize_t dirlen = 0;
	/* Get length of the index'th element */
	if((dirlen=H5PLget((unsigned)index,NULL,0))<0) {hstat = dirlen; goto done;}
	/* Alloc space for the dir name */
	if((dir = (char*)malloc((size_t)dirlen+1))==NULL) {stat = NC_ENOMEM; goto done;}
	if((hstat=H5PLget((unsigned)index,dir,(size_t)dirlen+1))<0) goto done;
	dir[dirlen] = '\0';
	if(entryp) {*entryp = dir; dir = NULL;}
    }
done:
    nullfree(dir);
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories
 * parsed from the paths argument.
 * In effect, this is sort of bulk loader for the path list.
 *
 * The path argument has the following syntax:
 *    paths := <empty> | dirlist
 *    dirlist := dir | dirlist separator dir
 *    separator := ';' | ':'
 *    dir := <OS specific directory path>
 * Note that the ':' separator is not legal on Windows machines.
 * The ';' separator is legal on all machines.
 * Using a paths argument of "" will clear the set of plugin paths.
 * @param paths to overwrite the current internal path list
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/
static int
NC4_hdf5_plugin_path_load(void* state, const char* paths)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;  
    NClist* newpaths = nclistnew();
    size_t npathsnew = 0;

    /* parse the path list */
    if((stat = NC_plugin_path_parse(paths,newpaths))) goto done;
    npathsnew = nclistlength(newpaths);
    
    /* Clear the current path list */
    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(npaths > 0) {
	unsigned i;
	for(i=0;i<npaths;i++) {
	    /* remove i'th element */
	    if((hstat=H5PLremove(i))<0) goto done;
	}
    }
    /* Insert the new path list */
    if(npathsnew > 0) {
	size_t i;
	for(i=0;i<nclistlength(newpaths);i++) {
	    const char* dir = (const char*)nclistget(newpaths,i);
	    if((hstat = H5PLappend(dir))<0) goto done;	    
	}
    }

done:
    nclistfreeall(newpaths);    
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}

/**
 * Append a directory to the end of the current internal path list.
 * @param dir directory to append.
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/

static int
NC4_hdf5_plugin_path_append(void* state, const char* path)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;

    if((hstat=H5PLappend(path))<0) goto done;
done:
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}

/**
 * Prepend a directory to the front of the current internal path list.
 * @param dir directory to prepend
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/

static int
NC4_hdf5_plugin_path_prepend(void* state, const char* path)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;

    if((hstat=H5PLprepend(path))<0) goto done;
done:
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}

/**
 * Remove all occurrences of a directory from the internal path sequence
 * @param dir directory to prepend
 * @return NC_NOERR
 * @author Dennis Heimbigner
*/

EXTERNL int
NC4_hdf5_plugin_path_remove(void* state, const char* template)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;  
    char* dir = NULL;

    if(template == NULL) goto done; /* ignore */
    if((hstat = H5PLsize(&npaths))<0) goto done;
    if(npaths > 0) {
	ssize_t dirlen = 0;
	unsigned i;
	/* Walk backwards */
	for(i=npaths;i-- > 0;) {
	   /* Get length of the i'th element */
	   if((dirlen=H5PLget((unsigned)i,NULL,0))<0) {hstat = dirlen; goto done;}
   	   /* Alloc space for the dir name */
	   if((dir = (char*)malloc((size_t)dirlen+1))==NULL) {stat = NC_ENOMEM; goto done;}
	    if((hstat=H5PLget((unsigned)i,dir,(size_t)dirlen+1))<0) goto done;
	    dir[dirlen] = '\0';
	    if(strcmp(dir,template)==0) {
		if((hstat=H5PLremove(i))<0) goto done;
	    }
	    free(dir); dir = NULL;
	}
    }
done:    
    nullfree(dir);
    if(stat == NC_NOERR && hstat < 0) stat = NC_EHDFERR;
    return THROW(stat);
}


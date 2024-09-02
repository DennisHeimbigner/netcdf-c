/**************************************************/
/* Global state plugin path implementation */

/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/**
 * @file
 * Functions for working with plugins. 
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "netcdf_filter.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "nclog.h"
#include "ncbytes.h"
#include "ncplugins.h"

/*
Unified plugin related code
*/
/**************************************************/
/* Plugin-path API */ 

/* list of environment variables to check for plugin roots */
#define PLUGIN_ENV "HDF5_PLUGIN_PATH"
#define PLUGIN_DIR_UNIX "/usr/local/hdf5/plugin"
#define PLUGIN_DIR_WIN "%s/hdf5/lib/plugin"
#define WIN32_ROOT_ENV "ALLUSERSPROFILE"

static int NC_plugin_path_initialized = 0;

/**
 * This function is called as part of nc_initialize.
 * Its purpose is to initialize the plugin paths state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_initialize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NULL;
    char* defaultpluginpath = NULL;
    const char* pluginroots = NULL;
    NClist* dirs = NULL;

    if(!NC_initialized) nc_initialize();
    if(NC_plugin_path_initialized != 0) goto done;
    NC_plugin_path_initialized = 1;

    gs = NC_getglobalstate();
    dirs = nclistnew();

   /* Setup the plugin path default */
   {
#ifdef _WIN32
	const char* win32_root;
	char dfalt[4096];
	win32_root = getenv(WIN32_ROOT_ENV);
	if(win32_root != NULL && strlen(win32_root) > 0) {
	    snprintf(dfalt,sizeof(dfalt),PLUGIN_DIR_WIN,win32_root);
	    defaultpluginpath = strdup(dfalt);
	}
#else /*!_WIN32*/
	defaultpluginpath = strdup(PLUGIN_DIR_UNIX);
#endif
    }

    /* Find the plugin directory root(s) */
    pluginroots = getenv(PLUGIN_ENV); /* Usually HDF5_PLUGIN_PATH */
    if(pluginroots  != NULL && strlen(pluginroots) == 0) pluginroots = NULL;
    if((stat = NC_plugin_path_parse(pluginroots,dirs))) goto done;
    /* Add the default to end of the dirs list if not already there */
    if(defaultpluginpath != NULL && !nclistmatch(dirs,defaultpluginpath,0)) {
        nclistpush(dirs,defaultpluginpath);
	defaultpluginpath = NULL;
    }

    /* Set the current plugin dirs sequence */
    assert(gs->pluginpaths == NULL);
    gs->pluginpaths = dirs; dirs = NULL;

#ifdef NETCDF_ENABLE_NCZARR
    if((stat = NCZ_plugin_path_initialize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_initialize())) goto done;
#endif

done:
    nullfree(defaultpluginpath);
    nclistfreeall(dirs);
    return NCTHROW(stat);
}

/**
 * This function is called as part of nc_finalize()
 * Its purpose is to clean-up plugin path state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_finalize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(NC_plugin_path_initialized == 0) goto done;
    NC_plugin_path_initialized = 0;

#ifdef NETCDF_ENABLE_NCZARR
    if((stat = NCZ_plugin_path_finalize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_finalize())) goto done;
#endif

    nclistfreeall(gs->pluginpaths); gs->pluginpaths = NULL;
done:
    return NCTHROW(stat);
}

/**
 * This function must be called to synchronize the plugin path state
 * with the state of the various implementations: currently libhdf5 and libnczarr.
 * @param formatx choose which dispatcher(s) to update: NC_FORMATX_NC_HDF5, NC_FORMATX_NCZARR, or 0(zero) to update all.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_sync(int formatx)
{
    int stat = NC_NOERR;

    if(!NC_initialized) nc_initialize();

    switch (formatx) {
#ifdef NETCDF_ENABLE_NCZARR
    case NC_FORMATX_NC_HDF5:
        if((stat = NCZ_plugin_path_sync(formatx))) goto done;    
	break;
#endif
#ifdef USE_HDF5
    case NC_FORMATX_NCZARR:
        if((stat = NC4_hdf5_plugin_path_sync(formatx))) goto done;
	break;
#endif
    case 0:
#ifdef NETCDF_ENABLE_NCZARR
        if((stat = NCZ_plugin_path_sync(NC_FORMATX_NCZARR))) goto done;    
#endif
#ifdef USE_HDF5
        if((stat = NC4_hdf5_plugin_path_sync(NC_FORMATX_NC_HDF5))) goto done;
#endif
	break;
    default:
        stat = NC_ENOTNC; goto done;    
    }	
done:
    return NCTHROW(stat);
}

/**
 * Return the current sequence of directories in the internal plugin path list.
 * Since this function does not modify the plugin path, it can be called at any time.
 *
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
nc_plugin_path_getall(size_t* npathsp, char** pathlist)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_getall(npathsp,pathlist,gs->pluginpaths);
}

/**
 * Get ith directory the internal path sequence.
 * The index starts at 0 (zero).
 * Caller frees the returned string.
 *
 * @param index of path to return
 * @entryp store copy of the index'th dir from the internal path or NULL if out of range.
 * @return NC_NOERR||NC_ERANGE if out of range
 *
	 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_getith(size_t index, char** entryp)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_getith(index,entryp,gs->pluginpaths);
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
 *
 * @param paths to overwrite the current internal path list
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_load(const char* paths)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_load(paths,gs->pluginpaths);
}

/**
 * Append a directory to the end of the current internal path list.
 *
 * @param dir directory to append.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_append(const char* path)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_append(path,gs->pluginpaths);
}

/**
 * Prepend a directory to the front of the current internal path list.
 *
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_prepend(const char* path)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_prepend(path,gs->pluginpaths);
}

/**
 * Remove all occurrences of a directory from the internal path sequence
 *
 * @param dir directory to prepend
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_remove(const char* dir)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    if(!NC_initialized) nc_initialize();
    return NCG_plugin_path_remove(dir,gs->pluginpaths);
}

/**************************************************/
/* Generic plugin path implementation */

EXTERNL int
NCG_plugin_path_getall(size_t* npathsp, char** pathlist, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    size_t npaths = 0;

    npaths = nclistlength(pluginpaths);
    if(npathsp) *npathsp = npaths;

    if(npaths > 0 && pathlist != NULL) {
	size_t i;
	for(i=0;i<npaths;i++) {
	    const char* dir = (const char*)nclistget(pluginpaths,i);
	    pathlist[i] = strdup(dir);
	}
    }
    return NCTHROW(stat);
}

EXTERNL int
NCG_plugin_path_getith(size_t index, char** entryp, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    size_t npaths = 0;
    const char* dir = NULL;
    
    npaths = nclistlength(pluginpaths);
    if(entryp == NULL || npaths == 0) goto done;
    if(index >= npaths) {stat = NC_ERANGE; goto done;} /* out of range */
    dir = (const char*)nclistget(pluginpaths,index);

done:
    if(entryp) *entryp = nulldup(dir);
    return NCTHROW(stat);
}

EXTERNL int
NCG_plugin_path_load(const char* paths, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    NClist* newpaths = nclistnew();
    size_t npathsnew = 0;

    /* Parse the paths */
    if((stat = NC_plugin_path_parse(paths,newpaths))) goto done;
    npathsnew = nclistlength(newpaths);

    /* Clear the current path list */
    nclistfreeall(pluginpaths);
    pluginpaths = NULL;

    if(npathsnew > 0) {
	/* Insert the new path list */
	assert(pluginpaths == NULL);
	pluginpaths = newpaths;
	newpaths = NULL;
    }

done:
    nclistfreeall(newpaths);    
    if(pluginpaths == NULL)
        pluginpaths = nclistnew();
    return NCTHROW(stat);
}

EXTERNL int
NCG_plugin_path_append(const char* path, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    nclistpush(pluginpaths,strdup(path));
    return NCTHROW(stat);
}

EXTERNL int
NCG_plugin_path_prepend(const char* path, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    nclistinsert(pluginpaths,0,strdup(path));
    return NCTHROW(stat);
}

EXTERNL int
NCG_plugin_path_remove(const char* dir, NClist* pluginpaths)
{
    int stat = NC_NOERR;
    size_t npaths = 0;
    size_t i;
    npaths = nclistlength(pluginpaths);
    if(npaths > 0 && dir != NULL) {
	/* Walk backward to avoid more complex logic; watch out for being unsigned */
	for(i=npaths;i-- > 0;) {
	    char* candidate = (char*)nclistget(pluginpaths,i);
	    if(strcmp(dir,candidate)==0) {
		nclistremove(pluginpaths,i);
		nullfree(candidate);
	    }
	}
    }
    return NCTHROW(stat);
}

/* Undocumented/Hidden for use for testing and debugging */

/* A utility function */
int
NC_plugin_path_parse(const char* path0, NClist* list)
{
    int i,stat = NC_NOERR;
    char* path = NULL;
    char* p;
    int count;
    size_t plen;
#ifdef _WIN32
    const char* seps = ";";
#else
    const char* seps = ";:";
#endif    

    if(path0 == NULL || path0[0] == '\0') goto done;
    if(list == NULL) {stat = NC_EINVAL; goto done;}
    plen = strlen(path0);
    if((path = malloc(plen+1+1))==NULL) {stat = NC_ENOMEM; goto done;}
    memcpy(path,path0,plen);
    path[plen] = '\0'; path[plen+1] = '\0';  /* double null term */
    for(count=0,p=path;*p;p++) {
	if(strchr(seps,*p) != NULL) {*p = '\0'; count++;}
    }
    count++; /* for last piece */
    for(p=path,i=0;i<count;i++) {
	size_t len = strlen(p);
	if(len > 0) 
	    nclistpush(list,nulldup(p));
        p = p+len+1; /* point to next piece */
    }

done:
    nullfree(path);
    return stat;
}

NCbytes* NC_pluginpaths_buf = NULL;

#define buf NC_pluginpaths_buf

const char*
NC_plugin_path_stringify(size_t npaths, char** paths)
{
    if(buf == NULL) buf = ncbytesnew();
    ncbytesclear(buf);
    if(npaths > 0) {
	size_t i;
	for(i=0;i<npaths;i++) {
	    if(i>0) ncbytescat(buf,";");
	    if(paths[i] != NULL) ncbytescat(buf,paths[i]);
	}
    } else
	ncbytesnull(buf);
    return ncbytescontents(buf);
}

const char*
NC_plugin_path_tostring(void)
{
    struct NCglobalstate* gs = NC_getglobalstate();
    return NC_plugin_path_stringify(nclistlength(gs->pluginpaths),(char**)nclistcontents(gs->pluginpaths));
}
#undef buf

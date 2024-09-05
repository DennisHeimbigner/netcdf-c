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
 * @return ::NC_NOERR
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
    int i;

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

    /* Initialize all the plugin path dispatchers and state*/
#ifdef USE_HDF5
    gs->formatxstate.pluginapi[NC_FORMATX_NC_HDF5] = &NC4_hdf5_pluginpathtable;
#endif
#ifdef NETCDF_ENABLE_NCZARR
    gs->formatxstate.pluginapi[NC_FORMATX_NCZARR] = &NCZ_pluginpathtable;
#endif
    /* Initialize all the plugin path dispatcher states */
    for(i=0;i<NC_FORMATX_COUNT;i++) {    
	if(gs->formatxstate.pluginapi[i] != NULL) {
	    if((stat = gs->formatxstate.pluginapi[i]->initialize(&gs->formatxstate.state[i], dirs))) goto done;
	    assert(gs->formatxstate.state[i] != NULL);
	}
    }

done:
    nullfree(defaultpluginpath);
    nclistfreeall(dirs);
    return NCTHROW(stat);
}

/**
 * This function is called as part of nc_finalize()
 * Its purpose is to clean-up plugin path state.
 *
 * @return ::NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_finalize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();
    int i;

    if(NC_plugin_path_initialized == 0) goto done;
    NC_plugin_path_initialized = 0;

    /* Finalize all the plugin path dispatchers */
    for(i=0;i<NC_FORMATX_COUNT;i++) {    
	if(gs->formatxstate.state[i] != NULL) {
	    if((stat = gs->formatxstate.pluginapi[i]->finalize(&gs->formatxstate.state[i]))) goto done;
	    gs->formatxstate.state[i] = NULL;
	}
    }
done:
    return NCTHROW(stat);
}

/**
 * Return the current sequence of directories in the internal plugin path list.
 * Since this function does not modify the plugin path, it can be called at any time.
 *
 * @param formatx the dispatcher from which to get the info
 * @param npaths return the number of paths in the path list
 * @param pathlist return the sequence of directies in the path list
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.
 * The first time with npaths not NULL and pathlist set to NULL
 *     to get the size of the path list.
 * The second time with pathlist not NULL to get the actual sequence of paths.
*/
EXTERNL int
nc_plugin_path_getall(int formatx, size_t* npathsp, char** pathlist)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    /* read functions can only apply to specific formatx */
    if(formatx == 0) {stat = NC_EINVAL; goto done;}
    if(gs->formatxstate.pluginapi[formatx] == NULL) {stat = NC_EPLUGIN; goto done;}
    if(gs->formatxstate.state[formatx] == NULL) {stat = NC_EPLUGIN; goto done;}
    if((stat = gs->formatxstate.pluginapi[formatx]->getall(gs->formatxstate.state[formatx],npathsp,pathlist))) goto done;
done:
    return NCTHROW(stat);
}

/**
 * Get ith directory the internal path sequence.
 * The index starts at 0 (zero).
 * Caller frees the returned string.
 *
 * @param formatx the dispatcher from which to get the info
 * @param index of path to return
 * @entryp store copy of the index'th dir from the internal path or NULL if out of range.
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_getith(int formatx, size_t index, char** entryp)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();
    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    /* read functions can only apply to specific formatx */
    if(formatx == 0) {stat = NC_EINVAL; goto done;}
    if(gs->formatxstate.pluginapi[formatx] == NULL) {stat = NC_EPLUGIN; goto done;}
    if(gs->formatxstate.state[formatx] == NULL) {stat = NC_EPLUGIN; goto done;}
    if((stat = gs->formatxstate.pluginapi[formatx]->getith(gs->formatxstate.state[formatx],index,entryp))) goto done;
done:
    return NCTHROW(stat);
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
 * @param formatx the dispatcher from which to get the info
 * @param paths to overwrite the current internal path list
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_load(int formatx, const char* paths)
{
    int i,stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    if(formatx) {
        if(gs->formatxstate.pluginapi[formatx] == NULL || gs->formatxstate.state[formatx] == NULL)
	    {stat = NC_EPLUGIN; goto done;}
	if((stat=gs->formatxstate.pluginapi[formatx]->load(gs->formatxstate.state[formatx],paths))) goto done;
    } else {/* forall dispatchers */
        for(i=0;i<NC_FORMATX_COUNT;i++) {
            if(gs->formatxstate.pluginapi[i] != NULL) {
		if(gs->formatxstate.state[i] == NULL)
		    {stat = NC_EPLUGIN; goto done;}
		if((stat=gs->formatxstate.pluginapi[i]->load(gs->formatxstate.state[i],paths))) goto done;
	    }
	}
    }
done:
    return NCTHROW(stat);
}

/**
 * Append a directory to the end of the current internal path list.
 *
 * @param formatx the dispatcher from which to get the info
 * @param dir directory to append.
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_append(int formatx, const char* dir)
{
    int i,stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    if(formatx) {
        if(gs->formatxstate.pluginapi[formatx] == NULL || gs->formatxstate.state[formatx] == NULL)
	    {stat = NC_EPLUGIN; goto done;}
	if((stat=gs->formatxstate.pluginapi[formatx]->append(gs->formatxstate.state[formatx],dir))) goto done;
    } else {/* forall dispatchers */
        for(i=0;i<NC_FORMATX_COUNT;i++) {
            if(gs->formatxstate.pluginapi[i] != NULL) {
		if(gs->formatxstate.state[i] == NULL)
		    {stat = NC_EPLUGIN; goto done;}
		if((stat=gs->formatxstate.pluginapi[i]->append(gs->formatxstate.state[i],dir))) goto done;
	    }
	}
    }
done:
    return NCTHROW(stat);
}

/**
 * Prepend a directory to the front of the current internal path list.
 *
 * @param formatx the dispatcher from which to get the info
 * @param dir directory to prepend
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_prepend(int formatx, const char* dir)
{
    int i,stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    if(formatx) {
        if(gs->formatxstate.pluginapi[formatx] == NULL || gs->formatxstate.state[formatx] == NULL)
	    {stat = NC_EPLUGIN; goto done;}
	if((stat=gs->formatxstate.pluginapi[formatx]->prepend(gs->formatxstate.state[formatx],dir))) goto done;
    } else {/* forall dispatchers */
        for(i=0;i<NC_FORMATX_COUNT;i++) {
            if(gs->formatxstate.pluginapi[i] != NULL) {
		if(gs->formatxstate.state[i] == NULL)
		    {stat = NC_EPLUGIN; goto done;}
		if((stat=gs->formatxstate.pluginapi[i]->prepend(gs->formatxstate.state[i],dir))) goto done;
	    }
	}
    }
done:
    return NCTHROW(stat);
}

/**
 * Remove all occurrences of a directory from the internal path sequence
 *
 * @param formatx the dispatcher from which to get the info
 * @param dir directory name to remove
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 * @return ::NC_EPLUGIN if the dispatcher or state for formatx is NULL
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_remove(int formatx, const char* dir)
{
    int i,stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    if(formatx) {
        if(gs->formatxstate.pluginapi[formatx] == NULL || gs->formatxstate.state[formatx] == NULL)
	    {stat = NC_EPLUGIN; goto done;}
	if((stat=gs->formatxstate.pluginapi[formatx]->remove(gs->formatxstate.state[formatx],dir))) goto done;
    } else {/* forall dispatchers */
        for(i=0;i<NC_FORMATX_COUNT;i++) {
            if(gs->formatxstate.pluginapi[i] != NULL) {
		if(gs->formatxstate.state[i] == NULL)
		    {stat = NC_EPLUGIN; goto done;}
		if((stat=gs->formatxstate.pluginapi[i]->remove(gs->formatxstate.state[i],dir))) goto done;
	    }
	}
    }
done:
    return NCTHROW(stat);
}

/**************************************************/
/* Undocumented/Hidden for use for testing and debugging */

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

const char*
NC_plugin_path_tostring(size_t npaths, char** paths)
{
static NCbytes* NC_pluginpaths_buf = NULL;
    if(NC_pluginpaths_buf == NULL) NC_pluginpaths_buf = ncbytesnew();
    ncbytesclear(NC_pluginpaths_buf);
    if(npaths > 0) {
	size_t i;
	for(i=0;i<npaths;i++) {
	    if(i>0) ncbytescat(NC_pluginpaths_buf,";");
	    if(paths[i] != NULL) ncbytescat(NC_pluginpaths_buf,paths[i]);
	}
    } else
	ncbytesnull(NC_pluginpaths_buf);
    return ncbytescontents(NC_pluginpaths_buf);
}

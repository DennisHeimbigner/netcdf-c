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
#include "netcdf_aux.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "nclog.h"
#include "ncbytes.h"
#include "ncproplist.h"

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

/* Forward */
static int reclaimstringvec(uintptr_t userdata, const char* key, void* value, uintptr_t size);

/**************************************************/
/**
 * This function is called as part of nc_initialize.
 * Its purpose is to initialize the plugin paths state.
 *
 * @param plist insert any plugin-path related keys into this argument
 * @return ::NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_initialize(NCproplist* plist)
{
    int stat = NC_NOERR;
    char* defaultpluginpath = NULL;
    const char* pluginroots = NULL;
    NClist* dirs = NULL;
    size_t ndirs;
    char** contents = NULL;

    if(NC_plugin_path_initialized != 0) goto done;
    NC_plugin_path_initialized = 1;

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
    if((stat = ncaux_plugin_path_parse(pluginroots,0,&ndirs,NULL))) goto done;
    if(ndirs > 0) {
	nclistsetlength(dirs,ndirs); /* may modify contents memory */
	contents = (char**)nclistcontents(dirs);
	/* Stash parsed dirs in to dirs contents */
	if((stat = ncaux_plugin_path_parse(pluginroots,0,&ndirs,contents))) goto done;
	contents = NULL;
    }
    /* Add the default to end of the dirs list if not already there */
    if(defaultpluginpath != NULL && !nclistmatch(dirs,defaultpluginpath,0)) {
        nclistpush(dirs,defaultpluginpath);
	defaultpluginpath = NULL;
    }

    /* Insert into initialization proplist */
    ndirs = nclistlength(dirs); /* do before extraction */
    contents = nclistextract(dirs);
    ncproplistaddx(plist, "plugin_path_defaults", contents, ndirs * sizeof(char*), (uintptr_t)NULL, reclaimstringvec);
    contents = NULL;
    ndirs = 0;

done:
    nullfree(defaultpluginpath);
    nclistfreeall(dirs);
    ncaux_plugin_path_freestringvec(ndirs,contents);    
    return NCTHROW(stat);
}

/* Wrap ncaux_plugin_path_freestringvec to act as a proplist value reclaim fcn. */
static int
reclaimstringvec(uintptr_t userdata, const char* key, void* value, uintptr_t size)
{
    size_t ndirs = 0;
    NC_UNUSED(userdata);
    NC_UNUSED(key);
    ndirs = (size_t)(size/sizeof(char*));
    return ncaux_plugin_path_freestringvec(ndirs, (char**)value);
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
    if(NC_plugin_path_initialized == 0) goto done;
    NC_plugin_path_initialized = 0;
done:
    return NCTHROW(stat);
}

/**
 * Return the current sequence of directories in the internal plugin path list.
 * Since this function does not modify the plugin path, it can be called at any time.
 *
 * @param formatx the dispatcher from which to get the info
 * @param ndirsp return the number of paths in the path list
 * @param dirs copy the sequence of directories in the path list into this; caller must free the copied strings
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or zero
 *
 * @author Dennis Heimbigner
 *
 * As a rule, this function needs to be called twice.  The first time
 * with ndirsp not NULL and dirs set to NULL to get the size of
 * the path list. The second time with dirs not NULL to get the
 * actual sequence of paths.
*/

EXTERNL int
nc_plugin_path_get(int formatx, size_t* ndirsp, char** dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    /* read functions can only apply to specific formatx */
    if(formatx == 0) {stat = NC_EINVAL; goto done;}

    if(gs->formatxstate.dispatchapi[formatx] == NULL) {stat = NC_EINVAL; goto done;}
    if((stat = gs->formatxstate.dispatchapi[formatx]->plugin_path_get(gs->formatxstate.state[formatx],ndirsp,dirs))) goto done;
done:
    return NCTHROW(stat);
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories
 * specified in the arguments.
 * If ndirs == 0 the path list will be cleared
 *
 * @param formatx the dispatcher to which to write; zero means all dispatchers
 * @param ndirs number of entries in dirs arg
 * @param dirs the actual directory paths
 *
 * @return ::NC_NOERR
 * @return ::NC_EINVAL if formatx is unknown or ndirs > 0 and dirs == NULL
 *
 * @author Dennis Heimbigner
 * 
 * Note that modifying the plugin paths must be done "atomically".
 * That is, in a multi-threaded environment, it is important that
 * the sequence of actions involved in setting up the plugin paths
 * must be done by a single processor or in some other way as to
 * guarantee that two or more processors are not simultaneously
 * accessing the plugin path read/write operations.
 * 
 * As an example, assume there exists a mutex lock called PLUGINLOCK.
 * Then any processor accessing the plugin paths should operate
 * as follows:
 * <pre>
 * lock(PLUGINLOCK);
 * nc_plugin_path_read(...);
 * <rebuild plugin path>
 * nc_plugin_path_write(...);
 * unlock(PLUGINLOCK);
 * </pre>
*/

EXTERNL int
nc_plugin_path_set(int formatx, size_t ndirs, char** const dirs)
{
    int i,stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(formatx < 0 || formatx >= NC_FORMATX_COUNT) {stat = NC_EINVAL; goto done;}
    if(ndirs > 0 && dirs == NULL) {stat = NC_EINVAL; goto done;}
    if(!NC_initialized) nc_initialize();
    /* forall dispatchers */
    for(i=1;i<NC_FORMATX_COUNT;i++) {
	if(i == formatx || formatx == 0) {
	    if(gs->formatxstate.dispatchapi[i] == NULL) continue;
	    if((stat=gs->formatxstate.dispatchapi[i]->plugin_path_set(gs->formatxstate.state[i],ndirs,dirs))) goto done;
	}
    }
done:
    return NCTHROW(stat);
}

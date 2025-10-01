/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"
#include "ncglobal.h"
#include "ncaws.h"
#include "ncpathmgr.h"
#include "nclist.h"
#include "ncuri.h"
#include "ncrc.h"
#include "nclog.h"
#include "ncs3sdk.h"

/**************************************************/

#define MAXPATH 1024

/**************************************************/
/* Global State constants and state */

/* The singleton global state object */
static NCglobalstate* nc_globalstate = NULL;

/* Forward */
static int NC_createglobalstate(void);
static void gs_chunkcache_init(NCglobalstate* gs);
static void gs_paths_init(NCglobalstate* gs);

/** \defgroup global_state Global state functions. */
/** \{

\ingroup global_state
*/

/* NCglobal state management */

static int
NC_createglobalstate(void)
{
    int stat = NC_NOERR;
    const char* tmp = NULL;
    NCglobalstate* gs = nc_globalstate; /* allow shorter name */
    
    if(gs == NULL) {
        gs = calloc(1,sizeof(NCglobalstate));
	if(gs == NULL) {stat = NC_ENOMEM; goto done;}
	/* Initialize struct pointers */
	if((gs->rcinfo = calloc(1,sizeof(struct NCRCinfo)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((gs->rcinfo->entries = nclistnew())==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((gs->chunkcache = calloc(1,sizeof(struct ChunkCache)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	if((gs->profiles = nclistnew())==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }

    /* Initialize chunk cache defaults */
    gs_chunkcache_init(gs);
    
    /* Initialize various paths */
    gs_paths_init(gs);
    
    /* Get .rc state */
    if(getenv(NCRCENVIGNORE) != NULL)
        gs->rcinfo->ignore = 1;
    tmp = getenv(NCRCENVRC);
    if(tmp != NULL && strlen(tmp) > 0)
        gs->rcinfo->rcfile = strdup(tmp);
    ncrc_initialize(); /* load and parse the .ncrc file */

    if(NC_profiles_load()) {
        nclog(NCLOGWARN,"AWS profiles not loaded");
    }

done:
    return stat;
}

/* Initialize chunk cache defaults */
static void
gs_chunkcache_init(NCglobalstate* gs)
{    
    gs->chunkcache->size = DEFAULT_CHUNK_CACHE_SIZE;		/**< Default chunk cache size. */
    gs->chunkcache->nelems = DEFAULT_CHUNKS_IN_CACHE;		/**< Default chunk cache number of elements. */
    gs->chunkcache->preemption = DEFAULT_CHUNK_CACHE_PREEMPTION;/**< Default chunk cache preemption. */
}

static void
gs_paths_init(NCglobalstate* gs)
{
    /* Capture temp dir*/
    {
	char* tempdir = NULL;
#if defined _WIN32 || defined __MSYS__ || defined __CYGWIN__
        tempdir = getenv("TEMP");
#else
	tempdir = "/tmp";
#endif
        if(tempdir == NULL) {
	    fprintf(stderr,"Cannot find a temp dir; using ./\n");
	    tempdir = ".";
	}
	gs->tempdir= strdup(tempdir);
    }

    /* Capture $HOME */
    {
#if defined(_WIN32) && !defined(__MINGW32__)
        char* home = getenv("USERPROFILE");
#else
        char* home = getenv("HOME");
#endif
        if(home == NULL) {
	    /* use cwd */
	    home = malloc(MAXPATH+1);
	    NCgetcwd(home,MAXPATH);
        } else
	    home = strdup(home); /* make it always free'able */
	assert(home != NULL);
        NCpathcanonical(home,&gs->home);
	nullfree(home);
    }
 
    /* Capture $CWD */
    {
        char cwdbuf[4096];

        cwdbuf[0] = '\0';
	(void)NCgetcwd(cwdbuf,sizeof(cwdbuf));

        if(strlen(cwdbuf) == 0) {
	    /* use tempdir */
	    strcpy(cwdbuf, gs->tempdir);
	}
        gs->cwd = strdup(cwdbuf);
    }

}

/* Get global state */
NCglobalstate*
NC_getglobalstate(void)
{
    if(nc_globalstate == NULL)
        NC_createglobalstate();
    return nc_globalstate;
}

void
NC_freeglobalstate(void)
{
    NCglobalstate* gs = nc_globalstate;
    if(gs != NULL) {
	NC_rcclear(gs->rcinfo);
        nullfree(gs->tempdir);
        nullfree(gs->home);
        nullfree(gs->cwd);
	nullfree(gs->chunkcache);
	nullfree(gs->rcinfo);
	nclistfree(gs->pluginpaths);
	NC_profiles_free(gs->profiles);
	free(gs);
	nc_globalstate = NULL;
    }
}

/** \} */

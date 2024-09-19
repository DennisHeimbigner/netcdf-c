/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"

#ifdef USE_PARALLEL
#include <mpi.h>
#endif

#include "ncdispatch.h"
#include "ncproplist.h"
#ifdef USE_NETCDF4
#include "nc4internal.h"
#endif
#ifdef USE_HDF5
#include "hdf5internal.h"
#endif

#ifdef _MSC_VER
#include <io.h>
#include <fcntl.h>
#endif

#ifdef NETCDF_ENABLE_S3
EXTERNL int NC_s3sdkinitialize(void);
EXTERNL int NC_s3sdkfinalize(void);
#endif

#ifdef USE_NETCDF4
EXTERNL int NC4_initialize(void);
EXTERNL int NC4_finalize(void);
#endif

int NC_initialized = 0;
int NC_finalized = 1;

#ifdef NETCDF_ENABLE_ATEXIT_FINALIZE
/* Provide the void function to give to atexit() */
static void
finalize_atexit(void)
{
    (void)nc_finalize();
}
#endif

/**
This procedure invokes all defined
initializers, and there is an initializer
for every known dispatch table.
So if you modify the format of NC_Dispatch,
then you need to fix it everywhere.
It also initializes appropriate external libraries.
*/

int
nc_initialize()
{
    int stat = NC_NOERR;
    size_t i;
    NCproplist* proplist = NULL; /* for accumulating init properties */
    
    if(NC_initialized) return NC_NOERR;
    NC_initialized = 1;
    NC_finalized = 0;
    NCglobalstate* gs = NULL;

    /* Do general initialization */
    if((stat = NCDISPATCH_initialize())) goto done;

    gs = NC_getglobalstate(); /* will allocate and clear */

    /* Non-dispatcher initialization */
#ifdef NETCDF_ENABLE_S3
    if((stat = NC_s3sdkinitialize())) goto done;
#endif
#ifdef USE_NETCDF4
    if((stat = NC4_initialize())) goto done;
#endif

    /* Initialize all the per-dispatcher api information */
#ifdef USE_HDF5
    gs->formatxstate.dispatchapi[NC_FORMATX_NC_HDF5] = NC4_hdf5_global_dispatch_table;
#endif
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    gs->formatxstate.dispatchapi[NC_FORMATX_NCZARR] = NCZ_global_dispatch_table;
#endif
    gs->formatxstate.dispatchapi[NC_FORMATX_NC3] = NC3_global_dispatch_table;
#ifdef NETCDF_ENABLE_DAP
    gs->formatxstate.dispatchapi[NC_FORMATX_DAP2] = NCD2_global_dispatch_table;
#endif
#ifdef NETCDF_ENABLE_DAP4
    gs->formatxstate.dispatchapi[NC_FORMATX_DAP4] = NCD4_global_dispatch_table;
#endif
#ifdef USE_PNETCDF
    gs->formatxstate.dispatchapi[NC_FORMATX_PNETCDF] = NCP_global_dispatch_table;
#endif
#ifdef USE_HDF4
    gs->formatxstate.dispatchapi[NC_FORMATX_NC4] = NC_HDF4_global_dispatch_table;
#endif

    /* Construct the property list for initialization */
    proplist = ncproplistnew(); /* for accumulating init properties */
    /* Invoke various functions to fill in the properly list */
    if((stat=nc_plugin_path_initialize(proplist))) goto done;

    /* Initialize all the per-dispatcher states */
    for(i=1;i<NC_FORMATX_COUNT;i++) {    
	if(gs->formatxstate.dispatchapi[i] != NULL) {
	    if((stat = gs->formatxstate.dispatchapi[i]->initialize(&gs->formatxstate.state[i],proplist))) goto done;
	    /* Might leave the state NULL if it has no per-dispatcher state */
	}
    }

#ifdef NETCDF_ENABLE_ATEXIT_FINALIZE
    /* Use atexit() to invoke nc_finalize */
    if(atexit(finalize_atexit))
	fprintf(stderr,"atexit failed\n");
#endif

done:
    ncproplistfree(proplist);
    return stat;
}

/**
This procedure invokes all defined
finalizers, and there should be one
for every known dispatch table.
So if you modify the format of NC_Dispatch,
then you need to fix it everywhere.
It also finalizes appropriate external libraries.
*/

int
nc_finalize(void)
{
    int stat = NC_NOERR;
    int failed = stat;
    NCglobalstate* gs = NC_getglobalstate();
    size_t i;

    if(NC_finalized) goto done;
    NC_initialized = 0;
    NC_finalized = 1;

    /* Finalize all the per-dispatcher states */
    for(i=1;i<NC_FORMATX_COUNT;i++) {    
	if(gs->formatxstate.dispatchapi[i] != NULL && gs->formatxstate.state[i] != NULL) {
	    if((stat = gs->formatxstate.dispatchapi[i]->finalize(&gs->formatxstate.state[i]))) goto done;
	    assert(gs->formatxstate.state[i] == NULL);
	}
    }

    /* Do general finalization */
    if((stat = NCDISPATCH_finalize())) failed = stat;

    /* Non-dispatcher finalizers */
#ifdef NETCDF_ENABLE_S3
    if((stat = NC_s3sdkfinalize())) failed = stat;
#endif
#ifdef USE_NETCDF4
    if((stat = NC4_finalize())) goto done;
#endif

done:
    if(failed) fprintf(stderr,"nc_finalize failed: %d\n",failed);
    return failed;
}

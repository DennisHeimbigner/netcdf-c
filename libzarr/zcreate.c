/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file
 * @internal The netCDF-4 file functions relating to file creation.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

/** @internal These flags may not be set for create. */
static const int ILLEGAL_CREATE_FLAGS = (NC_NOWRITE|NC_MMAP|NC_DISKLESS|NC_64BIT_OFFSET|NC_CDF5);

/**
 * @internal Create a netCDF-4/NCZ file.
 *
 * @param path The file name of the new file.
 * @param cmode The creation mode flag.
 * @param initialsz The proposed initial file size (advisory)
 * @param parameters extra parameter info (like  MPI communicator)
 * @param nc Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input (check cmode).
 * @return ::NC_EEXIST File exists and NC_NOCLOBBER used.
 * @return ::NC_EHDFERR ZARR returned error.
 * @ingroup netcdf4
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_create_file(const char *path, int cmode, size_t initialsz,
                void* parameters, int ncid)
{
    int retval = NC_NOERR;
    NC_FILE_INFO_T* h5 = NULL;
    NCZ_FILE_INFO_T *zinfo;
    NCZ_GRP_INFO_T *zgrp;

    assert(path);
    LOG((3, "%s: path %s mode 0x%x", __func__, path, cmode));

    /* Add necessary structs to hold netcdf-4 file data. */
    if ((retval = nc4_file_list_add(ncid, path, (NC_WRITE | cmode), (void**)&h5)))
        BAIL(retval);
    assert(h5 && h5->root_grp);
    h5->root_grp->atts_read = 1;

    h5->mem.inmemory = ((cmode & NC_INMEMORY) == NC_INMEMORY);
    h5->mem.diskless = ((cmode & NC_DISKLESS) == NC_DISKLESS);
    h5->mem.persist = ((cmode & NC_PERSIST) == NC_PERSIST);

    /* Add struct to hold NCZ-specific file metadata. */
    if (!(zinfo = calloc(1, sizeof(NCZ_FILE_INFO_T))))
        BAIL(NC_ENOMEM);

    /* Add struct to hold NCZ-specific group info. */
    if (!(zgrp = calloc(1, sizeof(NCZ_GRP_INFO_T))))
        BAIL(NC_ENOMEM);
    h5->root_grp->format_grp_info = zgrp;

    /* Do format specific setup */
    /* Should check if file already exists, and if NC_NOCLOBBER is specified,
       return an error */
    if((retval = ncz_create_dataset(h5,h5->root_grp)))
	BAIL(retval);

#ifdef LOOK
    if (H5Pset_cache(fapl_id, 0, ncz_chunk_cache_nelems, ncz_chunk_cache_size,
                     ncz_chunk_cache_preemption) < 0)
        BAIL(NC_EHDFERR);
    LOG((4, "%s: set HDF raw chunk cache to size %d nelems %d preemption %f",
         __func__, ncz_chunk_cache_size, ncz_chunk_cache_nelems,
         ncz_chunk_cache_preemption));
#endif

#ifdef LOOK
        {
            /* Create the ZARR file. */
            if ((h5->hdfid = H5Fcreate(path, flags, fcpl_id, fapl_id)) < 0)
                BAIL(EACCES);
        }

    /* Open the root group. */
    if ((ncz_grp->hdf_grpid = H5Gopen2(h5->hdfid, "/", H5P_DEFAULT)) < 0)
        BAIL(NC_EFILEMETA);
#endif

    /* Define mode gets turned on automatically on create. */
    h5->flags |= NC_INDEF;

    /* Set provenance. */
    if ((retval = NCZ_new_provenance(h5)))
        BAIL(retval);

    return NC_NOERR;

exit: /*failure exit*/
    if(!h5) return retval;
#ifdef LOOK
    ncz_close_ncz_file(h5, 1, NULL); /* treat like abort */
#endif
    return retval;
}

/**
 * @internal Create a netCDF-4/NCZ file.
 *
 * @param path The file name of the new file.
 * @param cmode The creation mode flag.
 * @param initialsz Ignored by this function.
 * @param basepe Ignored by this function.
 * @param chunksizehintp Ignored by this function.
 * @param parameters pointer to struct holding extra data (e.g. for
 * parallel I/O) layer. Ignored if NULL.
 * @param dispatch Pointer to the dispatch table for this file.
 * @param nc_file Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid input (check cmode).
 * @ingroup netcdf4
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_create(const char* path, int cmode, size_t initialsz, int basepe,
           size_t *chunksizehintp, void *parameters,
           const NC_Dispatch *dispatch, int ncid)
{
    int stat = NC_NOERR;

    assert(path);

    LOG((1, "%s: path %s cmode 0x%x parameters %p ncid %d",
         __func__, path, cmode, parameters,ncid));

    /* If this is our first file, initialize */
    if (!ncz_initialized) NCZ_initialize();

#ifdef LOGGING
    /* If nc logging level has changed, see if we need to turn on
     * NCZ's error messages. */
    NCZ_set_log_level();
#endif /* LOGGING */

    /* Check the cmode for validity. */
    if((cmode & ILLEGAL_CREATE_FLAGS) != 0)
    {stat = NC_EINVAL; goto done;}

    stat = ncz_create_file(path, cmode, initialsz, parameters, ncid);

done:
    return stat;
}

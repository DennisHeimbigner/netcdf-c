/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */

/**
 * @file
 * @internal The netCDF-4 file functions.
 *
 * This file is part of netcdf-4, a netCDF-like interface for NCZ, or
 * a ZARR backend for netCDF, depending on your point of view.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

/* Forward */
static int NCZ_enddef(int ncid);

/**
 * @internal This function will write all changed metadata and flush
 * ZARR file to disk.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINDEFINE Classic model file in define mode.
 * @return ::NC_EHDFERR ZARR error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
sync_netcdf4_file(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;

    assert(file && file->format_file_info);
    LOG((3, "%s", __func__));

    /* If we're in define mode, that's an error, for strict nc3 rules,
     * otherwise, end define mode. */
    if (file->flags & NC_INDEF)
    {
        if (file->cmode & NC_CLASSIC_MODEL)
            return NC_EINDEFINE;

        /* Turn define mode off. */
        file->flags ^= NC_INDEF;

        /* Redef mode needs to be tracked separately for nc_abort. */
        file->redef = NC_FALSE;
    }

#ifdef LOGGING
    /* This will print out the names, types, lens, etc of the vars and
       atts in the file, if the logging level is 2 or greater. */
    log_metadata_nc(file);
#endif

    /* Write any metadata that has changed. */
    if (!file->no_write)
    {
#ifdef LOOK
        /* Write any user-defined types. */
        if ((stat = ncz_rec_write_groups_types(zinfo,file->root_grp)))
            return stat;

        /* Write all the metadata. */
        if ((stat = ncz_rec_write_metadata(zinfo,file->root_grp)))
            return stat;
#endif
        /* Write all the metadata. */
	if((stat = ncz_sync_file(file)))
	    return stat;

        /* Write out provenance*/
        if((stat = NCZ_write_provenance(file)))
            return stat;
    }

#ifdef LOOK
     /* Tell ZARR to flush all changes to the file. */
     ncz_info = (NCZ_FILE_INFO_T *)h5->format_file_info;
     if (H5Fflush(ncz_info->hdfid, H5F_SCOPE_GLOBAL) < 0)
         return NC_EHDFERR;
#endif

    return NC_NOERR;
}

/**
 * @internal This function will free all allocated metadata memory,
 * and close the ZARR file. The group that is passed in must be the
 * root group of the file. If inmemory is used, then save
 * the final memory in mem.memio.
 *
 * @param file Pointer to ZARR file info struct.
 * @param abort True if this is an abort.
 * @param memio the place to return a core image if not NULL
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR could not close the file.
 * @return ::NC_EINDEFINE Classic model file is in define mode.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_close_netcdf4_file(NC_FILE_INFO_T* file, int abort)
{
    int stat = NC_NOERR;

    assert(file && file->root_grp && file->format_file_info);
    LOG((3, "%s: file->path %s abort %d", __func__, file->controller->path, abort));

    /* Free the fileinfo struct, which holds info from the fileinfo
     * hidden attribute. */
    NCZ_clear_provenance(&file->provenance);

#ifdef LOOK
     /* Close hdf file. It may not be open, since this function is also
      * called by NC_create() when a file opening is aborted. */
     if (zinfo->hdfid > 0 && H5Fclose(zinfo->hdfid) < 0)
     {
         dumpopenobjects(h5);
         return NC_EHDFERR;
     }
#endif

    /* Free the NCZ-specific info. */
    if (file->format_file_info)
        free(file->format_file_info);

    /* Free the NC_FILE_INFO_T struct. */
    if ((stat = nc4_nc4f_list_del(file)))
        return stat;

    return NC_NOERR;
}

/**
 * @internal This function will recurse through an open ZARR file and
 * release resources. All open ZARR objects in the file will be
 * closed.
 *
 * @param file Pointer to ZARR file info struct.
 * @param abort True if this is an abort.
 * @param memio the place to return a core image if not NULL
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR could not close the file.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_close_ncz_file(NC_FILE_INFO_T* file, int abort)
{
    int stat = NC_NOERR;

    assert(file && file->root_grp && file->format_file_info);
    LOG((3, "%s: file->path %s abort %d", __func__, file->controller->path, abort));

    /* According to the docs, always end define mode on close. */
    if (file->flags & NC_INDEF)
        file->flags ^= NC_INDEF;

    /* Sync the file, unless we're aborting, or this is a read-only
     * file. */
    if (!file->no_write && !abort)
        if ((stat = sync_netcdf4_file(file)))
            return stat;

    /* Close all open ZARR objects within the file. */
    if ((stat = ncz_rec_grp_NCZ_del(file->root_grp)))
        return stat;

    /* Release all intarnal lists and metadata associated with this
     * file. All ZARR objects have already been released. */
    if ((stat = ncz_close_netcdf4_file(file, abort)))
        return stat;

    return NC_NOERR;
}

#ifdef LOOK
/**
 * @internal Output a list of still-open objects in the NCZ
 * file. This is only called if the file fails to close cleanly.
 *
 * @param file Pointer to file info.
 *
 * @author Dennis Heimbigner
 */
static void
dumpopenobjects(NC_FILE_INFO_T* file)
{
    NCZ_FILE_INFO* zinfo = NULL;
    int nobjs;

    assert(file && file->format_file_info);
    zinfo = (NCZ_FILE_INFO*)file->format_file_info;

    if(zinfo->hdfid <= 0)
        return; /* File was never opened */

    nobjs = H5Fget_obj_count(zinfo->hdfid, H5F_OBJ_ALL);

    /* Apparently we can get an error even when nobjs == 0 */
    if(nobjs < 0) {
        return;
    } else if(nobjs > 0) {
        char msg[1024];
        int logit = 0;
        /* If the close doesn't work, probably there are still some NCZ
         * objects open, which means there's a bug in the library. So
         * print out some info on to help the poor programmer figure it
         * out. */
        snprintf(msg,sizeof(msg),"There are %d ZARR objects open!", nobjs);
#ifdef LOGGING
#ifdef LOGOPEN
        LOG((0, msg));
        logit = 1;
#endif
#else
        fprintf(stdout,"%s\n",msg);
        logit = 0;
#endif
        reportopenobjects(logit,zinfo->hdfid);
        fflush(stderr);
    }

    return;
}
#endif

/**
 * @internal Unfortunately HDF only allows specification of fill value
 * only when a dataset is created. Whereas in netcdf, you first create
 * the variable and then (optionally) specify the fill value. To
 * accomplish this in ZARR I have to delete the dataset, and recreate
 * it, with the fill value specified.
 *
 * @param ncid File and group ID.
 * @param fillmode File mode.
 * @param old_modep Pointer that gets old mode. Ignored if NULL.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_set_fill(int ncid, int fillmode, int *old_modep)
{
    NC_FILE_INFO_T* zinfo = NULL;
    int stat = NC_NOERR;

    LOG((2, "%s: ncid 0x%x fillmode %d", __func__, ncid, fillmode));

    /* Get pointer to file info. */
    if ((stat = nc4_find_grp_h5(ncid, NULL, &zinfo)))
        return stat;
    assert(zinfo);

    /* Trying to set fill on a read-only file? You sicken me! */
    if (zinfo->no_write)
        return NC_EPERM;

    /* Did you pass me some weird fillmode? */
    if (fillmode != NC_FILL && fillmode != NC_NOFILL)
        return NC_EINVAL;

    /* If the user wants to know, tell him what the old mode was. */
    if (old_modep)
        *old_modep = zinfo->fill_mode;

    zinfo->fill_mode = fillmode;

    return NC_NOERR;
}

/**
 * @internal Put the file back in redef mode. This is done
 * automatically for netcdf-4 files, if the user forgets.
 *
 * @param ncid File and group ID.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_redef(int ncid)
{
    NC_FILE_INFO_T* zinfo = NULL;
    int stat = NC_NOERR;

    LOG((1, "%s: ncid 0x%x", __func__, ncid));

    /* Find this file's metadata. */
    if ((stat = nc4_find_grp_h5(ncid, NULL, &zinfo)))
        return stat;
    assert(zinfo);

    /* If we're already in define mode, return an error. */
    if (zinfo->flags & NC_INDEF)
        return NC_EINDEFINE;

    /* If the file is read-only, return an error. */
    if (zinfo->no_write)
        return NC_EPERM;

    /* Set define mode. */
    zinfo->flags |= NC_INDEF;

    /* For nc_abort, we need to remember if we're in define mode as a
       redef. */
    zinfo->redef = NC_TRUE;

    return NC_NOERR;
}

/**
 * @internal For netcdf-4 files, this just calls nc_enddef, ignoring
 * the extra parameters.
 *
 * @param ncid File and group ID.
 * @param h_minfree Ignored for netCDF-4 files.
 * @param v_align Ignored for netCDF-4 files.
 * @param v_minfree Ignored for netCDF-4 files.
 * @param r_align Ignored for netCDF-4 files.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ__enddef(int ncid, size_t h_minfree, size_t v_align,
            size_t v_minfree, size_t r_align)
{
    return NCZ_enddef(ncid);
}

/**
 * @internal Take the file out of define mode. This is called
 * automatically for netcdf-4 files, if the user forgets.
 *
 * @param ncid File and group ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADGRPID Bad group ID.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
NCZ_enddef(int ncid)
{
    NC_FILE_INFO_T* h5 = NULL;
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
    int i;
    int stat = NC_NOERR;

    LOG((1, "%s: ncid 0x%x", __func__, ncid));

    /* Find pointer to group and zinfo. */
    if ((stat = nc4_find_grp_h5(ncid, &grp, &h5)))
        return stat;

    /* When exiting define mode, mark all variable written. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        assert(var);
        var->written_to = NC_TRUE;
    }

    return ncz_enddef_netcdf4_file(h5);
}

/**
 * @internal Flushes all buffers associated with the file, after
 * writing all changed metadata. This may only be called in data mode.
 *
 * @param ncid File and group ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EINDEFINE Classic model file is in define mode.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_sync(int ncid)
{
    NC_FILE_INFO_T* zinfo = NULL;
    int stat = NC_NOERR;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    if ((stat = nc4_find_grp_h5(ncid, NULL, &zinfo)))
        return stat;
    assert(zinfo);

    /* If we're in define mode, we can't sync. */
    if (zinfo->flags & NC_INDEF)
    {
        if (zinfo->cmode & NC_CLASSIC_MODEL)
            return NC_EINDEFINE;
        if ((stat = NCZ_enddef(ncid)))
            return stat;
    }

    return sync_netcdf4_file(zinfo);
}

/**
 * @internal From the netcdf-3 docs: The function nc_abort just closes
 * the netCDF dataset, if not in define mode. If the dataset is being
 * created and is still in define mode, the dataset is deleted. If
 * define mode was entered by a call to nc_redef, the netCDF dataset
 * is restored to its state before definition mode was entered and the
 * dataset is closed.
 *
 * @param ncid File and group ID.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_abort(int ncid)
{
    NC *nc;
    NC_FILE_INFO_T* zinfo = NULL;
    int delete_file = 0;
    char path[NC_MAX_NAME + 1];
    int stat = NC_NOERR;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    /* Find metadata for this file. */
    if ((stat = nc4_find_nc_grp_h5(ncid, &nc, NULL, &zinfo)))
        return stat;
    assert(zinfo);

    /* If we're in define mode, but not redefing the file, delete it. */
    if (zinfo->flags & NC_INDEF && !zinfo->redef)
    {
        delete_file++;
        strncpy(path, nc->path, NC_MAX_NAME);
    }

    /* Free any resources the netcdf-4 library has for this file's
     * metadata. */
    if ((stat = ncz_close_ncz_file(zinfo, 1)))
        return stat;

    /* Delete the file, if we should. */
    if (delete_file)
        if (remove(path) < 0)
            return NC_ECANTREMOVE;

    return NC_NOERR;
}

/**
 * @internal Close the netcdf file, writing any changes first.
 *
 * @param ncid File and group ID.
 * @param params any extra parameters in/out of close
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_close(int ncid, void* params)
{
    NC_GRP_INFO_T *grp;
    NC_FILE_INFO_T* h5;
    int stat = NC_NOERR;

    LOG((1, "%s: ncid 0x%x", __func__, ncid));

    /* Find our metadata for this file. */
    if ((stat = nc4_find_grp_h5(ncid, &grp, &h5)))
        return stat;

    assert(h5 && grp);

    /* This must be the root group. */
    if (grp->parent)
        return NC_EBADGRPID;

    /* Call the nc4 close. */
    if ((stat = ncz_close_netcdf4_file(h5, 0)))
        return stat;

    return NC_NOERR;
}

/**
 * @internal Learn number of dimensions, variables, global attributes,
 * and the ID of the first unlimited dimension (if any).
 *
 * @note It's possible for any of these pointers to be NULL, in which
 * case don't try to figure out that value.
 *
 * @param ncid File and group ID.
 * @param ndimsp Pointer that gets number of dimensions.
 * @param nvarsp Pointer that gets number of variables.
 * @param nattsp Pointer that gets number of global attributes.
 * @param unlimdimidp Pointer that gets first unlimited dimension ID,
 * or -1 if there are no unlimied dimensions.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp)
{
    NC *nc;
    NC_FILE_INFO_T* file;
    NC_GRP_INFO_T *grp;
    int stat = NC_NOERR;
    int i;

    LOG((2, "%s: ncid 0x%x", __func__, ncid));

    /* Find file metadata. */
    if ((stat = nc4_find_nc_grp_h5(ncid, &nc, &grp, &file)))
        return stat;

    assert(file && grp && nc);

    /* Count the number of dims, vars, and global atts; need to iterate
     * because of possible nulls. */
    if (ndimsp)
    {
        *ndimsp = ncindexcount(grp->dim);
    }
    if (nvarsp)
    {
        *nvarsp = ncindexcount(grp->vars);
    }
    if (nattsp)
    {
        /* Do we need to read the atts? */
        if (!grp->atts_read)
            if ((stat = ncz_read_atts(file,(NC_OBJ*)grp)))
                return stat;

        *nattsp = ncindexcount(grp->att);
    }

    if (unlimdimidp)
    {
        /* Default, no unlimited dimension */
        *unlimdimidp = -1;

        /* If there's more than one unlimited dim, which was not possible
           with netcdf-3, then only the last unlimited one will be reported
           back in xtendimp. */
        /* Note that this code is inconsistent with nc_inq_unlimid() */
        for(i=0;i<ncindexsize(grp->dim);i++) {
            NC_DIM_INFO_T* d = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
            if(d == NULL) continue;
            if(d->unlimited) {
                *unlimdimidp = d->hdr.id;
                break;
            }
        }
    }

    return NC_NOERR;
}

/**
 * @internal This function will do the enddef stuff for a netcdf-4 file.
 *
 * @param file Pointer to ZARR file info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOTINDEFINE Not in define mode.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_enddef_netcdf4_file(NC_FILE_INFO_T* file)
{
    assert(file);
    LOG((3, "%s", __func__));

    /* If we're not in define mode, return an error. */
    if (!(file->flags & NC_INDEF))
        return NC_ENOTINDEFINE;

    /* Turn define mode off. */
    file->flags ^= NC_INDEF;

    /* Redef mode needs to be tracked separately for nc_abort. */
    file->redef = NC_FALSE;

    return sync_netcdf4_file(file);
}

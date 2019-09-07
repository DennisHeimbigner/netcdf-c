/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See COPYRIGHT file for copying and redistribution
 * conditions. */
/**
 * @file
 * @internal This file contains functions that are used in file
 * opens.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "ncmodel.h"

#define NUM_TYPES 12 /**< Number of netCDF atomic types. */
#define CD_NELEMS_ZLIB 1 /**< Number of parameters needed for filter. */

#ifdef LOOK
/** @internal Native ZARR constants for atomic types. For performance,
 * fill this array only the first time, and keep it in global memory
 * for each further use. */
static hid_t h5_native_type_constant_g[NUM_TYPES];
#endif

/** @internal These flags may not be set for open mode. */
static const int ILLEGAL_OPEN_FLAGS = (NC_MMAP|NC_DISKLESS|NC_64BIT_OFFSET|NC_CDF5);

/* Forward */

#ifdef LOOK
/**
 * @internal struct to track ZARR object info, for
 * rec_read_metadata(). We get this info for every object in the
 * ZARR file when we H5Literate() over the file. */
typedef struct ncz_obj_info
{
    hid_t oid;                          /* ZARR object ID */
    char oname[NC_MAX_NAME + 1];        /* Name of object */
    H5G_stat_t statbuf;                 /* Information about the object */
    struct ncz_obj_info *next; /* Pointer to next node in list */
} ncz_obj_info_t;

/**
 * @internal User data struct for call to H5Literate() in
 * rec_read_metadata(). When iterating through the objects in a
 * group, if we find child groups, we save their ncz_obj_info_t
 * object in a list. Then we processes them after completely
 * processing the parent group. */
typedef struct user_data
{
    NClist *grps; /* NClist<ncz_obj_info_t*> */
    NC_GRP_INFO_T *grp; /* Pointer to parent group */
} user_data_t;

/* Custom iteration callback data */
typedef struct {
    NC_GRP_INFO_T *grp;
    NC_VAR_INFO_T *var;
} att_iter_info;
#endif /*LOOK*/

#ifdef LOOK
/**
 * @internal Given an ZARR type, set a pointer to netcdf type_info
 * struct, either an existing one (for user-defined types) or a newly
 * created one.
 *
 * @param h5 Pointer to file info struct.
 * @param datasetid ZARR dataset ID.
 * @param type_info Pointer to pointer that gets type info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EBADTYPID Type not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
get_type_info2(NC_FILE_INFO_T *h5, hid_t datasetid, NC_TYPE_INFO_T **type_info)
{
    NCZ_TYPE_INFO_T *ncz_type;
    htri_t is_str, equal = 0;
    H5T_class_t class;
    hid_t native_typeid, hdf_typeid;
    H5T_order_t order;
    int t;

    assert(h5 && type_info);
#ifde
    /* Because these N5T_NATIVE_* constants are actually function calls
     * (!) in H5Tpublic.h, I can't initialize this array in the usual
     * way, because at least some C compilers (like Irix) complain
     * about calling functions when defining constants. So I have to do
     * it like this. Note that there's no native types for char or
     * string. Those are handled later. */
    if (!h5_native_type_constant_g[1])
    {
        h5_native_type_constant_g[1] = H5T_NATIVE_SCHAR;
        h5_native_type_constant_g[2] = H5T_NATIVE_SHORT;
        h5_native_type_constant_g[3] = H5T_NATIVE_INT;
        h5_native_type_constant_g[4] = H5T_NATIVE_FLOAT;
        h5_native_type_constant_g[5] = H5T_NATIVE_DOUBLE;
        h5_native_type_constant_g[6] = H5T_NATIVE_UCHAR;
        h5_native_type_constant_g[7] = H5T_NATIVE_USHORT;
        h5_native_type_constant_g[8] = H5T_NATIVE_UINT;
        h5_native_type_constant_g[9] = H5T_NATIVE_LLONG;
        h5_native_type_constant_g[10] = H5T_NATIVE_ULLONG;
    }


    /* Get the ZARR typeid - we'll need it later. */
    if ((hdf_typeid = H5Dget_type(datasetid)) < 0)
        return NC_EHDFERR;

    /* Get the native typeid. Will be equivalent to hdf_typeid when
     * creating but not necessarily when reading, a variable. */
    if ((native_typeid = H5Tget_native_type(hdf_typeid, H5T_DIR_DEFAULT)) < 0)
        return NC_EHDFERR;

    /* Is this type an integer, string, compound, or what? */
    if ((class = H5Tget_class(native_typeid)) < 0)
        return NC_EHDFERR;

    /* Is this an atomic type? */
    if (class == H5T_STRING || class == H5T_INTEGER || class == H5T_FLOAT)
    {
        /* Allocate a phony NC_TYPE_INFO_T struct to hold type info. */
        if (!(*type_info = calloc(1, sizeof(NC_TYPE_INFO_T))))
            return NC_ENOMEM;

        /* Allocate storage for NCZ-specific type info. */
        if (!(ncz_type = calloc(1, sizeof(NCZ_TYPE_INFO_T))))
            return NC_ENOMEM;
        (*type_info)->format_type_info = ncz_type;

        /* H5Tequal doesn't work with H5T_C_S1 for some reason. But
         * H5Tget_class will return H5T_STRING if this is a string. */
        if (class == H5T_STRING)
        {
            if ((is_str = H5Tis_variable_str(native_typeid)) < 0)
                return NC_EHDFERR;
            /* Make sure fixed-len strings will work like variable-len
             * strings */
            if (is_str || H5Tget_size(hdf_typeid) > 1)
            {
                /* Set a class for the type */
                t = NUM_TYPES - 1;
                (*type_info)->nc_type_class = NC_STRING;
            }
            else
            {
                /* Set a class for the type */
                t = 0;
                (*type_info)->nc_type_class = NC_CHAR;
            }
        }
        else
        {
            for (t = 1; t < NUM_TYPES - 1; t++)
            {
                if ((equal = H5Tequal(native_typeid,
                                      h5_native_type_constant_g[t])) < 0)
                    return NC_EHDFERR;
                if (equal)
                    break;
            }

            /* Find out about endianness. As of HDF 1.8.6, this works
             * with all data types Not just H5T_INTEGER. See
             * https://www.hdfgroup.org/NCZ/doc/RM/RM_H5T.html#Datatype-GetOrder */
            if ((order = H5Tget_order(hdf_typeid)) < 0)
                return NC_EHDFERR;

            if (order == H5T_ORDER_LE)
                (*type_info)->endianness = NC_ENDIAN_LITTLE;
            else if (order == H5T_ORDER_BE)
                (*type_info)->endianness = NC_ENDIAN_BIG;
            else
                return NC_EBADTYPE;

            if (class == H5T_INTEGER)
                (*type_info)->nc_type_class = NC_INT;
            else
                (*type_info)->nc_type_class = NC_FLOAT;
        }
        (*type_info)->hdr.id = nc_type_constant_g[t];
        (*type_info)->size = nc_type_size_g[t];
        if (!((*type_info)->hdr.name = strdup(nc_type_name_g[t])))
            return NC_ENOMEM;
        ncz_type->hdf_typeid = hdf_typeid;
        ncz_type->native_hdf_typeid = native_typeid;
        return NC_NOERR;
    }
    else
    {
        NC_TYPE_INFO_T *type;

        /* This is a user-defined type. */
        if((type = ncz_rec_find_hdf_type(h5, native_typeid)))
            *type_info = type;

        /* The type entry in the array of user-defined types already has
         * an open data typeid (and native typeid), so close the ones we
         * opened above. */
        if (H5Tclose(native_typeid) < 0)
            return NC_EHDFERR;
        if (H5Tclose(hdf_typeid) < 0)
            return NC_EHDFERR;

        if (type)
            return NC_NOERR;
    }

    return NC_EBADTYPID;
}
#endif
#ifdef LOOK
/**
 * @internal For files without any netCDF-4 dimensions defined, create phony
 * dimension to match the available datasets.
 *
 * @param grp Pointer to the group info.
 * @param hdf_datasetid ZARR datsetid for the var's dataset.
 * @param var Pointer to the var info.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR ZARR returned an error.
 * @returns NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
create_phony_dims(NC_GRP_INFO_T *grp, hid_t hdf_datasetid, NC_VAR_INFO_T *var)
{
    NC_DIM_INFO_T *dim;
    hid_t spaceid = 0;
    hsize_t *h5dimlen = NULL, *h5dimlenmax = NULL;
    int dataset_ndims;
    int d;
    int stat = NC_NOERR;

    /* Find the space information for this dimension. */
    if ((spaceid = H5Dget_space(hdf_datasetid)) < 0)
        BAIL(NC_EHDFERR);

    /* Get the len of each dim in the space. */
    if (var->ndims)
    {
        /* Allocate storage for dim lens and max lens for this var. */
        if (!(h5dimlen = malloc(var->ndims * sizeof(hsize_t))))
            return NC_ENOMEM;
        if (!(h5dimlenmax = malloc(var->ndims * sizeof(hsize_t))))
            BAIL(NC_ENOMEM);

        /* Get ndims, also len and mac len of all dims. */
        if ((dataset_ndims = H5Sget_simple_extent_dims(spaceid, h5dimlen,
                                                       h5dimlenmax)) < 0)
            BAIL(NC_EHDFERR);
        assert(dataset_ndims == var->ndims);
    }
    else
    {
        /* Make sure it's scalar. */
        assert(H5Sget_simple_extent_type(spaceid) == H5S_SCALAR);
    }

    /* Create a phony dimension for each dimension in the dataset,
     * unless there already is one the correct size. */
    for (d = 0; d < var->ndims; d++)
    {
        int k;
        int match;

        /* Is there already a phony dimension of the correct size? */
        for (match=-1, k = 0; k < ncindexsize(grp->dim); k++)
        {
            dim = (NC_DIM_INFO_T *)ncindexith(grp->dim, k);
            assert(dim);
            if ((dim->len == h5dimlen[d]) &&
                ((h5dimlenmax[d] == H5S_UNLIMITED && dim->unlimited) ||
                 (h5dimlenmax[d] != H5S_UNLIMITED && !dim->unlimited)))
            {match = k; break;}
        }

        /* Didn't find a phony dim? Then create one. */
        if (match < 0)
        {
            char phony_dim_name[NC_MAX_NAME + 1];
            sprintf(phony_dim_name, "phony_dim_%d", grp->ncz_info->next_dimid);
            LOG((3, "%s: creating phony dim for var %s", __func__, var->hdr.name));

            /* Add phony dim to metadata list. */
            if ((stat = ncz_dim_list_add(grp, phony_dim_name, h5dimlen[d], -1, &dim)))
                BAIL(stat);

            /* Create struct for NCZ-specific dim info. */
            if (!(dim->format_dim_info = calloc(1, sizeof(NCZ_DIM_INFO_T))))
                BAIL(NC_ENOMEM);
            if (h5dimlenmax[d] == H5S_UNLIMITED)
                dim->unlimited = NC_TRUE;
        }

        /* The variable must remember the dimid. */
        var->dimids[d] = dim->hdr.id;
        var->dim[d] = dim;
    } /* next dim */

exit:
    /* Free resources. */
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    if (h5dimlenmax)
        free(h5dimlenmax);
    if (h5dimlen)
        free(h5dimlen);

    return stat;
}

/**
 * @internal Iterate through the vars in this file and make sure we've
 * got a dimid and a pointer to a dim for each dimension. This may
 * already have been done using the COORDINATES hidden attribute, in
 * which case this function will not have to do anything. This is
 * desirable because recurdively matching the dimscales (when
 * necessary) is very much the slowest part of opening a file.
 *
 * @param grp Pointer to group info struct.
 *
 * @returns NC_NOERR No error.
 * @returns NC_EHDFERR ZARR returned an error.
 * @returns NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
rec_match_dimscales(NC_GRP_INFO_T *grp)
{
    NC_VAR_INFO_T *var;
    NC_DIM_INFO_T *dim;
    int stat = NC_NOERR;
    int i;

    assert(grp && grp->hdr.name);
    LOG((4, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* Perform var dimscale match for child groups. */
    for (i = 0; i < ncindexsize(grp->children); i++)
        if ((stat = rec_match_dimscales((NC_GRP_INFO_T *)ncindexith(grp->children, i))))
            return stat;

    /* Check all the vars in this group. If they have dimscale info,
     * try and find a dimension for them. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
    {
        NCZ_VAR_INFO_T *ncz_var;
        int d;

        /* Get pointer to var and to the NCZ-specific var info. */
        var = (NC_VAR_INFO_T *)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);
        ncz_var = (NCZ_VAR_INFO_T *)var->format_var_info;

        /* Check all vars and see if dim[i] != NULL if dimids[i]
         * valid. Recall that dimids were initialized to -1. */
        for (d = 0; d < var->ndims; d++)
        {
            if (!var->dim[d])
                ncz_find_dim(grp, var->dimids[d], &var->dim[d], NULL);
        }

        /* Skip dimension scale variables */
        if (var->dimscale)
            continue;

        /* If we have already read hidden coordinates att, then we don't
         * have to match dimscales for this var. */
        if (var->coords_read)
            continue;

        /* Skip dimension scale variables */
        if (!var->dimscale)
        {
            int d;
            int j;

            /* Are there dimscales for this variable? */
            if (ncz_var->dimscale_ncz_objids)
            {
                for (d = 0; d < var->ndims; d++)
                {
                    NC_GRP_INFO_T *g;
                    nc_bool_t finished = NC_FALSE;
                    LOG((5, "%s: var %s has dimscale info...", __func__, var->hdr.name));

                    /* If we already have the dimension, we don't need to
                     * match the dimscales. This is better because matching
                     * the dimscales is slow. */
                    if (var->dim[d])
                        continue;

                    /* Now we have to try to match dimscales. Check this
                     * and parent groups. */
                    for (g = grp; g && !finished; g = g->parent)
                    {
                        /* Check all dims in this group. */
                        for (j = 0; j < ncindexsize(g->dim); j++)
                        {
                            /* Get the ZARR specific dim info. */
                            NCZ_DIM_INFO_T *ncz_dim;
                            dim = (NC_DIM_INFO_T *)ncindexith(g->dim, j);
                            assert(dim && dim->format_dim_info);
                            ncz_dim = (NCZ_DIM_INFO_T *)dim->format_dim_info;

                            /* Check for exact match of fileno/objid arrays
                             * to find identical objects in ZARR file. */
                            if (ncz_var->dimscale_ncz_objids[d].fileno[0] == ncz_dim->ncz_objid.fileno[0] &&
                                ncz_var->dimscale_ncz_objids[d].objno[0] == ncz_dim->ncz_objid.objno[0] &&
                                ncz_var->dimscale_ncz_objids[d].fileno[1] == ncz_dim->ncz_objid.fileno[1] &&
                                ncz_var->dimscale_ncz_objids[d].objno[1] == ncz_dim->ncz_objid.objno[1])
                            {
                                LOG((4, "%s: for dimension %d, found dim %s", __func__,
                                     d, dim->hdr.name));
                                var->dimids[d] = dim->hdr.id;
                                var->dim[d] = dim;
                                finished = NC_TRUE;
                                break;
                            }
                        } /* next dim */
                    } /* next grp */
                } /* next var->dim */
            }
            else
            {
                /* No dimscales for this var! Invent phony dimensions. */
                if ((stat = create_phony_dims(grp, ncz_var->hdf_datasetid, var)))
                    return stat;
            }
        }
    }

    return stat;
}
#endif /*LOOK*/

/**
 * @internal Check for the attribute that indicates that netcdf
 * classic model is in use.
 *
 * @param root_grp pointer to the group info for the root group of the
 * @param is_classic store 1 if this is a classic file.
 * file.
 *
 * @return NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
check_for_classic_model(NC_GRP_INFO_T *root_grp, int *is_classic)
{
    int attr_exists = 0;
    /* Check inputs. */
    assert(root_grp && root_grp->format_grp_info && !root_grp->parent
           && is_classic);

#ifdef LOOK
    /* Get the ZARR group id. */
    grpid = ((NCZ_GRP_INFO_T *)(root_grp->format_grp_info))->hdf_grpid;

    /* If this attribute exists in the root group, then classic model
     * is in effect. */
    if ((attr_exists = H5Aexists(grpid, NC3_STRICT_ATT_NAME)) < 0)
        return NC_EHDFERR;
#endif
    *is_classic = attr_exists ? 1 : 0;

    return NC_NOERR;
}

/**
 * @internal Open a netcdf-4 file. Things have already been kicked off
 * in ncfunc.c in nc_open, but here the netCDF-4 part of opening a
 * file is handled.
 *
 * @param path The file name of the new file.
 * @param mode The open mode flag.
 * @param parameters File parameters.
 * @param nc Pointer to NC file info.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EINTERNAL Internal list error.
 * @return ::NC_EHDFERR HDF error.
 * @return ::NC_EMPI MPI error for parallel.
 * @return ::NC_EPARINIT Parallel I/O initialization error.
 * @return ::NC_EINMEMMORY Memory file error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
ncz_open_file(const char *path, int mode, void* parameters, int ncid)
{
    int stat;
    NC_FILE_INFO_T *h5 = NULL;
    int is_classic;
    NC* nc = NULL;

    LOG((3, "%s: path %s mode %d", __func__, path, mode));
    assert(path);

    /* Find pointer to NC. */
    if ((stat = NC_check_id(ncid, &nc)))
        return stat;
    assert(nc && nc->model->impl == NC_FORMATX_ZARR);

    /* Add necessary structs to hold netcdf-4 file data;
       will define the NC_FILE_INFO_T for the file
       and the NC_GRP_INFO_T for the root group. */
    if ((stat = nc4_nc4f_list_add(nc, path, mode)))
        goto exit;
    h5 = (NC_FILE_INFO_T*)nc->dispatchdata;
    assert(h5 && h5->root_grp);

    h5->mem.inmemory = ((mode & NC_INMEMORY) == NC_INMEMORY);
    h5->mem.diskless = ((mode & NC_DISKLESS) == NC_DISKLESS);
    h5->mem.persist = ((mode & NC_PERSIST) == NC_PERSIST);

    /* Does the mode specify that this file is read-only? */
    if ((mode & NC_WRITE) == 0)
	h5->no_write = NC_TRUE;

    /* Setup zarr state */
    if((stat = ncz_open_dataset(h5,h5->root_grp)))
	goto exit;

    /* Now read in all the metadata. Some types
     * information may be difficult to resolve here, if, for example, a
     * dataset of user-defined type is encountered before the
     * definition of that type. */
    if((stat = ncz_read_file(h5)))
       goto exit;

    /* Check for classic model attribute. */
    if ((stat = check_for_classic_model(h5->root_grp, &is_classic)))
       goto exit;
    if (is_classic)
       h5->cmode |= NC_CLASSIC_MODEL;

    /* Set the provenance info for this file */
    if ((stat = NCZ_read_provenance(h5)))
       goto exit;

#ifdef LOGGING
    /* This will print out the names, types, lens, etc of the vars and
       atts in the file, if the logging level is 2 or greater. */
    log_metadata_nc(h5);
#endif

    return NC_NOERR;

exit:
    if (h5)
	ncz_close_file(h5, 1); /*  treat like abort*/
    return stat;
}

/**
 * @internal Open a netCDF-4 file.
 *
 * @param path The file name of the new file.
 * @param mode The open mode flag.
 * @param basepe Ignored by this function.
 * @param chunksizehintp Ignored by this function.
 * @param parameters pointer to struct holding extra data (e.g. for parallel I/O)
 * layer. Ignored if NULL.
 * @param dispatch Pointer to the dispatch table for this file.
 * @param nc_file Pointer to an instance of NC.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL Invalid inputs.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_open(const char *path, int mode, int basepe, size_t *chunksizehintp,
         void *parameters, const NC_Dispatch *dispatch, int ncid)
{
    NCURI* uri = NULL;
    NClist* allparams = nclistnew();

    assert(path && dispatch);

    LOG((1, "%s: path %s mode %d params %x",
         __func__, path, mode, parameters));

    /* Check the mode for validity */
    if (mode & ILLEGAL_OPEN_FLAGS)
        return NC_EINVAL;

    if((mode & NC_DISKLESS) && (mode & NC_INMEMORY))
        return NC_EINVAL;

    /* If this is our first file, initialize NCZ. */
    if (!ncz_initialized) NCZ_initialize();

#ifdef LOGGING
    /* If nc logging level has changed, see if we need to turn on
     * NCZ's error messages. */
    NCZ_set_log_level();
#endif /* LOGGING */

    /* collect all parameters */
    if(parameters != NULL) {
	/* Extract any parameters and add to allparams */
    }
    ncuriparse(path,&uri);
    if(uri != NULL) {
	const char** frags = ncurifragmentparams(uri);
	if(frags != NULL) {
	    for(;*frags;frags+=2) {
		nclistpush(allparams,strdup(frags[0]));
		nclistpush(allparams,strdup(frags[1]));
	    }
	}
    }
    /* Rebuild the path without any fragment parameters */
    path = ncuribuild(uri,NULL,NULL,NCURISVC);

    /* Open the file. */
    return ncz_open_file(path, mode, allparams, ncid);
}

#ifdef LOOK
/**
 * @internal Find out what filters are applied to this ZARR dataset,
 * fletcher32, deflate, and/or shuffle. All other filters are just
 * dumped The possible values of
 *
 * @param propid ID of ZARR var creation properties list.
 * @param var Pointer to NC_VAR_INFO_T for this variable.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int get_filter_info(hid_t propid, NC_VAR_INFO_T *var)
{
    H5Z_filter_t filter;
    int num_filters;
    unsigned int cd_values_zip[CD_NELEMS_ZLIB];
    size_t cd_nelems = CD_NELEMS_ZLIB;
    int f;

    assert(var);

    if ((num_filters = H5Pget_nfilters(propid)) < 0)
        return NC_EHDFERR;

    for (f = 0; f < num_filters; f++)
    {
        if ((filter = H5Pget_filter2(propid, f, NULL, &cd_nelems, cd_values_zip,
                                     0, NULL, NULL)) < 0)
            return NC_EHDFERR;
        switch (filter)
        {
        case H5Z_FILTER_SHUFFLE:
            var->shuffle = NC_TRUE;
            break;

        case H5Z_FILTER_FLETCHER32:
            var->fletcher32 = NC_TRUE;
            break;

        case H5Z_FILTER_DEFLATE:
            var->deflate = NC_TRUE;
            if (cd_nelems != CD_NELEMS_ZLIB ||
                cd_values_zip[0] > NC_MAX_DEFLATE_LEVEL)
                return NC_EHDFERR;
            var->deflate_level = cd_values_zip[0];
            break;

        case H5Z_FILTER_SZIP:
            /* Szip is tricky because the filter code expands the set of parameters from 2 to 4
               and changes some of the parameter values */
            var->filterid = filter;
            if(cd_nelems == 0)
                var->params = NULL;
            else {
                /* We have to re-read the parameters based on actual nparams,
                   which in the case of szip, differs from users original nparams */
                var->params = (unsigned int*)calloc(1,sizeof(unsigned int)*cd_nelems);
                if(var->params == NULL)
                    return NC_ENOMEM;
                if((filter = H5Pget_filter2(propid, f, NULL, &cd_nelems,
                                            var->params, 0, NULL, NULL)) < 0)
                    return NC_EHDFERR;
                /* fix up the parameters and the #params */
                var->nparams = cd_nelems;
            }
            break;

        default:
            var->filterid = filter;
            var->nparams = cd_nelems;
            if(cd_nelems == 0)
                var->params = NULL;
            else {
                /* We have to re-read the parameters based on actual nparams */
                var->params = (unsigned int*)calloc(1,sizeof(unsigned int)*var->nparams);
                if(var->params == NULL)
                    return NC_ENOMEM;
                if((filter = H5Pget_filter2(propid, f, NULL, &cd_nelems,
                                            var->params, 0, NULL, NULL)) < 0)
                    return NC_EHDFERR;
            }
            break;
        }
    }
    return NC_NOERR;
}
#endif

#ifdef LOOK
/**
 * @internal Learn if there is a fill value defined for a variable,
 * and, if so, its value.
 *
 * @param propid ID of ZARR var creation properties list.
 * @param var Pointer to NC_VAR_INFO_T for this variable.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int get_fill_info(hid_t propid, NC_VAR_INFO_T *var)
{
    H5D_fill_value_t fill_status;

    /* Is there a fill value associated with this dataset? */
    if (H5Pfill_value_defined(propid, &fill_status) < 0)
        return NC_EHDFERR;

    /* Get the fill value, if there is one defined. */
    if (fill_status == H5D_FILL_VALUE_USER_DEFINED)
    {
        /* Allocate space to hold the fill value. */
        if (!var->fill_value)
        {
            if (var->type_info->nc_type_class == NC_VLEN)
            {
                if (!(var->fill_value = malloc(sizeof(nc_vlen_t))))
                    return NC_ENOMEM;
            }
            else if (var->type_info->nc_type_class == NC_STRING)
            {
                if (!(var->fill_value = malloc(sizeof(char *))))
                    return NC_ENOMEM;
            }
            else
            {
                assert(var->type_info->size);
                if (!(var->fill_value = malloc(var->type_info->size)))
                    return NC_ENOMEM;
            }
        }

        /* Get the fill value from the ZARR property lust. */
        if (H5Pget_fill_value(propid, ((NCZ_TYPE_INFO_T *)var->type_info->format_type_info)->native_hdf_typeid,
                              var->fill_value) < 0)
            return NC_EHDFERR;
    }
    else
        var->no_fill = NC_TRUE;

    return NC_NOERR;
}
#endif

#ifdef LOOK
/**
 * @internal Learn the chunking settings of a var.
 *
 * @param propid ID of ZARR var creation properties list.
 * @param var Pointer to NC_VAR_INFO_T for this variable.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int get_chunking_info(hid_t propid, NC_VAR_INFO_T *var)
{
    H5D_layout_t layout;
    hsize_t chunksize[H5S_MAX_RANK] = {0};
    int d;

    /* Get the chunking info the var. */
    if ((layout = H5Pget_layout(propid)) < -1)
        return NC_EHDFERR;

    /* Remember the layout and, if chunked, the chunksizes. */
    if (layout == H5D_CHUNKED)
    {
        if (H5Pget_chunk(propid, H5S_MAX_RANK, chunksize) < 0)
            return NC_EHDFERR;
        if (!(var->chunksizes = malloc(var->ndims * sizeof(size_t))))
            return NC_ENOMEM;
        for (d = 0; d < var->ndims; d++)
            var->chunksizes[d] = chunksize[d];
    }
    else if (layout == H5D_CONTIGUOUS || layout == H5D_COMPACT)
        var->contiguous = NC_TRUE;

    return NC_NOERR;
}
#endif

#ifdef LOOK
/**
 * @internal This function reads scale info for vars, whether they
 * are scales or not.
 *
 * @param grp Pointer to group info struct.
 * @param dim Pointer to dim info struct if this is a scale, NULL
 * otherwise.
 * @param var Pointer to var info struct.
 * @param ncz_var Pointer to ZARR var info struct.
 * @param ndims Number of dims for this var.
 * @param datasetid ZARR datasetid.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EVARMETA Error with var metadata.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
get_scale_info(NC_GRP_INFO_T *grp, NC_DIM_INFO_T *dim, NC_VAR_INFO_T *var,
               NCZ_VAR_INFO_T *ncz_var, int ndims, hid_t datasetid)
{
    int stat;

    /* If it's a scale, mark it as such. */
    if (dim)
    {
        assert(ndims);
        var->dimscale = NC_TRUE;

        /* If this is a multi-dimensional coordinate var, then the
         * dimids must be stored in the hidden coordinates attribute. */
        if (var->ndims > 1)
        {
            if ((stat = read_coord_dimids(grp, var)))
                return stat;
        }
        else
        {
            /* This is a 1-dimensional coordinate var. */
            assert(!strcmp(var->hdr.name, dim->hdr.name));
            var->dimids[0] = dim->hdr.id;
            var->dim[0] = dim;
        }
        dim->coord_var = var;
    }
    else /* Not a scale. */
    {
        if (!var->coords_read)
            if ((stat = get_attached_info(var, ncz_var, ndims, datasetid)))
                return stat;
    }

    return NC_NOERR;
}
#endif

#ifdef LOOK
/**
 * @internal Get the metadata for a variable.
 *
 * @param var Pointer to var info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EVARMETA Error with var metadata.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
ncz_get_var_meta(NC_VAR_INFO_T *var)
{
    NCZ_VAR_INFO_T *ncz_var;
    hid_t access_pid = 0;
    hid_t propid = 0;
    double rdcc_w0;
    int stat = NC_NOERR;

    assert(var && var->format_var_info);
    LOG((3, "%s: var %s", __func__, var->hdr.name));

    /* Have we already read the var metadata? */
    if (var->meta_read)
        return NC_NOERR;

    /* Get pointer to the NCZ-specific var info struct. */
    ncz_var = (NCZ_VAR_INFO_T *)var->format_var_info;

    /* Get the current chunk cache settings. */
    if ((access_pid = H5Dget_access_plist(ncz_var->hdf_datasetid)) < 0)
        BAIL(NC_EVARMETA);

    /* Learn about current chunk cache settings. */
    if ((H5Pget_chunk_cache(access_pid, &(var->chunk_cache_nelems),
                            &(var->chunk_cache_size), &rdcc_w0)) < 0)
        BAIL(NC_EHDFERR);
    var->chunk_cache_preemption = rdcc_w0;

    /* Get the dataset creation properties. */
    if ((propid = H5Dget_create_plist(ncz_var->hdf_datasetid)) < 0)
        BAIL(NC_EHDFERR);

    /* Get var chunking info. */
    if ((stat = get_chunking_info(propid, var)))
        BAIL(stat);

    /* Get filter info for a var. */
    if ((stat = get_filter_info(propid, var)))
        BAIL(stat);

    /* Get fill value, if defined. */
    if ((stat = get_fill_info(propid, var)))
        BAIL(stat);

    /* Is this a deflated variable with a chunksize greater than the
     * current cache size? */
    if ((stat = ncz_adjust_var_cache(var->container, var)))
        BAIL(stat);

    if (var->coords_read && !var->dimscale)
        if ((stat = get_attached_info(var, ncz_var, var->ndims, ncz_var->hdf_datasetid)))
            return stat;

    /* Remember that we have read the metadata for this var. */
    var->meta_read = NC_TRUE;

exit:
    if (access_pid && H5Pclose(access_pid) < 0)
        BAIL2(NC_EHDFERR);
    if (propid > 0 && H5Pclose(propid) < 0)
        BAIL2(NC_EHDFERR);
    return stat;
}
#endif

#ifdef LOOK
/**
 * @internal This function is called by read_dataset(), (which is
 * called by rec_read_metadata()) when a netCDF variable is found in
 * the file. This function reads in all the metadata about the
 * var. Attributes are not read until the user asks for information
 * about one of them.
 *
 * @param grp Pointer to group info struct.
 * @param datasetid ZARR dataset ID.
 * @param obj_name Name of the ZARR object to read.
 * @param ndims Number of dimensions.
 * @param dim If non-NULL, then this var is a coordinate var for a
 * dimension, and this points to the info for that dimension.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EVARMETA Error with var metadata.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
read_var(NC_GRP_INFO_T *grp, hid_t datasetid, const char *obj_name,
         size_t ndims, NC_DIM_INFO_T *dim)
{
    NC_VAR_INFO_T *var = NULL;
    NCZ_VAR_INFO_T *ncz_var;
    int incr_id_rc = 0; /* Whether dataset ID's ref count has been incremented */
    char *finalname = NULL;
    int stat = NC_NOERR;

    assert(obj_name && grp);
    LOG((4, "%s: obj_name %s", __func__, obj_name));

    /* Check for a weird case: a non-coordinate variable that has the
     * same name as a dimension. It's legal in netcdf, and requires
     * that the ZARR dataset name be changed. */
    if (strlen(obj_name) > strlen(NON_COORD_PREPEND) &&
        !strncmp(obj_name, NON_COORD_PREPEND, strlen(NON_COORD_PREPEND)))
    {
        /* Allocate space for the name. */
        if (!(finalname = malloc(((strlen(obj_name) -
                                   strlen(NON_COORD_PREPEND))+ 1) * sizeof(char))))
            BAIL(NC_ENOMEM);
        strcpy(finalname, &obj_name[strlen(NON_COORD_PREPEND)]);
    } else
        finalname = strdup(obj_name);

    /* Add a variable to the end of the group's var list. */
    if ((stat = ncz_var_list_add(grp, finalname, ndims, &var)))
        BAIL(stat);

    /* Add storage for NCZ-specific var info. */
    if (!(var->format_var_info = calloc(1, sizeof(NCZ_VAR_INFO_T))))
        BAIL(NC_ENOMEM);
    ncz_var = (NCZ_VAR_INFO_T *)var->format_var_info;

    /* Fill in what we already know. */
    ncz_var->hdf_datasetid = datasetid;
    H5Iinc_ref(ncz_var->hdf_datasetid); /* Increment number of objects using ID */
    incr_id_rc++; /* Indicate that we've incremented the ref. count (for errors) */
    var->created = NC_TRUE;
    var->atts_read = 0;

    /* Try and read the dimids from the COORDINATES attribute. If it's
     * not present, we will have to do dimsscale matching to locate the
     * dims for this var. */
    stat = read_coord_dimids(grp, var);
    if (stat && stat != NC_ENOTATT)
        BAIL(stat);
    stat = NC_NOERR;

    /* Handle scale info. */
    if ((stat = get_scale_info(grp, dim, var, ncz_var, ndims, datasetid)))
        BAIL(stat);

    /* Learn all about the type of this variable. This will fail for
     * ZARR reference types, and then the var we just created will be
     * deleted, thus ignoring ZARR reference type objects. */
    if ((stat = get_type_info2(var->container->h5, ncz_var->hdf_datasetid,
                                 &var->type_info)))
        BAIL(stat);

    /* Indicate that the variable has a pointer to the type */
    var->type_info->rc++;

exit:
    if (finalname)
        free(finalname);
    if (stat)
    {
        /* If there was an error, decrement the dataset ref counter, and
         * delete the var info struct we just created. */
        if (incr_id_rc && H5Idec_ref(datasetid) < 0)
            BAIL2(NC_EHDFERR);
        if (var)
            ncz_var_list_del(grp, var);
    }

    return stat;
}
#endif

#ifdef LOOK
/**
 * @internal Given an ZARR type, set a pointer to netcdf type.
 *
 * @param h5 Pointer to file info struct.
 * @param native_typeid ZARR type ID.
 * @param xtype Pointer that gets netCDF type.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EBADTYPID Type not found.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
get_netcdf_type(NC_FILE_INFO_T *h5, hid_t native_typeid,
                nc_type *xtype)
{
    NC_TYPE_INFO_T *type;
    H5T_class_t class;
    htri_t is_str, equal = 0;

    assert(h5 && xtype);

    if ((class = H5Tget_class(native_typeid)) < 0)
        return NC_EHDFERR;

    /* H5Tequal doesn't work with H5T_C_S1 for some reason. But
     * H5Tget_class will return H5T_STRING if this is a string. */
    if (class == H5T_STRING)
    {
        if ((is_str = H5Tis_variable_str(native_typeid)) < 0)
            return NC_EHDFERR;
        if (is_str)
            *xtype = NC_STRING;
        else
            *xtype = NC_CHAR;
        return NC_NOERR;
    }
    else if (class == H5T_INTEGER || class == H5T_FLOAT)
    {
        /* For integers and floats, we don't have to worry about
         * endianness if we compare native types. */
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_SCHAR)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_BYTE;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_SHORT)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_SHORT;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_INT)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_INT;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_FLOAT)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_FLOAT;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_DOUBLE)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_DOUBLE;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_UCHAR)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_UBYTE;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_USHORT)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_USHORT;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_UINT)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_UINT;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_LLONG)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_INT64;
            return NC_NOERR;
        }
        if ((equal = H5Tequal(native_typeid, H5T_NATIVE_ULLONG)) < 0)
            return NC_EHDFERR;
        if (equal)
        {
            *xtype = NC_UINT64;
            return NC_NOERR;
        }
    }

    /* Maybe we already know about this type. */
    if (!equal)
        if((type = ncz_rec_find_hdf_type(h5, native_typeid)))
        {
            *xtype = type->hdr.id;
            return NC_NOERR;
        }

    *xtype = NC_NAT;
    return NC_EBADTYPID;
}

/**
 * @internal Read an attribute. This is called by
 * att_read_callbk().
 *
 * @param grp Pointer to group info struct.
 * @param attid Attribute ID.
 * @param att Pointer that gets att info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EATTMETA Att metadata error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
read_ncz_att(NC_GRP_INFO_T *grp, hid_t attid, NC_ATT_INFO_T *att)
{
    NCZ_ATT_INFO_T *ncz_att;
    hid_t spaceid = 0, file_typeid = 0;
    hsize_t dims[1] = {0}; /* netcdf attributes always 1-D. */
    size_t type_size;
    int att_ndims;
    hssize_t att_npoints;
    H5T_class_t att_class;
    int fixed_len_string = 0;
    size_t fixed_size = 0;
    int stat = NC_NOERR;

    assert(att && att->hdr.name && att->format_att_info);
    LOG((5, "%s: att->hdr.id %d att->hdr.name %s att->nc_typeid %d att->len %d",
         __func__, att->hdr.id, att->hdr.name, (int)att->nc_typeid, att->len));

    /* Get NCZ-sepecific info stuct for this attribute. */
    ncz_att = (NCZ_ATT_INFO_T *)att->format_att_info;

    /* Get type of attribute in file. */
    if ((file_typeid = H5Aget_type(attid)) < 0)
        return NC_EATTMETA;
    if ((ncz_att->native_hdf_typeid = H5Tget_native_type(file_typeid,
                                                          H5T_DIR_DEFAULT)) < 0)
        BAIL(NC_EHDFERR);
    if ((att_class = H5Tget_class(ncz_att->native_hdf_typeid)) < 0)
        BAIL(NC_EATTMETA);
    if (att_class == H5T_STRING &&
        !H5Tis_variable_str(ncz_att->native_hdf_typeid))
    {
        fixed_len_string++;
        if (!(fixed_size = H5Tget_size(ncz_att->native_hdf_typeid)))
            BAIL(NC_EATTMETA);
    }
    if ((stat = get_netcdf_type(grp->h5, ncz_att->native_hdf_typeid,
                                  &(att->nc_typeid))))
        BAIL(stat);


    /* Get len. */
    if ((spaceid = H5Aget_space(attid)) < 0)
        BAIL(NC_EATTMETA);
    if ((att_ndims = H5Sget_simple_extent_ndims(spaceid)) < 0)
        BAIL(NC_EATTMETA);
    if ((att_npoints = H5Sget_simple_extent_npoints(spaceid)) < 0)
        BAIL(NC_EATTMETA);

    /* If both att_ndims and att_npoints are zero, then this is a
     * zero length att. */
    if (att_ndims == 0 && att_npoints == 0)
        dims[0] = 0;
    else if (att->nc_typeid == NC_STRING)
        dims[0] = att_npoints;
    else if (att->nc_typeid == NC_CHAR)
    {
        /* NC_CHAR attributes are written as a scalar in NCZ, of type
         * H5T_C_S1, of variable length. */
        if (att_ndims == 0)
        {
            if (!(dims[0] = H5Tget_size(file_typeid)))
                BAIL(NC_EATTMETA);
        }
        else
        {
            /* This is really a string type! */
            att->nc_typeid = NC_STRING;
            dims[0] = att_npoints;
        }
    }
    else
    {
        H5S_class_t space_class;

        /* All netcdf attributes are scalar or 1-D only. */
        if (att_ndims > 1)
            BAIL(NC_EATTMETA);

        /* Check class of ZARR dataspace */
        if ((space_class = H5Sget_simple_extent_type(spaceid)) < 0)
            BAIL(NC_EATTMETA);

        /* Check for NULL ZARR dataspace class (should be weeded out
         * earlier) */
        if (H5S_NULL == space_class)
            BAIL(NC_EATTMETA);

        /* check for SCALAR ZARR dataspace class */
        if (H5S_SCALAR == space_class)
            dims[0] = 1;
        else /* Must be "simple" dataspace */
        {
            /* Read the size of this attribute. */
            if (H5Sget_simple_extent_dims(spaceid, dims, NULL) < 0)
                BAIL(NC_EATTMETA);
        }
    }

    /* Tell the user what the length if this attribute is. */
    att->len = dims[0];

    /* Allocate some memory if the len is not zero, and read the
       attribute. */
    if (dims[0])
    {
        if ((stat = ncz_get_typelen_mem(grp->h5, att->nc_typeid,
                                          &type_size)))
            return stat;
        if (att_class == H5T_VLEN)
        {
            if (!(att->vldata = malloc((unsigned int)(att->len * sizeof(hvl_t)))))
                BAIL(NC_ENOMEM);
            if (H5Aread(attid, ncz_att->native_hdf_typeid, att->vldata) < 0)
                BAIL(NC_EATTMETA);
        }
        else if (att->nc_typeid == NC_STRING)
        {
            if (!(att->stdata = calloc(att->len, sizeof(char *))))
                BAIL(NC_ENOMEM);
            /* For a fixed length ZARR string, the read requires
             * contiguous memory. Meanwhile, the netCDF API requires that
             * nc_free_string be called on string arrays, which would not
             * work if one contiguous memory block were used. So here I
             * convert the contiguous block of strings into an array of
             * malloced strings (each string with its own malloc). Then I
             * copy the data and free the contiguous memory. This
             * involves copying the data, which is bad, but this only
             * occurs for fixed length string attributes, and presumably
             * these are small. (And netCDF-4 does not create them - it
             * always uses variable length strings. */
            if (fixed_len_string)
            {
                int i;
                char *contig_buf, *cur;

                /* Alloc space for the contiguous memory read. */
                if (!(contig_buf = malloc(att->len * fixed_size * sizeof(char))))
                    BAIL(NC_ENOMEM);

                /* Read the fixed-len strings as one big block. */
                if (H5Aread(attid, ncz_att->native_hdf_typeid, contig_buf) < 0) {
                    free(contig_buf);
                    BAIL(NC_EATTMETA);
                }

                /* Copy strings, one at a time, into their new home. Alloc
                   space for each string. The user will later free this
                   space with nc_free_string. */
                cur = contig_buf;
                for (i = 0; i < att->len; i++)
                {
                    if (!(att->stdata[i] = malloc(fixed_size))) {
                        free(contig_buf);
                        BAIL(NC_ENOMEM);
                    }
                    strncpy(att->stdata[i], cur, fixed_size);
                    cur += fixed_size;
                }

                /* Free contiguous memory buffer. */
                free(contig_buf);
            }
            else
            {
                /* Read variable-length string atts. */
                if (H5Aread(attid, ncz_att->native_hdf_typeid, att->stdata) < 0)
                    BAIL(NC_EATTMETA);
            }
        }
        else
        {
            if (!(att->data = malloc((unsigned int)(att->len * type_size))))
                BAIL(NC_ENOMEM);
            if (H5Aread(attid, ncz_att->native_hdf_typeid, att->data) < 0)
                BAIL(NC_EATTMETA);
        }
    }

    if (H5Tclose(file_typeid) < 0)
        BAIL(NC_EHDFERR);
    if (H5Sclose(spaceid) < 0)
        return NC_EHDFERR;

    return NC_NOERR;

exit:
    if (H5Tclose(file_typeid) < 0)
        BAIL2(NC_EHDFERR);
    if (spaceid > 0 && H5Sclose(spaceid) < 0)
        BAIL2(NC_EHDFERR);
    return stat;
}

/**
 * @internal Wrap ZARR allocated memory free operations
 *
 * @param memory Pointer to memory to be freed.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static void
nczfree(void* memory)
{
#ifndef JNA
    /* On Windows using the microsoft runtime, it is an error
       for one library to free memory allocated by a different library.*/
#ifdef HAVE_H5FREE_MEMORY
    if(memory != NULL) H5free_memory(memory);
#else
#ifndef _WIN32
    if(memory != NULL) free(memory);
#endif
#endif
#endif
}
#endif /*LOOK*/

#ifdef LOOK
/**
 * @internal Read information about a user defined type from the NCZ
 * file, and stash it in the group's list of types.
 *
 * @param grp Pointer to group info struct.
 * @param hdf_typeid ZARR type ID.
 * @param type_name Pointer that gets the type name.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EHDFERR ZARR returned error.
 * @return ::NC_EBADTYPID Type not found.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
read_type(NC_GRP_INFO_T *grp, hid_t hdf_typeid, char *type_name)
{
    NC_TYPE_INFO_T *type;
    NCZ_TYPE_INFO_T *ncz_type;
    H5T_class_t class;
    hid_t native_typeid;
    size_t type_size;
    int nmembers;
    int stat;

    assert(grp && type_name);

    LOG((4, "%s: type_name %s grp->hdr.name %s", __func__, type_name,
         grp->hdr.name));

    /* What is the native type for this platform? */
    if ((native_typeid = H5Tget_native_type(hdf_typeid, H5T_DIR_DEFAULT)) < 0)
        return NC_EHDFERR;

    /* What is the size of this type on this platform. */
    if (!(type_size = H5Tget_size(native_typeid)))
        return NC_EHDFERR;
    LOG((5, "type_size %d", type_size));

    /* Add to the list for this new type, and get a local pointer to it. */
    if ((stat = ncz_type_list_add(grp, type_size, type_name, &type)))
        return stat;

    /* Allocate storage for NCZ-specific type info. */
    if (!(ncz_type = calloc(1, sizeof(NCZ_TYPE_INFO_T))))
        return NC_ENOMEM;
    type->format_type_info = ncz_type;

    /* Remember NCZ-specific type info. */
    ncz_type->hdf_typeid = hdf_typeid;
    ncz_type->native_hdf_typeid = native_typeid;

    /* Remember we have committed this type. */
    type->committed = NC_TRUE;

    /* Increment number of objects using ID. */
    if (H5Iinc_ref(ncz_type->hdf_typeid) < 0)
        return NC_EHDFERR;

    /* What is the class of this type, compound, vlen, etc. */
    if ((class = H5Tget_class(hdf_typeid)) < 0)
        return NC_EHDFERR;
    switch (class)
    {
    case H5T_STRING:
        type->nc_type_class = NC_STRING;
        break;

    case H5T_COMPOUND:
    {
        int nmembers;
        unsigned int m;
        char* member_name = NULL;
#ifdef JNA
        char jna[1001];
#endif

        type->nc_type_class = NC_COMPOUND;

        if ((nmembers = H5Tget_nmembers(hdf_typeid)) < 0)
            return NC_EHDFERR;
        LOG((5, "compound type has %d members", nmembers));
        type->u.c.field = nclistnew();
        nclistsetalloc(type->u.c.field,nmembers);

        for (m = 0; m < nmembers; m++)
        {
            hid_t member_hdf_typeid;
            hid_t member_native_typeid;
            size_t member_offset;
            H5T_class_t mem_class;
            nc_type member_xtype;

            /* Get the typeid and native typeid of this member of the
             * compound type. */
            if ((member_hdf_typeid = H5Tget_member_type(native_typeid, m)) < 0)
                return NC_EHDFERR;

            if ((member_native_typeid = H5Tget_native_type(member_hdf_typeid,
                                                           H5T_DIR_DEFAULT)) < 0)
                return NC_EHDFERR;

            /* Get the name of the member.*/
            member_name = H5Tget_member_name(native_typeid, m);
            if (!member_name || strlen(member_name) > NC_MAX_NAME) {
                stat = NC_EBADNAME;
                break;
            }
#ifdef JNA
            else {
                strncpy(jna,member_name,1000);
                member_name = jna;
            }
#endif

            /* Offset in bytes on *this* platform. */
            member_offset = H5Tget_member_offset(native_typeid, m);

            /* Get dimensional data if this member is an array of something. */
            if ((mem_class = H5Tget_class(member_hdf_typeid)) < 0)
                return NC_EHDFERR;
            if (mem_class == H5T_ARRAY)
            {
                int ndims, dim_size[NC_MAX_VAR_DIMS];
                hsize_t dims[NC_MAX_VAR_DIMS];
                int d;

                if ((ndims = H5Tget_array_ndims(member_hdf_typeid)) < 0)
                    return NC_EHDFERR;

                if (H5Tget_array_dims(member_hdf_typeid, dims, NULL) != ndims)
                    return NC_EHDFERR;

                for (d = 0; d < ndims; d++)
                    dim_size[d] = dims[d];

                /* What is the netCDF typeid of this member? */
                if ((stat = get_netcdf_type(grp->h5, H5Tget_super(member_hdf_typeid),
                                              &member_xtype)))
                    return stat;

                /* Add this member to our list of fields in this compound type. */
                if ((stat = ncz_field_list_add(type, member_name, member_offset,
                                                 member_xtype, ndims, dim_size)))
                    return stat;
            }
            else
            {
                /* What is the netCDF typeid of this member? */
                if ((stat = get_netcdf_type(grp->h5, member_native_typeid,
                                              &member_xtype)))
                    return stat;

                /* Add this member to our list of fields in this compound type. */
                if ((stat = ncz_field_list_add(type, member_name, member_offset,
                                                 member_xtype, 0, NULL)))
                    return stat;
            }

            nczfree(member_name);
        }
    }
    break;

    case H5T_VLEN:
    {
        htri_t ret;

        /* For conveninence we allow user to pass vlens of strings
         * with null terminated strings. This means strings are
         * treated slightly differently by the API, although they are
         * really just VLENs of characters. */
        if ((ret = H5Tis_variable_str(hdf_typeid)) < 0)
            return NC_EHDFERR;
        if (ret)
            type->nc_type_class = NC_STRING;
        else
        {
            hid_t base_hdf_typeid;
            nc_type base_nc_type = NC_NAT;

            type->nc_type_class = NC_VLEN;

            /* Find the base type of this vlen (i.e. what is this a
             * vlen of?) */
            if (!(base_hdf_typeid = H5Tget_super(native_typeid)))
                return NC_EHDFERR;

            /* What size is this type? */
            if (!(type_size = H5Tget_size(base_hdf_typeid)))
                return NC_EHDFERR;

            /* What is the netcdf corresponding type. */
            if ((stat = get_netcdf_type(grp->h5, base_hdf_typeid,
                                          &base_nc_type)))
                return stat;
            LOG((5, "base_hdf_typeid 0x%x type_size %d base_nc_type %d",
                 base_hdf_typeid, type_size, base_nc_type));

            /* Remember the base type for this vlen. */
            type->u.v.base_nc_typeid = base_nc_type;
        }
    }
    break;

    case H5T_OPAQUE:
        type->nc_type_class = NC_OPAQUE;
        break;

    case H5T_ENUM:
    {
        hid_t base_hdf_typeid;
        nc_type base_nc_type = NC_NAT;
        void *value;
        int i;
        char *member_name = NULL;
#ifdef JNA
        char jna[1001];
#endif

        type->nc_type_class = NC_ENUM;

        /* Find the base type of this enum (i.e. what is this a
         * enum of?) */
        if (!(base_hdf_typeid = H5Tget_super(hdf_typeid)))
            return NC_EHDFERR;
        /* What size is this type? */
        if (!(type_size = H5Tget_size(base_hdf_typeid)))
            return NC_EHDFERR;
        /* What is the netcdf corresponding type. */
        if ((stat = get_netcdf_type(grp->h5, base_hdf_typeid,
                                      &base_nc_type)))
            return stat;
        LOG((5, "base_hdf_typeid 0x%x type_size %d base_nc_type %d",
             base_hdf_typeid, type_size, base_nc_type));

        /* Remember the base type for this enum. */
        type->u.e.base_nc_typeid = base_nc_type;

        /* Find out how many member are in the enum. */
        if ((nmembers = H5Tget_nmembers(hdf_typeid)) < 0)
            return NC_EHDFERR;
        type->u.e.enum_member = nclistnew();
        nclistsetalloc(type->u.e.enum_member,nmembers);

        /* Allocate space for one value. */
        if (!(value = calloc(1, type_size)))
            return NC_ENOMEM;

        /* Read each name and value defined in the enum. */
        for (i = 0; i < nmembers; i++)
        {
            /* Get the name and value from NCZ. */
            if (!(member_name = H5Tget_member_name(hdf_typeid, i)))
                return NC_EHDFERR;

#ifdef JNA
            strncpy(jna,member_name,1000);
            member_name = jna;
#endif

            if (strlen(member_name) > NC_MAX_NAME)
                return NC_EBADNAME;

            if (H5Tget_member_value(hdf_typeid, i, value) < 0)
                return NC_EHDFERR;

            /* Insert new field into this type's list of fields. */
            if ((stat = ncz_enum_member_add(type, type->size,
                                              member_name, value)))
                return stat;

            nczfree(member_name);
        }
        free(value);
    }
    break;

    default:
        LOG((0, "unknown class"));
        return NC_EBADCLASS;
    }
    return stat;
}

/**
 * @internal Callback function for reading attributes. This is used
 * for both global and variable attributes.
 *
 * @param loc_id ZARR attribute ID.
 * @param att_name Name of the attrigute.
 * @param ainfo ZARR info struct for attribute.
 * @param att_data Pointer to an att_iter_info struct, which contains
 * pointers to the NC_GRP_INFO_T and (for variable attributes) the
 * NC_VAR_INFO_T. For global atts the var pointer is NULL.
 *
 * @return ::NC_NOERR No error. Iteration continues.
 * @return ::-1 Error. Stop iteration.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static herr_t
att_read_callbk(hid_t loc_id, const char *att_name, const H5A_info_t *ainfo,
                void *att_data)
{

    hid_t attid = 0;
    NC_ATT_INFO_T *att;
    NCindex *list;
    att_iter_info *att_info = (att_iter_info *)att_data;
    int stat = NC_NOERR;

    /* Determin what list is being added to. */
    list = att_info->var ? att_info->var->att : att_info->grp->att;

    /* This may be an attribute telling us that strict netcdf-3 rules
     * are in effect. If so, we will make note of the fact, but not add
     * this attribute to the metadata. It's not a user attribute, but
     * an internal netcdf-4 one. */
    if (!strcmp(att_name, NC3_STRICT_ATT_NAME))
    {
        /* Only relevant for groups, not vars. */
        if (!att_info->var)
            att_info->grp->h5->cmode |= NC_CLASSIC_MODEL;
        return NC_NOERR;
    }

    /* Should we ignore this attribute? */
    if (NC_findreserved(att_name))
        return NC_NOERR;

    /* Add to the end of the list of atts for this var. */
    if ((stat = ncz_att_list_add(list, att_name, &att)))
        BAIL(-1);

    /* Allocate storage for the ZARR specific att info. */
    if (!(att->format_att_info = calloc(1, sizeof(NCZ_ATT_INFO_T))))
        BAIL(-1);

    /* Open the att by name. */
    if ((attid = H5Aopen(loc_id, att_name, H5P_DEFAULT)) < 0)
        BAIL(-1);
    LOG((4, "%s::  att_name %s", __func__, att_name));

    /* Read the rest of the info about the att,
     * including its values. */
    if ((stat = read_ncz_att(att_info->grp, attid, att)))
        BAIL(stat);

    if (att)
        att->created = NC_TRUE;

exit:
    if (stat == NC_EBADTYPID)
    {
        /* NC_EBADTYPID will be normally converted to NC_NOERR so that
           the parent iterator does not fail. */
        stat = ncz_att_list_del(list, att);
        att = NULL;
    }
    if (attid > 0 && H5Aclose(attid) < 0)
        stat = -1;

    /* Since this is a ZARR iterator callback, return -1 for any error
     * to stop iteration. */
    if (stat)
        stat = -1;
    return stat;
}
#endif /*LOOK*/

#ifdef LOOK
/**
 * @internal Read a dataset. This function is called when
 * read_ncz_obj() encounters a  dataset when opening a file.
 *
 * @param grp Pointer to group info struct.
 * @param datasetid ZARR dataset ID.
 * @param obj_name Object name.
 * @param statbuf ZARR status buffer.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EHDFERR ZARR returned error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
read_dataset(NC_GRP_INFO_T *grp, hid_t datasetid, const char *obj_name,
             const H5G_stat_t *statbuf)
{
    NC_DIM_INFO_T *dim = NULL;   /* Dimension created for scales */
    NCZ_DIM_INFO_T *ncz_dim;
    hid_t spaceid = 0;
    int ndims;
    htri_t is_scale;
    int stat = NC_NOERR;

    /* Get the dimension information for this dataset. */
    if ((spaceid = H5Dget_space(datasetid)) < 0)
        BAIL(NC_EHDFERR);
    if ((ndims = H5Sget_simple_extent_ndims(spaceid)) < 0)
        BAIL(NC_EHDFERR);

    /* Is this a dimscale? */
    if ((is_scale = H5DSis_scale(datasetid)) < 0)
        BAIL(NC_EHDFERR);
    if (is_scale)
    {
        hsize_t dims[H5S_MAX_RANK];
        hsize_t max_dims[H5S_MAX_RANK];

        /* Query the scale's size & max. size */
        if (H5Sget_simple_extent_dims(spaceid, dims, max_dims) < 0)
            BAIL(NC_EHDFERR);

        /* Read the scale information. */
        if ((stat = read_scale(grp, datasetid, obj_name, statbuf, dims[0],
                                 max_dims[0], &dim)))
            BAIL(stat);
        ncz_dim = (NCZ_DIM_INFO_T *)dim->format_dim_info;
    }

    /* Add a var to the linked list, and get its metadata,
     * unless this is one of those funny dimscales that are a
     * dimension in netCDF but not a variable. (Spooky!) */
    if (!dim || (dim && !ncz_dim->hdf_dimscaleid))
        if ((stat = read_var(grp, datasetid, obj_name, ndims, dim)))
            BAIL(stat);

exit:
    if (spaceid && H5Sclose(spaceid) <0)
        BAIL2(stat);

    return stat;
}

/**
 * @internal Add ZARR object info for a group to a list for later
 * processing. We do this when we encounter groups, so that the parent
 * group can be fully processed before the child groups.
 *
 * @param udata Pointer to the user data, in this case a
 * user_data_t.
 * @param oinfo The ZARR object info.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM Out of memory.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
oinfo_list_add(user_data_t *udata, const ncz_obj_info_t *oinfo)
{
    ncz_obj_info_t *new_oinfo;    /* Pointer to info for object */

    /* Allocate memory for the object's info. */
    if (!(new_oinfo = calloc(1, sizeof(ncz_obj_info_t))))
        return NC_ENOMEM;

    /* Make a copy of the object's info. */
    memcpy(new_oinfo, oinfo, sizeof(ncz_obj_info_t));

    /* Add it to the list for future processing. */
    nclistpush(udata->grps, new_oinfo);

    return NC_NOERR;
}

/**
 * @internal Callback function called by H5Literate() for every NCZ
 * object in the file.
 *
 * @note This function is called by ZARR so does not return a netCDF
 * error code.
 *
 * @param grpid ZARR group ID.
 * @param name Name of object.
 * @param info Info struct for object.
 * @param _op_data Pointer to user data, a user_data_t. It will
 * contain a pointer to the current group and a list of
 * ncz_obj_info_t. Any child groups will get their ncz_obj_info
 * added to this list.
 *
 * @return H5_ITER_CONT No error, continue iteration.
 * @return H5_ITER_ERROR ZARR error, stop iteration.
 * @author Dennis Heimbigner, Ed Hartnett
 */
static int
read_ncz_obj(hid_t grpid, const char *name, const H5L_info_t *info,
              void *_op_data)
{
    /* Pointer to user data for callback */
    user_data_t *udata = (user_data_t *)_op_data;
    ncz_obj_info_t oinfo;    /* Pointer to info for object */
    int stat = H5_ITER_CONT;

    /* Open this critter. */
    if ((oinfo.oid = H5Oopen(grpid, name, H5P_DEFAULT)) < 0)
        BAIL(H5_ITER_ERROR);

    /* Get info about the object.*/
    if (H5Gget_objinfo(oinfo.oid, ".", 1, &oinfo.statbuf) < 0)
        BAIL(H5_ITER_ERROR);

    strncpy(oinfo.oname, name, NC_MAX_NAME);

    /* Add object to list, for later */
    switch(oinfo.statbuf.type)
    {
    case H5G_GROUP:
        LOG((3, "found group %s", oinfo.oname));

        /* Defer descending into child group immediately, so that the
         * types in the current group can be processed and be ready for
         * use by vars in the child group(s). */
        if (oinfo_list_add(udata, &oinfo))
            BAIL(H5_ITER_ERROR);
        break;

    case H5G_DATASET:
        LOG((3, "found dataset %s", oinfo.oname));

        /* Learn all about this dataset, which may be a dimscale
         * (i.e. dimension metadata), or real data. */
        if ((stat = read_dataset(udata->grp, oinfo.oid, oinfo.oname,
                                   &oinfo.statbuf)))
        {
            /* Allow NC_EBADTYPID to transparently skip over datasets
             * which have a datatype that netCDF-4 doesn't undertand
             * (currently), but break out of iteration for other
             * errors. */
            if (stat != NC_EBADTYPID)
                BAIL(H5_ITER_ERROR);
            else
                stat = H5_ITER_CONT;
        }

        /* Close the object */
        if (H5Oclose(oinfo.oid) < 0)
            BAIL(H5_ITER_ERROR);
        break;

    case H5G_TYPE:
        LOG((3, "found datatype %s", oinfo.oname));

        /* Process the named datatype */
        if (read_type(udata->grp, oinfo.oid, oinfo.oname))
            BAIL(H5_ITER_ERROR);

        /* Close the object */
        if (H5Oclose(oinfo.oid) < 0)
            BAIL(H5_ITER_ERROR);
        break;

    default:
        LOG((0, "Unknown object class %d in %s!", oinfo.statbuf.type, __func__));
        BAIL(H5_ITER_ERROR);
    }

exit:
    if (stat)
    {
        if (oinfo.oid > 0 && H5Oclose(oinfo.oid) < 0)
            BAIL2(H5_ITER_ERROR);
    }

    return (stat);
}
#endif /*LOOK*/

#if 0
/**
 * @internal This is the main function to recursively read and
 * define all the group metadata, but not their content
 * @param file Pointer to a containing dataset
 * @param grp Pointer to a group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ECANTWRITE File must be opened read-only.
 * @author Dennis Heimbigner
 */
static int
rec_read_groups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int i, stat = NC_NOERR;

    assert(grp && grp->hdr.name && grp->format_grp_info);
    LOG((3, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* create all subgroups of grp */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* subg = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	/* Create the corresponding .zgroup object */
	if((stat=ncz_sync_grp_meta(file,subg)))
	    goto done;
    }    
    /* Recurse down */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* subg = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = rec_read_groups(file,subg)))
	    goto done;
    }

done:
    return stat;
}

/**
 * @internal This is the main function to recursively walk al
 * groups and sync their metadata content: dims, types, vars.
 * The info comes from the map.
 * @param file Pointer to a containing dataset
 * @param grp Pointer to a group.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ECANTWRITE File must be opened read-only.
 * @author Dennis Heimbigner
 */
static int
rec_read_group_content(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int i, stat = NC_NOERR;

    assert(grp && grp->hdr.name && grp->format_grp_info);
    LOG((3, "%s: grp->hdr.name %s", __func__, grp->hdr.name));

    /* Fill in this group's content */

    /* Dimensions */
    if((stat = ncz_read_dims(file,grp)))
	goto done;

    /* Types */
    /* NO-OP for now */

    /* Group level attributes */

    if((stat = ncz_read_atts(file,(NC_OBJ*)grp)))
	goto done;

    /* Variables */
    if((stat = ncz_read_vars(file,grp)))
	goto done;

    /* Iterate to read variable attributes */
    for(i=0;i<ncindexsize(grp->vars);i++) {
	NC_VAR_INFO_T* v = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
        if((stat = ncz_read_atts(file,(NC_OBJ*)v)))
	    goto done;
    }

    /* Recurse to walk all sub groups breadth first */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* subg = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = rec_read_group_content(file,subg)))
	    goto done;
    }

done:
    return stat;
}

/**
 * @internal This is the main function to recursively read all the
 * metadata for the file. The links in the 'grp' are iterated over and
 * added to the file's metadata information. Note that child groups
 * are not immediately processed, but are deferred until all the other
 * links in the group are handled (so that vars in the child groups
 * are guaranteed to have types that they use in a parent group in
 * place).
 *
 * @param grp Pointer to a file
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EHDFERR ZARR error.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_ECANTWRITE File must be opened read-only.
 * @author Dennis Heimbigner
 */
static int
rec_read_metadata(NC_FILE_INFO_T* file)
{
    int i, stat = NC_NOERR;

    assert(file && file->format_file_info);
    LOG((3, "%s: file->controller->path %s", __func__, file->controller->path));

    /* Step1: define all user-defined types. Since user-defined types
       are not yet implemented, this is a no-op.
    */

    /* Step 2: Walk depth-first all the groups to define
       dimensions and attributes and variables */

#ifdef LOOK
    /* Iterate over links in this group, building lists for the types,
     * datasets and groups encountered. A pointer to udata will be
     * passed as a parameter to the callback function
     * read_ncz_obj(). (I have also tried H5Oiterate(), but it is much
     * slower iterating over the same file - Ed.) */
    if (H5Literate(zgrp->hdf_grpid, iter_index, H5_ITER_INC, &idx,
                   read_ncz_obj, (void *)&udata) < 0)
        BAIL(NC_EHDFERR);

    /* Process the child groups found. (Deferred until now, so that the
     * types in the current group get processed and are available for
     * vars in the child group(s).) */
    for (i = 0; i < nclistlength(udata.grps); i++)
    {
        NC_GRP_INFO_T *child_grp;
        oinfo = (ncz_obj_info_t*)nclistget(udata.grps, i);

        /* Add group to file's hierarchy. */
        if ((stat = zgrp_list_add(grp->h5, grp, oinfo->oname,
                                       &child_grp)))
            BAIL(stat);

        /* Allocate storage for NCZ-specific group info. */
        if (!(child_grp->format_grp_info = calloc(1, sizeof(ZGRP_INFO_T))))
            return NC_ENOMEM;

        /* Recursively read the child group's metadata. */
        if ((stat = rec_read_metadata(child_grp)))
            BAIL(stat);
    }
#endif /*LOOK*/

    /* When reading existing file, mark all variables as written. */
    for (i = 0; i < ncindexsize(grp->vars); i++)
        ((NC_VAR_INFO_T *)ncindexith(grp->vars, i))->written_to = NC_TRUE;

exit:
    if (pid > 0 && H5Pclose(pid) < 0)
        BAIL2(NC_EHDFERR);

    /* Clean up list of child groups. */
    for (i = 0; i < nclistlength(udata.grps); i++)
    {
        oinfo = (ncz_obj_info_t *)nclistget(udata.grps, i);
        /* Close the open ZARR object. */
        if (H5Oclose(oinfo->oid) < 0)
            BAIL2(NC_EHDFERR);
        free(oinfo);
    }
    nclistfree(udata.grps);

    return stat;
}
#endif /*0*/


/* Copyright 2005-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal This header file contains prototypes and initialization
 * for the ZARR dispatch layer.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "config.h"
#include "nczinternal.h"

static const NC_Dispatch NCZ_dispatcher = {

    NC_FORMATX_ZARR,

    NCZ_create,
    NCZ_open,

    NCZ_redef,
    NCZ__enddef,
    NCZ_sync,
    NCZ_abort,
    NCZ_close,
    NCZ_set_fill,
    NC_NOTNC3_inq_base_pe,
    NC_NOTNC3_set_base_pe,
    NCZ_inq_format,
    NCZ_inq_format_extended,

    NCZ_inq,
    NCZ_inq_type,

    NCZ_def_dim,
    NCZ_inq_dimid,
    NCZ_inq_dim,
    NCZ_inq_unlimdim,
    NCZ_rename_dim,

    NCZ_inq_att,
    NCZ_inq_attid,
    NCZ_inq_attname,
    NCZ_rename_att,
    NCZ_del_att,
    NCZ_get_att,
    NCZ_put_att,

    NCZ_def_var,
    NCZ_inq_varid,
    NCZ_rename_var,
    NCZ_get_vara,
    NCZ_put_vara,
    NCZ_get_vars,
    NCZ_put_vars,
    NCDEFAULT_get_varm,
    NCDEFAULT_put_varm,

    NCZ_inq_var_all,

    NCZ_var_par_access,
    NCZ_def_var_fill,

    NCZ_show_metadata,
    NCZ_inq_unlimdims,

    NCZ_inq_ncid,
    NCZ_inq_grps,
    NCZ_inq_grpname,
    NCZ_inq_grpname_full,
    NCZ_inq_grp_parent,
    NCZ_inq_grp_full_ncid,
    NCZ_inq_varids,
    NCZ_inq_dimids,
    NCZ_inq_typeids,
    NCZ_inq_type_equal,
    NCZ_def_grp,
    NCZ_rename_grp,
    NCZ_inq_user_type,
    NCZ_inq_typeid,

    NCZ_def_compound,
    NCZ_insert_compound,
    NCZ_insert_array_compound,
    NCZ_inq_compound_field,
    NCZ_inq_compound_fieldindex,
    NCZ_def_vlen,
    NCZ_put_vlen_element,
    NCZ_get_vlen_element,
    NCZ_def_enum,
    NCZ_insert_enum,
    NCZ_inq_enum_member,
    NCZ_inq_enum_ident,
    NCZ_def_opaque,
    NCZ_def_var_deflate,
    NCZ_def_var_fletcher32,
    NCZ_def_var_chunking,
    NCZ_def_var_endian,
    NCZ_def_var_filter,
    NCZ_set_var_chunk_cache,
    NCZ_get_var_chunk_cache,

};

const NC_Dispatch* NCZ_dispatch_table = NULL; /* moved here from ddispatch.c */

/**
 * @internal Initialize the ZARR dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int
NCZ_initialize(void)
{
    NCZ_dispatch_table = &NCZ_dispatcher;
    if (!ncz_initialized)
        ncz_initialize();
    return NCZ_provenance_init();
}

/**
 * @internal Finalize the ZARR dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_finalize(void)
{
    (void)ncz_finalize();
    return NC_NOERR;
}

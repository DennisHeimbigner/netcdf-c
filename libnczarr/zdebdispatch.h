/* Copyright 2005-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file
 * @internal This header file contains prototypes and initialization
 * for the ZARR dispatch layer.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"

static int NCZDB_create(const char *path, int cmode, size_t initialsz, int basepe, size_t *chunksizehintp, void *parameters, const struct NC_Dispatch *table, int ncid)
{
    return THROW(NCZ_create(path,cmode,initialsz,basepe,chunksizehintp,parameters,table,ncid));
}

static int NCZDB_open(const char *path, int mode, int basepe, size_t *chunksizehintp,void *parameters, const struct NC_Dispatch *table, int ncid)
{
    return THROW(NCZ_open(path,mode,basepe,chunksizehintp,parameters,table,ncid));
}

static int NCZDB_redef(int ncid)
{
    return THROW(NCZ_redef(ncid));
}

static int NCZDB__enddef(int ncid,size_t h_minfree,size_t v_align,size_t v_minfree,size_t r_align)
{
    return THROW(NCZ__enddef(ncid,h_minfree,v_align,v_minfree,r_align));
}

static int NCZDB_sync(int ncid)
{
    return THROW(NCZ_sync(ncid));
}

static int NCZDB_abort(int ncid)
{
    return THROW(NCZ_abort(ncid));
}

static int NCZDB_close(int ncid, void* params)
{
    return THROW(NCZ_close(ncid,params));
}

static int NCZDB_set_fill(int ncid, int fillmode, int *old)
{
    return THROW(NCZ_set_fill(ncid,fillmode,old));
}

static int NCZDB_inq_format(int ncid, int* formatp)
{
    return THROW(NC4_inq_format(ncid,formatp));
}

static int NCZDB_inq_format_extended(int ncid, int *formatp, int *modep)
{
    return THROW(NCZ_inq_format_extended(ncid,formatp,modep));
}

static int NCZDB_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *udimp)
{
    return THROW(NCZ_inq(ncid,ndimsp,nvarsp,nattsp,udimp));
}

static int NCZDB_inq_type(int ncid, nc_type xtype, char *name, size_t *size)
{
    return THROW(NC4_inq_type(ncid,xtype,name,size));
}

static int NCZDB_def_dim(int ncid, const char *name, size_t len, int *idp)
{
    return THROW(NCZ_def_dim(ncid,name,len,idp));
}

static int NCZDB_inq_dimid(int ncid, const char *name, int *idp)
{
    return THROW(NC4_inq_dimid(ncid,name,idp));
}

static int NCZDB_inq_dim(int ncid, int dimid, char *name, size_t *lenp)
{
    return THROW(NCZ_inq_dim(ncid,dimid,name,lenp));
}

static int NCZDB_inq_unlimdim(int ncid,  int *unlimdimidp)
{
    return THROW(NC4_inq_unlimdim(ncid,unlimdimidp));
}

static int NCZDB_rename_dim(int ncid, int dimid, const char *name)
{
    return THROW(NCZ_rename_dim(ncid,dimid,name));
}

static int NCZDB_inq_att(int ncid, int varid, const char *name, nc_type *xtypep, size_t *lenp)
{
    return THROW(NCZ_inq_att(ncid,varid,name,xtypep,lenp));
}

static int NCZDB_inq_attid(int ncid, int varid, const char* name, int *idp)
{
    return THROW(NCZ_inq_attid(ncid,varid,name,idp));
}

static int NCZDB_inq_attname(int ncid, int varid, int attnum, char *name)
{
    return THROW(NCZ_inq_attname(ncid,varid,attnum,name));
}

static int NCZDB_rename_att(int ncid, int varid, const char* name, const char *newname)
{
    return THROW(NCZ_rename_att(ncid,varid,name,newname));
}

static int NCZDB_del_att(int ncid, int varid, const char *name)
{
    return THROW(NCZ_del_att(ncid,varid,name));
}

static int NCZDB_get_att(int ncid, int varid, const char* name, void *data, nc_type memtype)
{
    return THROW(NCZ_get_att(ncid,varid,name,data,memtype));
}

static int NCZDB_put_att(int ncid, int varid, const char* name, nc_type filetype, size_t len, const void *data, nc_type memtype)
{
    return THROW(NCZ_put_att(ncid,varid,name,filetype,len,data,memtype));
}

static int NCZDB_def_var(int ncid, const char* name, nc_type xtype, int ndims, const int *dimidsp, int *varidp)
{
    return THROW(NCZ_def_var(ncid,name,xtype,ndims,dimidsp,varidp));
}

static int NCZDB_inq_varid(int ncid, const char* name, int *varidp)
{
    return THROW(NC4_inq_varid(ncid,name,varidp));
}

static int NCZDB_rename_var(int ncid, int varid, const char *name)
{
    return THROW(NCZ_rename_var(ncid,varid,name));
}

static int NCZDB_get_vara(int ncid, int varid, const size_t *startp, const size_t *countp, void *ip, nc_type memtype)
{
    return THROW(NCZ_get_vara(ncid,varid,startp,countp,ip,memtype));
}

static int NCZDB_put_vara(int ncid, int varid, const size_t *startp, const size_t *countp, const void *ip, nc_type memtype)
{
    return THROW(NCZ_put_vara(ncid,varid,startp,countp,ip,memtype));
}

static int NCZDB_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp, int *shufflep, int *deflatep, int *deflate_levelp, int *fletcher32p, int *contiguousp, size_t *chunksizesp, int *no_fill, void *fill_valuep, int *endiannessp, unsigned int *idp, size_t *nparamsp, unsigned int *params)
{
    return THROW(NCZ_inq_var_all(ncid,varid,name,xtypep,ndimsp,dimidsp,nattsp,shufflep,deflatep,deflate_levelp,fletcher32p,contiguousp,chunksizesp,no_fill,fill_valuep,endiannessp,idp,nparamsp,params));
}

static int NCZDB_var_par_access(int ncid, int varid, int par_access)
{
    return THROW(NCZ_var_par_access(ncid,varid,par_access));
}

static int NCZDB_def_var_fill(int ncid, int varid, int no_fill, const void *fill_value)
{
    return THROW(NCZ_def_var_fill(ncid,varid,no_fill,fill_value));
}

static int NCZDB_show_metadata(int ncid)
{
    return THROW(NCZ_show_metadata(ncid));
}

static int NCZDB_inq_unlimdims(int ncid, int* n, int* uidsp)
{
    return THROW(NCZ_inq_unlimdims(ncid,n,uidsp));
}

static int NCZDB_inq_ncid(int ncid, const char* name, int* grpidp)
{
    return THROW(NC4_inq_ncid(ncid,name,grpidp));
}

static int NCZDB_inq_grps(int ncid, int* n, int* ncids)
{
    return THROW(NC4_inq_grps(ncid,n,ncids));
}

static int NCZDB_inq_grpname(int ncid, char* name)
{
    return THROW(NC4_inq_grpname(ncid,name));
}

static int NCZDB_inq_grpname_full(int ncid, size_t* lenp, char* fullname)
{
    return THROW(NC4_inq_grpname_full(ncid,lenp,fullname));
}

static int NCZDB_inq_grp_parent(int ncid, int* parentidp)
{
    return THROW(NC4_inq_grp_parent(ncid,parentidp));
}

static int NCZDB_inq_grp_full_ncid(int ncid, const char* fullname, int* grpidp)
{
    return THROW(NC4_inq_grp_full_ncid(ncid,fullname,grpidp));
}

static int NCZDB_inq_varids(int ncid, int* nvars, int* varids)
{
    return THROW(NC4_inq_varids(ncid,nvars,varids));
}

static int NCZDB_inq_dimids(int ncid, int* ndims, int* dimids, int inclparents)
{
    return THROW(NC4_inq_dimids(ncid,ndims,dimids,inclparents));
}

static int NCZDB_inq_typeids(int ncid, int* ntypes, int* typeids)
{
    return THROW(NCZ_inq_typeids(ncid,ntypes,typeids));
}

static int NCZDB_inq_type_equal(int ncid1, nc_type tid1, int ncid2, nc_type tid2, int* eq)
{
    return THROW(NCZ_inq_type_equal(ncid1,tid1,ncid2,tid2,eq));
}

static int NCZDB_def_grp(int parent, const char* name, int* grpid)
{
    return THROW(NCZ_def_grp(parent,name,grpid));
}

static int NCZDB_rename_grp(int ncid, const char* name)
{
    return THROW(NCZ_rename_grp(ncid,name));
}

static int NCZDB_inq_user_type(int ncid, nc_type xtype, char* name, size_t* size, nc_type* basetid, size_t* nfields, int* classp)
{
    return THROW(NC4_inq_user_type(ncid,xtype,name,size,basetid,nfields,classp));
}

static int NCZDB_inq_typeid(int ncid, const char* name, nc_type* tidp)
{
    return THROW(NCZ_inq_typeid(ncid,name,tidp));
}

static int NCZDB_def_var_chunking(int ncid, int varid, int storage, const size_t *chunksizes)
{
    return THROW(NCZ_def_var_chunking(ncid,varid,storage,chunksizes));
}

static int NCZDB_def_var_endian(int ncid, int varid, int endian)
{
    return THROW(NCZ_def_var_endian(ncid,varid,endian));
}

static int NCZDB_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption)
{
    return THROW(NCZ_set_var_chunk_cache(ncid,varid,size,nelems,preemption));
}

static int NCZDB_get_var_chunk_cache(int ncid, int varid, size_t *sizep, size_t *nelemsp, float *preemptionp)
{
    return THROW(NC4_get_var_chunk_cache(ncid,varid,sizep,nelemsp,preemptionp));
}

static int NCZDB_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids)
{
    return THROW(NCZ_inq_var_filter_ids(ncid,varid,nfilters,filterids));
}

static int NCZDB_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params)
{
    return THROW(NCZ_inq_var_filter_info(ncid,varid,id,nparams,params));
}


#if 0
static int NCZDB_get_vars(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, void *ip, nc_type memtype)
{
    return THROW(NCZ_get_vars(ncid,varid,startp,countp,stridep,ip,memtype));
}

static int NCZDB_put_vars(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const void *ip, nc_type memtype)
{
    return THROW(NCZ_put_vars(ncid,varid,startp,countp,stridep,ip,memtype));
}

static int NCZDB_def_var_deflate(int ncid, int varid, int shuffle, int deflate, int level)
{
    return THROW(NCZ_def_var_deflate(ncid,varid,shuffle,deflate,level));
}

static int NCZDB_def_var_fletcher32(int ncid, int varid, int fletcher32)
{
    return THROW(NCZ_def_var_fletcher32(ncid,varid,fletcher32));
}

static int NCZDB_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams, const unsigned int *params)
{
    return THROW(NCZ_def_var_filter(ncid,varid,id,nparams,params));
}

#endif

static const NC_Dispatch NCZ_dispatcher_debug = {

    NC_FORMATX_NCZARR,
    NC_DISPATCH_VERSION,

    NCZDB_create,
    NCZDB_open,

    NCZDB_redef,
    NCZDB__enddef,
    NCZDB_sync,
    NCZDB_abort,
    NCZDB_close,
    NCZDB_set_fill,
    NCZDB_inq_format,
    NCZDB_inq_format_extended,

    NCZDB_inq,
    NCZDB_inq_type,

    NCZDB_def_dim,
    NCZDB_inq_dimid,
    NCZDB_inq_dim,
    NCZDB_inq_unlimdim,
    NCZDB_rename_dim,

    NCZDB_inq_att,
    NCZDB_inq_attid,
    NCZDB_inq_attname,
    NCZDB_rename_att,
    NCZDB_del_att,
    NCZDB_get_att,
    NCZDB_put_att,

    NCZDB_def_var,
    NCZDB_inq_varid,
    NCZDB_rename_var,
    NCZDB_get_vara,
    NCZDB_put_vara,
    NCDEFAULT_get_vars,
    NCDEFAULT_put_vars,
    NCDEFAULT_get_varm,
    NCDEFAULT_put_varm,

    NCZDB_inq_var_all,

    NCZDB_var_par_access,
    NCZDB_def_var_fill,

    NCZDB_show_metadata,
    NCZDB_inq_unlimdims,

    NCZDB_inq_ncid,
    NCZDB_inq_grps,
    NCZDB_inq_grpname,
    NCZDB_inq_grpname_full,
    NCZDB_inq_grp_parent,
    NCZDB_inq_grp_full_ncid,
    NCZDB_inq_varids,
    NCZDB_inq_dimids,
    NCZDB_inq_typeids,
    NCZDB_inq_type_equal,
    NCZDB_def_grp,
    NCZDB_rename_grp,
    NCZDB_inq_user_type,
    NCZDB_inq_typeid,

    NC_NOTNC4_def_compound,
    NC_NOTNC4_insert_compound,
    NC_NOTNC4_insert_array_compound,
    NC_NOTNC4_inq_compound_field,
    NC_NOTNC4_inq_compound_fieldindex,
    NC_NOTNC4_def_vlen,
    NC_NOTNC4_put_vlen_element,
    NC_NOTNC4_get_vlen_element,
    NC_NOTNC4_def_enum,
    NC_NOTNC4_insert_enum,
    NC_NOTNC4_inq_enum_member,
    NC_NOTNC4_inq_enum_ident,
    NC_NOTNC4_def_opaque,
    NC_NOTNC4_def_var_deflate,
    NC_NOTNC4_def_var_fletcher32,
    NCZDB_def_var_chunking,
    NCZDB_def_var_endian,
    NC_NOTNC4_def_var_filter,
    NCZDB_set_var_chunk_cache,
    NCZDB_get_var_chunk_cache,
    NCZDB_inq_var_filter_ids,
    NCZDB_inq_var_filter_info,
};


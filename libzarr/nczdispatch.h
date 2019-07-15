/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef _NCZDISPATCH_H
#define _NCZDISPATCH_H

extern int NCZ_initialize(void);
extern int NCZ_finalize(void);


extern int NCZ_create(const char *path, int cmode,
              size_t initialsz, int basepe, size_t *chunksizehintp,
              void* parameters, const NC_Dispatch* table, NC* ncp);

extern int NCZ_open(const char *path, int mode,
            int basepe, size_t *chunksizehintp,
            void* parameters, const NC_Dispatch* table, NC* ncp);

extern int NCZ_redef(int);
extern int NCZ__enddef(int,size_t,size_t,size_t,size_t);
extern int NCZ_sync(int);
extern int NCZ_abort(int);
extern int NCZ_close(int,void*);
extern int NCZ_set_fill(int,int,int*);
extern int NCZ_inq_base_pe(int,int*);
extern int NCZ_set_base_pe(int,int);
extern int NCZ_inq_format(int,int*);
extern int NCZ_inq_format_extended(int,int*,int*);

extern int NCZ_inq(int,int*,int*,int*,int*);
extern int NCZ_inq_type(int, nc_type, char*, size_t*);

extern int NCZ_def_dim(int, const char*, size_t, int*);
extern int NCZ_inq_dimid(int, const char*, int*);
extern int NCZ_inq_dim(int, int, char*, size_t*);
extern int NCZ_inq_unlimdim(int ncid,  int *unlimdimidp);
extern int NCZ_rename_dim(int, int, const char*);

extern int NCZ_inq_att(int, int, const char*, nc_type*, size_t*);
extern int NCZ_inq_attid(int, int, const char*, int*);
extern int NCZ_inq_attname(int, int, int, char*);
extern int NCZ_rename_att(int, int, const char*, const char*);
extern int NCZ_del_att(int, int, const char*);
extern int NCZ_get_att(int, int, const char*, void*, nc_type);
extern int NCZ_put_att(int, int, const char*, nc_type, size_t, const void*, nc_type);

extern int NCZ_def_var(int, const char*, nc_type, int, const int*, int*);
extern int NCZ_inq_varid(int, const char*, int*);
extern int NCZ_rename_var(int, int, const char*);

extern int NCZ_get_vara(int, int, const size_t*, const size_t*, void*, nc_type);
extern int NCZ_put_vara(int, int, const size_t*, const size_t*, const void*, nc_type);

/* Added to solve Ferret performance problem with Opendap */
extern int NCZ_get_vars(int, int, const size_t*, const size_t*, const ptrdiff_t*, void*, nc_type);
extern int NCZ_put_vars(int, int, const size_t*, const size_t*, const ptrdiff_t*, const void*, nc_type);

extern int NCZ_get_varm(int, int, const size_t*, const size_t*, const ptrdiff_t*, const ptrdiff_t*, void*, nc_type);
extern int NCZ_put_varm(int, int, const size_t*, const size_t*, const ptrdiff_t*, const ptrdiff_t*, const void*, nc_type);

extern int NCZ_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
               int *ndimsp, int *dimidsp, int *nattsp,
               int *shufflep, int *deflatep, int *deflate_levelp,
               int *fletcher32p, int *contiguousp, size_t *chunksizesp,
               int *no_fill, void *fill_valuep, int *endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
              );

extern int NCZ_var_par_access(int, int, int);
extern int NCZ_def_var_fill(int, int, int, const void*);

extern int NCZ_show_metadata(int);
extern int NCZ_inq_unlimdims(int, int*, int*);
extern int NCZ_inq_ncid(int, const char*, int*);
extern int NCZ_inq_grps(int, int*, int*);
extern int NCZ_inq_grpname(int, char*);
extern int NCZ_inq_grpname_full(int, size_t*, char*);
extern int NCZ_inq_grp_parent(int, int*);
extern int NCZ_inq_grp_full_ncid(int, const char*, int*);
extern int NCZ_inq_varids(int, int* nvars, int*);
extern int NCZ_inq_dimids(int, int* ndims, int*, int);
extern int NCZ_inq_typeids(int, int* ntypes, int*);
extern int NCZ_inq_type_equal(int, nc_type, int, nc_type, int*);
extern int NCZ_def_grp(int, const char*, int*);
extern int NCZ_rename_grp(int, const char*);
extern int NCZ_inq_user_type(int, nc_type, char*, size_t*, nc_type*, size_t*, int*);
extern int NCZ_inq_typeid(int, const char*, nc_type*);

extern int NCZ_def_compound(int, size_t, const char*, nc_type*);
extern int NCZ_insert_compound(int, nc_type, const char*, size_t, nc_type);
extern int NCZ_insert_array_compound(int, nc_type, const char*, size_t, nc_type, int, const int*);
extern int NCZ_inq_compound_field(int, nc_type, int, char*, size_t*, nc_type*, int*, int*);
extern int NCZ_inq_compound_fieldindex(int, nc_type, const char*, int*);
extern int NCZ_def_vlen(int, const char*, nc_type base_typeid, nc_type*);
extern int NCZ_put_vlen_element(int, int, void*, size_t, const void*);
extern int NCZ_get_vlen_element(int, int, const void*, size_t*, void*);
extern int NCZ_def_enum(int, nc_type, const char*, nc_type*);
extern int NCZ_insert_enum(int, nc_type, const char*, const void*);
extern int NCZ_inq_enum_member(int, nc_type, int, char*, void*);
extern int NCZ_inq_enum_ident(int, nc_type, long long, char*);
extern int NCZ_def_opaque(int, size_t, const char*, nc_type*);
extern int NCZ_def_var_deflate(int, int, int, int, int);
extern int NCZ_def_var_fletcher32(int, int, int);
extern int NCZ_def_var_chunking(int, int, int, const size_t*);
extern int NCZ_def_var_endian(int, int, int);
extern int NCZ_def_var_filter(int, int, unsigned int, size_t, const unsigned int*);
extern int NCZ_set_var_chunk_cache(int, int, size_t, size_t, float);
extern int NCZ_get_var_chunk_cache(int ncid, int varid, size_t *sizep, size_t *nelemsp, float *preemptionp);

#endif /*_NCZDISPATCH_H*/

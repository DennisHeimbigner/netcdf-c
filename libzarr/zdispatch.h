/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Includes prototypes for libzarr dispatch functions.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef ZDISPATCH_H
#define ZDISPATCH_H

#if defined(__cplusplus)
extern "C" {
#endif

    EXTERNL int
    NCZ_inq_att(int ncid, int varid, const char *name,
                     nc_type *xtypep, size_t *lenp);

    EXTERNL int
    NCZ_inq_attid(int ncid, int varid, const char *name, int *idp);

    EXTERNL int
    NCZ_inq_attname(int ncid, int varid, int attnum, char *name);

    EXTERNL int
    NCZ_rename_att(int ncid, int varid, const char *name, const char *newname);

    EXTERNL int
    NCZ_del_att(int ncid, int varid, const char*);

    EXTERNL int
    NCZ_put_att(int ncid, int varid, const char *name, nc_type datatype,
                     size_t len, const void *value, nc_type);

    EXTERNL int
    NCZ_get_att(int ncid, int varid, const char *name, void *value, nc_type);

    EXTERNL int
    NCZ_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
                         int *ndimsp, int *dimidsp, int *nattsp,
                         int *shufflep, int *deflatep, int *deflate_levelp,
                         int *fletcher32p, int *contiguousp, size_t *chunksizesp,
                         int *no_fill, void *fill_valuep, int *endiannessp,
                         unsigned int *idp, size_t *nparamsp, unsigned int *params);

    EXTERNL int
    NCZ_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems,
                                 float preemption);

#if defined(__cplusplus)
}
#endif

#endif /*ZDISPATCH_H */

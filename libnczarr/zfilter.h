/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file This header file containsfilter related  macros, types, and prototypes for
 * the filter code in libnczarr. This header should not be included in
 * code outside libnczarr.
 *
 * @author Dennis Heimbigner
 */

#ifndef ZFILTER_H
#define ZFILTER_H

/* zfilter.c */
/* Dispatch functions are also in zfilter.c */
/* Filterlist management */

/* The NC_VAR_INFO_T->filters field is an NClist of this struct */
typedef struct NCZ_Filter {
    int flags;             /**< Flags describing state of this filter. */
    unsigned int filterid; /**< ID for arbitrary filter. */
    size_t nparams;        /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
    const void* code;      /**<Pointer to the loaded filter info. */
} NCZ_Filter;

/*Mnemonic*/
#define ENCODING 1

/* list of environment variables to check for plugin roots */
#define plugin_env "HDF5_PLUGIN_PATH"
#define plugin_dir_unix "/usr/local/hdf5/plugin"
#define plugin_dir_win "%s/hdf5/lib/plugin"
#define win32_root_env "ALLUSERSPROFILE"

int NCZ_filter_initialize(void);
int NCZ_filter_finalize(void);
int NCZ_filter_lookup(NC_VAR_INFO_T* var, unsigned int id, struct NCZ_Filter** specp);
int NCZ_addfilter(NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params);
int NCZ_filter_freelist(NC_VAR_INFO_T* var);
int NCZ_filter_free(NCZ_Filter* f);
int NCZ_applyfilterchain(NClist* chain, size_t insize, void* indata, size_t* outlen, void** outdata, int encode);
int NCZ_filter_jsonize(NC_VAR_INFO_T*, NCZ_Filter* filter, NCjson** jfilterp);
int NCZ_filter_build(NC_VAR_INFO_T* var, NCjson* jfilter, NCZ_Filter** filterp);

#endif /*ZFILTER_H*/
/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal This header file contains macros, types and prototypes
 * used to build and manipulate filters. It contains definitions
 * for multiple file formats.
 *
 * @author Dennis Heimbigner
 */

#ifndef NCFILTER_H
#define NCFILTER_H


/**************************************************/
/* Internal filter related structures */

/* Internal filter actions */
#define NCFILTER_DEF		1
#define NCFILTER_REMOVE  	2
#define NCFILTER_INQ	    	3
#define NCFILTER_FILTERIDS      4
#define NCFILTER_INFO		5
#define NCFILTER_FREESPEC	6
#define NCFILTER_CLIENT_REG	10
#define NCFILTER_CLIENT_UNREG	11
#define NCFILTER_CLIENT_INQ	12

typedef enum NC_FILTER_SORT {
	NC_FILTER_SORT_SPEC=((int)1),
	NC_FILTER_SORT_IDS=((int)2),
	NC_FILTER_SORT_CLIENT=((int)3),
} NC_FILTER_SORT;

/* Provide structs to pass args to filter_actions function for HDF5*/

typedef struct NC_FILTER_SPEC_HDF5 {
    NC_Filterspec format;
    int active;            /**< true iff HDF5 library was told to activate filter */
    unsigned int filterid; /**< ID for arbitrary filter. */
    size_t nparams;        /**< nparams for arbitrary filter. */
    unsigned int* params;  /**< Params for arbitrary filter. */
} NC_FILTER_SPEC_HDF5;

typedef struct NC_FILTERIDS_HDF5 {
    size_t nfilters;          /**< number of filters */
    unsigned int* filterids;  /**< Filter ids. */
} NC_FILTERIDS_HDF5;

typedef struct NC_FILTER_CLIENT_HDF5 {
    unsigned int id;
    /* The filter info for hdf5 */
    /* Avoid needing hdf.h by using void* */
    void* info;
} NC_FILTER_CLIENT_HDF5;

typedef struct NC_FILTER_OBJ_HDF5 {
    NC_Filterobject hdr; /* So we can cast it */
    NC_FILTER_SORT sort; /* discriminate union */
    union {
        NC_FILTER_SPEC_HDF5 spec;
        NC_FILTERIDS_HDF5 ids;
        NC_FILTER_CLIENT_HDF5 client;
    } u;
} NC_FILTER_OBJ_HDF5;

extern void NC4_freefilterspec(NC_FILTER_SPEC_HDF5*);

/**************************************************/
/* Provide structs to pass args to filter_actions function using strings */

typedef struct NCX_FILTER_SPEC {
    NC_Filterspec format;
    int active;            /**< true iff underlying library was told to activate filter */
    char* filterid;	   /**< ID for arbitrary filter. */
    char* params;  	   /**< Params for arbitrary filter. */
} NCX_FILTER_SPEC;

typedef struct NCX_FILTERIDS {
    size_t nfilters;   /**< number of filters */
    char** filterids;  /**< Filter ids. */
} NCX_FILTERIDS;

typedef struct NCX_FILTER_CLIENT {
    char* id;
    /* The filter info for x */
    /* Avoid needing hdf.h by using void* */
    void* info;
} NCX_FILTER_CLIENT;

typedef struct NCX_FILTER_OBJ {
    NC_Filterobject hdr; /* So we can cast it */
    NC_FILTER_SORT sort; /* discriminate union */
    union {
        NCX_FILTER_SPEC spec;
        NCX_FILTERIDS ids;
        NCX_FILTER_CLIENT client;
    } u;
} NCX_FILTER_OBJ;

extern void NCX_freefilterspec(NCX_FILTER_SPEC*);

#endif /*NCFILTER_H*/

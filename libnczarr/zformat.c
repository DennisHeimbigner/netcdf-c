/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zformat.h"
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
#include "zfilter.h"
#endif

/**************************************************/

extern int NCZF2_initialize(void);
extern int NCZF2_finalize(void);
extern int NCZF3_initialize(void);
extern int NCZF3_finalize(void);


/**************************************************/

int
NCZF_initialize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF2_initialize())) goto done;
    if((stat=NCZF3_initialize())) goto done;
done:
    return THROW(stat);
}

int
NCZF_finalize(void)
{
    int stat = NC_NOERR;
    if((stat=NCZF2_finalize())) goto done;
    if((stat=NCZF3_finalize())) goto done;
done:
    return THROW(stat);
}

int
NCZF_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->create(file,uri,map);
    return THROW(stat);
}

int
NCZF_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->open(file,uri,map);
    return THROW(stat);
}

int
NCZF_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->close(file);
    return THROW(stat);
}

int
NCZF_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->hdf2codec(file,var,filter);
    return THROW(stat);
}

/**************************************************/
/*
From Zarr V2 Specification:
"The compressed sequence of bytes for each chunk is stored under
a key formed from the index of the chunk within the grid of
chunks representing the array.  To form a string key for a
chunk, the indices are converted to strings and concatenated
with the dimension_separator character ('.' or '/') separating
each index. For example, given an array with shape (10000,
10000) and chunk shape (1000, 1000) there will be 100 chunks
laid out in a 10 by 10 grid. The chunk with indices (0, 0)
provides data for rows 0-1000 and columns 0-1000 and is stored
under the key "0.0"; the chunk with indices (2, 4) provides data
for rows 2000-3000 and columns 4000-5000 and is stored under the
key "2.4"; etc."
*/

/**
 * @param R Rank
 * @param chunkindices The chunk indices
 * @param dimsep the dimension separator
 * @param keyp Return the chunk key string
 */
int
NCZF_buildchunkkey(const NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->buildchunkkey(rank,chunkindices,dimsep,keyp);
    return THROW(stat);
}

/**************************************************/
/* Compile incoming metadata */

int
NCZF_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->readmeta(file);
    return THROW(stat);
}

/* De-compile incoming metadata */

int
NCZF_writemeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->writemeta(file);
    return THROW(stat);
}

/* Get one of two key values from a dict */
int
NCZ_dictgetalt(const NCjson* jdict, const char* name, const char* alt, const NCjson** jvaluep)
{
    int stat = NC_NOERR;
    const NCjson* jvalue = NULL;
    NCJcheck(NCJdictget(jdict,name,&jvalue)); /* try this first */
    if(jvalue == NULL) {
        NCJcheck(NCJdictget(jdict,alt,&jvalue)); /* try this alternative*/
    }
    if(jvaluep) *jvaluep = jvalue;
done:
    return THROW(stat);
}

/* Get _nczarr_xxx from either .zXXX or .zattrs */
int
NCZ_getnczarrkey(NC_OBJ* container, const char* name, const NCjson** jncxxxp)
{
    int stat = NC_NOERR;
    const NCjson* jxxx = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct ZARROBJ* zobj = NULL;

    /* Decode container */
    if(container->sort == NCGRP) {
	grp = (NC_GRP_INFO_T*)container;
	zobj = &((NCZ_GRP_INFO_T*)grp->format_grp_info)->zgroup;
    } else {
	var = (NC_VAR_INFO_T*)container;
	zobj = &((NCZ_VAR_INFO_T*)var->format_var_info)->zarray;
    }

    /* Try .zattrs first */
    if(zobj->atts != NULL) {
	jxxx = NULL;
        NCJcheck(NCJdictget(zobj->atts,name,&jxxx));
    }
    if(name == NULL) {
        jxxx = NULL;
        /* Try .zxxx second */
	if(zobj->obj != NULL) {
            NCJcheck(NCJdictget(zobj->obj,name,&jxxx));
	}
	/* Mark as old style with _nczarr_xxx in obj not attributes */
	zobj->nczv1 = 1;
    }
    if(jncxxxp) *jncxxxp = jxxx;
done:
    return THROW(stat);
}

int
NCZ_downloadzarrobj(NC_FILE_INFO_T* file, struct ZARROBJ* zobj, const char* fullpath, const char* objname)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZMAP* map = ((NCZ_FILE_INFO_T*)file->format_file_info)->map;

    /* Download .zXXX and .zattrs */
    nullfree(zobj->prefix);
    zobj->prefix = strdup(fullpath);
    NCJreclaim(zobj->obj); zobj->obj = NULL;
    NCJreclaim(zobj->atts); zobj->obj = NULL;
    if((stat = nczm_concat(fullpath,objname,&key))) goto done;
    if((stat=NCZ_downloadjson(map,key,&zobj->obj))) goto done;
    nullfree(key); key = NULL;
    if((stat = nczm_concat(fullpath,ZATTRS,&key))) goto done;
    if((stat=NCZ_downloadjson(map,key,&zobj->atts))) goto done;
done:
    nullfree(key);
    return THROW(stat);
}

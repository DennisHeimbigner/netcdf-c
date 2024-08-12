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

struct ZJSON emptyjsonz = {NULL,NULL,0};

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

int
NCZF_dtype2nctype(const NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->dtype2nctype(file,dtype,typehint,nctypep,endianp,typelenp);
    return THROW(stat);
}

int
NCZF_nctype2dtype(const NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typelen, char** dtypep, char** daliasp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->nctype2dtype(file,nctype,endianness,typelen,dtypep,daliasp);
    return THROW(stat);
}

int
NCZF_searchvars(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->searchvars(file,grp,varnames);
    return THROW(stat);
}

int
NCZF_searchsubgrps(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrps)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->searchsubgrps(file,grp,subgrps);
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
NCZF_encode_chunkkey(const NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->build_chunkkey(rank,chunkindices,dimsep,keyp);
    return THROW(stat);
}

/**************************************************/
/* Encode netcdf-4 metadata into json*/

int
NCZF_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_group(file, grp, jgroupp);
    return THROW(stat);
}

int
NCZF_encode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_superblock(file, root, jsuperp);
    return THROW(stat);
}

int
NCZF_encode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* dims, NCjson** jdimsp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_grp_dims(file, grp,  dims, jdimsp);
    return THROW(stat);
}

int
NCZF_encode_grp_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* vars, NCjson** jvarsp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_grp_vars(file, grp, vars, jvarsp);
    return THROW(stat);
}

int
NCZF_encode_grp_subgroups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCindex* subgrps, NCjson** jsubgrpsp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_grp_subgroups(file, grp, subgrps, jsubgrpsp);
    return THROW(stat);
}

int
NCZF_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_nczarr_group(file, grp, jdims, jvars, jsubgrps, jnczgrpp);
    return THROW(stat);
}

int
NCZF_encode_group_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jsuper, NCjson* jatts, NCjson* jtypes, NCjson** jgrpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_group_json(file, grp, jsuper, jatts, jtypes, jgrpp);
    return THROW(stat);
}

int
NCZF_encode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCindex* attlist, NCjson** jattsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_attributes_json(file, container, attlist, jattsp, jtypesp);
    return THROW(stat);
}

int
NCZF_decode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZJSON* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_var_json(file, var, jsonz);
    return THROW(stat);
}

int
NCZF_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->encode_nczarr_array(file, var, jnczarrayp);
    return THROW(stat);
}

/* Write JSON to storage */

int
NCZF_upload_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jgroup, const NCjson* jatts)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->upload_grp_json(file, grp,  jgroup,  jatts);
    return THROW(stat);
}

int
NCZF_upload_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jvar, const NCjson* jatts)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->upload_var_json(file, var,  jvar,  jatts);
    return THROW(stat);
}

/**************************************************/
/* Compile incoming metadata */

int
NCZF_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jgroup, NCjson* jatts, NCjson** jsuperp, NClist* dims, NClist* vars, NClist* subgrps)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_group(file, grp, jgroup, jatts, jsuperp, dims, vars, subgrps);
    return THROW(stat);
}

int
NCZF_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const struct ZJson* jsonz, NCjson** jdimsp, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_nczarr_group(file, grp, jsonz, jdimsp, jvars, jsubgrps,  jnczgrpp);
    return THROW(stat);
}

int
NCZF_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson* jsuper)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_superblock(file, root, jsuper);
    return THROW(stat);
}

int
NCZF_decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_grp_dims(file, grp, jdims);
    return THROW(stat);
}

int
NCZF_decode_grp_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname, NC_VAR_INFO_T** jvarp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_grp_var(file, grp, varname, jvarp);
    return THROW(stat);
}

int
NCZF_decode_grp_subgroup(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* subgrpname, NC_GRP_INFO_T** subgrpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_grp_subgroup(file, parent, subgrpname, subgrpp);
    return THROW(stat);
}

int
NCZF_decode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson** jattsp, const NCjson** jtypesp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_attributes_json(file, container, jattsp, jtypesp);
    return THROW(stat);
}

int
NCZF_decode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvarp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_var_json(file, var, jvarp);
    return THROW(stat);
}

int
NCZF_decode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->decode_nczarr_array(file, var, jnczarrayp);
    return THROW(stat);
}

int
NCZF_create_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* gname, NC_GRP_INFO_T** grpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->create_grp(file, parent, grpname, grpp);
    return THROW(stat);
}

int
NCZF_create_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* varname, NC_VAR_INFO_T** varp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->create_var(file, var, varp);
    return THROW(stat);
}

int
NCZF_download_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZJSON* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->download_grp_json(file, grp, jsonz);
    return THROW(stat);
}

int
NCZF_download_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZJSON* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    stat = zfile->dispatcher->download_var_json(file, var, jsonz);
    return THROW(stat);
}

/**************************************************/
/* Misc.
void
NCZ_clear_zjson(struct ZJSON* zjson)
{
    if(zjson != NULL) {
        NCJreclaim(zjson->jobj);
	if(!zjson->constatt) NCJreclaim(zjson->jatts);
    }
}

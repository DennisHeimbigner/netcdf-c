/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"
#include "zplugins.h"
#include "znc4.h"
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
#include "netcdf_filter_build.h"
#endif

/**************************************************/

/*Mnemonics*/
#define ISATTR 1

/**************************************************/
/* Static zarr type name table */
/* Used to convert nc_type <=> dtype */
static const struct ZTYPESV2 {
    const char* dtype;
    const char* dtypeattr;
} znamesv2[N_NCZARR_TYPES] = {
/* nc_type	 dtype */
/*NC_NAT*/	{NULL,0},
/*NC_BYTE*/	{"|i1",NULL},
/*NC_CHAR*/	{">S1",NULL},
/*NC_SHORT*/	{"|i2",NULL},
/*NC_INT*/	{"|i4",NULL},
/*NC_FLOAT*/	{"|f4",NULL},
/*NC_DOUBLE*/	{"|f8",NULL},
/*NC_UBYTE*/	{"|u1",NULL},
/*NC_USHORT*/	{"|u2",NULL},
/*NC_UINT*/	{"|u4",NULL},
/*NC_INT64*/	{"|i8",NULL},
/*NC_UINT64*/	{"|u8",NULL},
/*NC_STRING*/	{"|S%d",NULL},
/*NC_JSON*/	{">S1","|J0"} /* NCZarr internal type */
};

struct Ainfo {
    nc_type type;
    int endianness;
    size_t typelen;
};

/* Capture arguments for ncz4_create_var */
struct CVARGS {
	const char* varname;
	nc_type vtype;
	int storage;
	int scalar;
	int endianness;
	size_t maxstrlen;
	char dimension_separator;
	char order;
	size_t rank;
	size64_t shapes[NC_MAX_VAR_DIMS];
	size64_t chunks[NC_MAX_VAR_DIMS];
	int dimids[NC_MAX_VAR_DIMS];
	NClist* filterlist;
	int no_fill;
	void* fill_value;
};

/**************************************************/
/* Forward */

static int ZF2_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_close(NC_FILE_INFO_T* file);
static int ZF2_download_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj);
static int ZF2_download_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj);
static int ZF2_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* jgroup, NCjson** jzgrpp, NCjson** jzsuperp);
static int ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, const NCjson* jsuper, int* zarrformat, int* nczarrformat);
static int ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jnczgrp, NClist* vars, NClist* subgrps);
static int ZF2_decode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj, NClist* jfilters, size64_t** shapep, size64_t** chunksp, NClist* dimrefs);
static int ZF2_decode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jncvar, const NCjson* jatts);
static int ZF2_upload_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj);
static int ZF2_upload_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj);
static int ZF2_encode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
static int ZF2_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jzgroupp);
static int ZF2_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jattsp, NCjson** jgroupp);
static int ZF2_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jzvarp);
static int ZF2_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jattsp, NClist* filtersj, NCjson** jvarp);
static int ZF2_encode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jnczconp, NCjson** jattsp);
static int ZF2_encode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter, NCjson** jfilterp);
static int ZF2_decode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jfilter, NCZ_Filter* filter);
static int ZF2_hdf2codec(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF2_codec2hdf(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF2_searchobjects(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrpnames);
static int ZF2_encode_chunkkey(NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);
static int ZF2_decode_chunkkey(NC_FILE_INFO_T* file, char* dimsep, char* chunkname, size_t* rankp, size64_t** chunkindicesp);

static int decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims, NClist* dimdefs);
static int dtype2nctype(NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp);
static int nctype2dtype(NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typesize, char** dtypep, char** dattrtypep);

/**************************************************/
/* Format dispatch table */

static const NCZ_Formatter NCZ_formatter2_table =
{
    NCZARRFORMAT2,
    ZARRFORMAT2,
    NCZ_FORMATTER_VERSION,

    /*File-Level Operations*/
    ZF2_create,
    ZF2_open,
    ZF2_close,

    /*Read JSON Metadata*/
    ZF2_download_grp,
    ZF2_download_var,

    ZF2_decode_group,
    ZF2_decode_superblock,
    ZF2_decode_nczarr_group,
    ZF2_decode_var,
    ZF2_decode_attributes,

    /*Write JSON Metadata*/
    ZF2_upload_grp,
    ZF2_upload_var,

    ZF2_encode_superblock,
    ZF2_encode_nczarr_group,
    ZF2_encode_group,

    ZF2_encode_nczarr_array,
    ZF2_encode_var,

    ZF2_encode_attributes,

    /*Filter Processing*/
    ZF2_encode_filter,
    ZF2_decode_filter,

    /*Filter Conversion*/
    ZF2_hdf2codec,
    ZF2_codec2hdf,

    /*Search*/
    ZF2_searchobjects,

    /*Chunkkeys*/
    ZF2_encode_chunkkey,
    ZF2_decode_chunkkey,
};

const NCZ_Formatter* NCZ_formatter2 = &NCZ_formatter2_table;

int
NCZF2_initialize(void)
{
    return NC_NOERR;
}

int
NCZF2_finalize(void)
{
    return NC_NOERR;
}

/**************************************************/

/*File-Level Operations*/

/**
 * @internal Synchronize file metadata from internal to map.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ZF2_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

int
ZF2_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/

/*Dowload JSON Metadata*/
int
ZF2_download_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* fullpath = NULL;
    char* key = NULL;

    /* Download .zgroup and .zattrs */
    if((stat = NCZ_grpkey(grp,&fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2GROUP,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jobj))) goto done;
    nullfree(key); key = NULL;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jatts))) goto done;
    zobj->constjatts = 0;    

done:
    nullfree(key);
    nullfree(fullpath);
    return THROW(stat);
}

int
ZF2_download_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* fullpath = NULL;
    char* key = NULL;

    /* Download .zgroup and .zattrs */
    if((stat = NCZ_varkey(var,&fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jobj))) goto done;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jatts))) goto done;
    zobj->constjatts = 0;    

done:
    nullfree(key);
    nullfree(fullpath);
    return THROW(stat);
}

int
ZF2_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj, NCjson** jzgrpp, NCjson** jzsuperp)
{
    int stat = NC_NOERR;
    /* Extract _nczarr_group from zobj->attr */
    NCJcheck(NCJdictget(zobj->jatts,NCZ_GROUP,(NCjson**)jzgrpp));
    /* Extract _nczarr_superblock from zobj->attr */
    NCJcheck(NCJdictget(zobj->jatts,NCZ_SUPERBLOCK,(NCjson**)jzgrpp));
done:
    return THROW(stat);
}

int
ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, const NCjson* jsuper, int* zformatp, int* nczformatp)
{
    int stat = NC_NOERR;
    const NCjson* format = NULL;
    int zformat = 0;
    int nczformat = 0;

    assert(jsuper != NULL);
    
    if(zformatp) *zformatp = 0;
    if(nczformatp) *nczformatp = 0;
    
    /* Extract the zarr format number and the nczarr format number */
    NCJcheck(NCJdictget(jsuper,"zarr_format",(NCjson**)&format));
    if(format != NULL) {
	if(NCJsort(format) != NCJ_INT) {stat = NC_ENOTZARR; goto done;}
	if(1!=sscanf(NCJstring(format),ZARR_FORMAT_VERSION_TEMPLATE,&zformat)) {stat = NC_ENOTZARR; goto done;}
    }
    NCJcheck(NCJdictget(jsuper,"nczarr_format",(NCjson**)&format));
    if(format != NULL) {
	if(NCJsort(format) != NCJ_INT) {stat = NC_ENOTZARR; goto done;}
	if(1!=sscanf(NCJstring(format),NCZARR_FORMAT_VERSION_TEMPLATE,&nczformat)) {stat = NC_ENOTZARR; goto done;}
    }
    
    if(zformatp) *zformatp = zformat;
    if(nczformatp) *nczformatp = nczformat;

done:
    return THROW(stat);
}

int
ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jnczgrp, NClist* vars, NClist* subgrps)
{
    int stat = NC_NOERR;
    size_t i;
    const NCjson* jvalue = NULL;
    NClist* dimdefs = nclistnew(); /* NClist<struct NCZ_DimInfo> */

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJcheck(NCZ_dictgetalt2(jnczgrp,&jvalue,"dimensions","dims"));
    if(jvalue != NULL) {
	if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Decode the dimensions defined in this group */
	if((stat = decode_grp_dims(file,grp,jvalue,dimdefs))) goto done;
    }

    NCJcheck(NCZ_dictgetalt2(jnczgrp,&jvalue,"arrays","vars"));
    if(jvalue != NULL) {
	/* Extract the variable names in this group */
	for(i=0;i<NCJarraylength(jvalue);i++) {
	    NCjson* jname = NCJith(jvalue,i);
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    nclistpush(vars,strdup(norm_name));
	}
    }

    NCJcheck(NCJdictget(jnczgrp,"groups",(NCjson**)&jvalue));
    if(jvalue != NULL) {
	/* Extract the subgroup names in this group */
	for(i=0;i<NCJarraylength(jvalue);i++) {
	    NCjson* jname = NCJith(jvalue,i);
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    nclistpush(subgrps,strdup(norm_name));
	}
    }

    /* Declare the dimensions in this group */
    for(i=0;i<nclistlength(dimdefs);i++) {
	struct NCZ_DimInfo* di = (struct NCZ_DimInfo*)nclistget(dimdefs,i);
	if((stat = ncz4_create_dim(file,grp,di,NULL))) goto done;
    }

done:
    NCZ_reclaim_diminfo_list(dimdefs);
    return ZUNTRACE(THROW(stat));
}

int
ZF2_decode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj, NClist* jfilters, size64_t** shapesp, size64_t** chunksp, NClist* dimrefs)
{
    int stat = NC_NOERR;
    size_t j;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    int purezarr = 0;
    /* per-variable info */
    NCZ_VAR_INFO_T* zvar = NULL;
    const NCjson* jvar = NULL;
    const NCjson* jatts = NULL;
    const NCjson* jncvar = NULL;
    const NCjson* jdimrefs = NULL;
    const NCjson* jvalue = NULL;
    int varsized = 0;
    int suppress = 0; /* Abort processing of this variable */
    nc_type vtype = NC_NAT;
    size_t maxstrlen = 0;
    size_t netcdf_rank = 0;  /* true rank => scalar => 0 */
    size_t zarr_rank = 0; /* |shape| */
    struct NCZ_AttrInfo ainfo;
    size64_t* shapes = NULL;
    size64_t* chunks = NULL;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    const NCjson* jfilter = NULL;
#endif

    TESTPUREZARR;

    memset(&ainfo,0,sizeof(ainfo));

    jvar = zobj->jobj;
    assert(jvar != NULL);
    jatts = zobj->jatts;

    /* Verify the format */
    {
	int format;
	NCJcheck(NCJdictget(jvar,"zarr_format",(NCjson**)&jvalue));
	sscanf(NCJstring(jvalue),ZARR_FORMAT_VERSION_TEMPLATE,&format);
	if(format != zinfo->zarr.zarr_format) {stat = (THROW(NC_ENCZARR)); goto done;}
    }

    /* Set the type and endianness of the variable */
    {
	int endianness;
	NCJcheck(NCJdictget(jvar,"dtype",(NCjson**)&jvalue));
	/* Convert dtype to nc_type + endianness */
	if((stat = dtype2nctype(file,NCJstring(jvalue),!ISATTR,&vtype,&endianness,&maxstrlen))) goto done;
	if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
	    /* Locate the NC_TYPE_INFO_T object */
	    if((stat = ncz_gettype(file,var->container,vtype,&var->type_info))) goto done;
	} else {stat = NC_EBADTYPE; goto done;}
	var->endianness = endianness;
	var->type_info->endianness = var->endianness; /* Propagate */
	if(vtype == NC_STRING) {
	    zvar->maxstrlen = maxstrlen;
	    if(zvar->maxstrlen == 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
	}
    }

    { /* Extract the shape */
	NCJcheck(NCJdictget(jvar,"shape",(NCjson**)&jvalue));
	if(NCJsort(jvalue) != NCJ_ARRAY) {stat = THROW(NC_ENOTZARR); goto done;}
	zarr_rank = NCJarraylength(jvalue);
	if(zarr_rank == 0)  {stat = THROW(NC_ENOTZARR); goto done;}
	if((shapes = (size64_t*)calloc(zarr_rank,sizeof(size64_t)))==NULL) {stat = NC_ENOMEM; goto done;}
	if((stat=NCZ_decodesizet64vec(jvalue, &zarr_rank, shapes))) goto done;	
    }

    /* Rank processing */
    {
	if(zarr_rank == 0) {
	    /* suppress variable */
	    ZLOG(NCLOGWARN,"Empty shape for variable %s suppressed",var->hdr.name);
	    suppress = 1;
	    goto suppressvar;
	}
	if(zvar->scalar) {
	    netcdf_rank = 1; /* Always chunked */
	    zarr_rank = 1; /* Zarr does not support scalars */
	} else 
	    netcdf_rank = zarr_rank;
	assert(netcdf_rank > 0);
	/* Set the rank of the variable */
	if((stat = nc4_var_set_ndims(var, netcdf_rank))) goto done;
    }

    /*
     * Collect the dimension names for this variable.
     * In order of preference:
     * 1. _nczarr_var.dimensions -- the name are FQNs.
     * 2. _ARRAY_DIMENSIONS -- the xarray names are relative names and are scoped to root group.
     * 3. _Anonymous_Dim_n -- scoped to root group and n is the length of the dimensions.
     */
    if(!purezarr) {
	if(jatts == NULL) {stat = NC_ENCZARR; goto done;}
	/* Extract the _NCZARR_ARRAY values */
	/* Do this first so we know about storage esp. scalar */
	/* Extract the NCZ_ARRAY dict */
	if((stat = NCZ_getnczarrkey(file,zobj,NCZ_ARRAY,&jncvar))) goto done;
	if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
	assert((NCJsort(jncvar) == NCJ_DICT));
	/* Extract scalar flag */
	if((stat = NCJdictget(jncvar,"scalar",(NCjson**)&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL) zvar->scalar = 1;
	/* Ignore storage flag and treat everything as chunked */
	var->storage = NC_CHUNKED;
	/* Extract dimrefs list	 */
	if((stat = NCZ_dictgetalt2(jncvar,&jdimrefs,"dimension_references","dimrefs"))) goto done;
	if(jdimrefs != NULL) {
	    assert((NCJsort(jdimrefs) == NCJ_ARRAY));
	    if(zvar->scalar) {
		assert(NCJarraylength(jdimrefs) == 1);
	    } else {
		for(j=0;j<zarr_rank;j++) {
		    const NCjson* dimpath = NCJith(jdimrefs,j);
		    assert(NCJsort(dimpath) == NCJ_STRING);
		    nclistpush(dimrefs,strdup(NCJstring(dimpath)));
		}
	    }
	    jdimrefs = NULL; /* avoid double free */
	    if(nclistlength(dimrefs) != zarr_rank) {stat = NC_ENOTZARR; goto done;}
	}
    }
    if(nclistlength(dimrefs) == 0) { /* Try XARRAY Attribute */
	NCJcheck(NCJdictget(jvar,NC_XARRAY_DIMS,(NCjson**)&jvalue));
	if(jvalue != NULL) {
	    zvar->scalar = 0; /* Xarray does not support scalars */
	    assert((NCJsort(jdimrefs) == NCJ_ARRAY));
	    if(NCJarraylength(jvalue) != zarr_rank) {stat = NC_ENOTZARR; goto done;}
	    for(j=0;j<zarr_rank;j++) {
		const NCjson* dimpath = NCJith(jvalue,j);
		assert(NCJsort(dimpath) == NCJ_STRING);
		nclistpush(dimrefs,strdup(NCJstring(dimpath)));
	    }
	}
	if(nclistlength(dimrefs) != zarr_rank) {stat = NC_ENOTZARR; goto done;}
    }
    if(nclistlength(dimrefs) == 0) { /* Finally, simulate it from the shape of the variable */
	char anonname[NC_MAX_NAME];
	for(j=0;j<zarr_rank;j++) {
	    snprintf(anonname,sizeof(anonname),"/%s_%lld",NCDIMANON,shapes[j]);
	    nclistpush(dimrefs,strdup(anonname));
	}
    }

    /* Capture dimension_separator (must precede chunk cache creation) */
    {
	NCglobalstate* ngs = NC_getglobalstate();
	assert(ngs != NULL);
	zvar->dimension_separator = 0;
	if((stat = NCJdictget(jvar,"dimension_separator",(NCjson**)&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL) {
	    /* Verify its value */
	    if(NCJsort(jvalue) == NCJ_STRING && NCJstring(jvalue) != NULL && strlen(NCJstring(jvalue)) == 1)
	       zvar->dimension_separator = NCJstring(jvalue)[0];
	}
	/* If value is invalid, then use global default */
	if(!islegaldimsep(zvar->dimension_separator))
	    zvar->dimension_separator = ngs->zarr.dimension_separator; /* use global value */
	assert(islegaldimsep(zvar->dimension_separator)); /* we are hosed */
    }

    /* fill_value; must precede calls to adjust cache */
    {
	NCJcheck(NCJdictget(jvar,"fill_value",(NCjson**)&jvalue));
	if(jvalue == NULL || NCJsort(jvalue) == NCJ_NULL)
	    var->no_fill = 1;
	else {
	    var->no_fill = 0;
	    ainfo.nctype = vtype;
	    ainfo.jdata = jvalue;
	    if((stat = NCZ_computeattrdata(&ainfo))) goto done;
	    assert(ainfo.nctype == vtype);
	    /* Note that we do not create the _FillValue
	       attribute here to avoid having to read all
	       the attributes and thus foiling lazy read.*/
	}
    }

    /* chunks */
    {
	if((stat = NCJdictget(jvar,"chunks",(NCjson**)&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_ARRAY)
	    {stat = (THROW(NC_ENCZARR)); goto done;}
#if 0
	/* Verify the rank */
	if(zvar->scalar || zarr_rank == 0) {
	    if(var->ndims != 0)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    zvar->chunkproduct = 1;
	    zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	    /* Create the cache */
	    if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
	}
#endif
        if(zarr_rank == 0) {stat = NC_ENCZARR; goto done;}
	if(var->ndims != netcdf_rank) {stat = (THROW(NC_ENCZARR)); goto done;}
        var->storage = NC_CHUNKED;
	if((chunks = malloc(sizeof(size64_t)*zarr_rank)) == NULL) {stat = NC_ENOMEM; goto done;}
	if((stat = NCZ_decodesizet64vec(jvalue, &zarr_rank, chunks))) goto done;
#if 0
	    if((var->chunksizes = malloc(sizeof(size_t)*(size_t)zarr_rank)) == NULL)
		{stat = NC_ENOMEM; goto done;}
#endif
#if 0
	    /* validate the chunk sizes */
	    zvar->chunkproduct = 1;
	    for(j=0;j<netcdf_rank;j++) {
		if(chunks[j] == 0)
		    {stat = (THROW(NC_ENCZARR)); goto done;}
		var->chunksizes[j] = (size_t)chunks[j];
		zvar->chunkproduct *= chunks[j];
	    }
	    zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	    /* Create the cache */
	    if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
	}
	if((stat = NCZ_adjust_var_cache(var))) goto done;
#endif
    }

    /* Capture row vs column major; currently, column major not used*/
    {
	if((stat = NCJdictget(jvar,"order",(NCjson**)&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(strcmp(NCJstring(jvalue),"C") > 0)
	    ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 1;
	else ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 0;
    }

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* Do filters key before compressor key so final filter chain is in correct order */
    {
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"filters",(NCjson**)&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_NULL) {
	    int k;
	    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
	    for(k=0;;k++) {
		jfilter = NULL;
		jfilter = NCJith(jvalue,k);
		if(jfilter == NULL) break; /* done */
		if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
		nclistpush(jfilters,jfilter);
	    }
	}
    }
    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    { 
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"compressor",(NCjson**)&jfilter))<0) {stat = NC_EINVAL; goto done;}
	if(jfilter != NULL && NCJsort(jfilter) != NCJ_NULL) {
	    if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
	    nclistpush(jfilters,jfilter);
	}
    }
    /* Suppress variable if there are filters and var is not fixed-size */
    if(varsized && nclistlength((NClist*)var->filters) > 0)
	suppress = 1;
#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/

#if 0
    if(zarr_rank > 0) {
	/* Convert dimrefs to specific dimensions */
	if((stat = computedimrefs(file, var, netcdf_rank, dimnames, shapes, var->dim))) goto done;
	if(!zvar->scalar) {
	    /* Extract the dimids */
	    for(j=0;j<netcdf_rank;j++)
		var->dimids[j] = var->dim[j]->hdr.id;
	}
    }
#endif /*0*/

suppressvar:
    if(suppress) {
	NC_GRP_INFO_T* grp = var->container;
	/* Reclaim NCZarr variable specific info */
	(void)NCZ_zclose_var1(var);
	/* Remove from list of variables and reclaim the top level var object */
	(void)nc4_var_list_del(grp, var);
	var = NULL;
    }

    if(shapesp) {*shapesp = shapes; shapes = NULL;}
    if(chunksp) {*chunksp = chunks; chunks = NULL;}

done:
    nullfree(chunks);
    NCZ_clearAttrInfo(&ainfo);
    nullfree(shapes); shapes = NULL;
    return THROW(stat);
}

int
ZF2_decode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jncvar, const NCjson* jatts)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/

int
ZF2_upload_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* fullpath = NULL;
    char* key = NULL;

    /* Construct grp key */
    if((stat = NCZ_grpkey(grp,&fullpath))) goto done;

    /* build ZGROUP path */
    if((stat = nczm_concat(fullpath,Z2GROUP,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,zobj->jobj))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,zobj->jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nullfree(fullpath);
    nullfree(key);
    return THROW(stat);
}

int
ZF2_upload_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* fullpath = NULL;
    char* key = NULL;

    /* Construct var key */
    if((stat = NCZ_varkey(var,&fullpath))) goto done;

    /* build ZARRAY path */
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,zobj->jobj))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,zobj->jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nullfree(fullpath);
    nullfree(key);
    return THROW(stat);
    return THROW(stat);
}

/*Write JSON Metadata*/
int
ZF2_encode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    char version[64];
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jsuper = NULL;
    /* create superblock */
    snprintf(version,sizeof(version),NCZARR_FORMAT_VERSION_TEMPLATE, zinfo->zarr.nczarr_format);
    NCJnew(NCJ_DICT,&jsuper);
    NCJcheck(NCJinsertstring(jsuper,"version",version));
    if(jsuperp) {*jsuperp = jsuper; jsuper = NULL;}
done:
    NCJreclaim(jsuper);
    return THROW(stat);
}

int
ZF2_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jnczgrpp)
{
    int stat = NC_NOERR;
    size_t i;
    NCjson* jnczgrp = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrps = NULL;

    /* Create the NCZ_GROUP dict */
    NCJnew(NCJ_DICT,&jnczgrp);

    /* Collect and encode the grp dimension declarations */
    NCJnew(NCJ_DICT,&jdims);
    for(i=0;i<ncindexsize(grp->dim);i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
	NCJcheck(NCJinsertint(jdims,dim->hdr.name,(long long)dim->len));
    }
    NCJcheck(NCJinsert(jnczgrp,"dimensions",jdims)); jdims = NULL;

    /* Collect and insert the variable names in this group */
    NCJnew(NCJ_ARRAY,&jvars);
    for(i=0;i<ncindexsize(grp->vars);i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	NCJcheck(NCJaddstring(jvars,NCJ_STRING,var->hdr.name));
    }
    NCJcheck(NCJinsert(jnczgrp,"arrays",jvars)); jvars = NULL;

    /* Collect and insert the variable names in this group */
    NCJnew(NCJ_ARRAY,&jsubgrps);
    for(i=0;i<ncindexsize(grp->children);i++) {
        NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	NCJcheck(NCJaddstring(jsubgrps,NCJ_STRING,grp->hdr.name));
    }
    NCJcheck(NCJinsert(jnczgrp,"groups",jsubgrps)); jsubgrps = NULL;

    if(jnczgrpp) {*jnczgrpp = jnczgrp; jnczgrp = NULL;}
done:
    return THROW(stat);
}

int
ZF2_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jattsp, NCjson** jgroupp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jgroup = NULL;

    NCJnew(NCJ_DICT,&jgroup);
    NCJcheck(NCJinsertint(jgroup,"zarr_format",zinfo->zarr.zarr_format));
    if(jgroupp) {*jgroupp = jgroup; jgroup = NULL;}
done:
    NCJreclaim(jgroup);
    return THROW(stat);
}

int
ZF2_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
{
    int stat = NC_NOERR;
    NClist* dimrefs = NULL;
    NCjson* jnczarray = NULL;
    NCjson* jdimrefs = NULL;
    NCbytes* dimfqn = ncbytesnew();
    size_t i;

    NCJnew(NCJ_DICT,&jnczarray);

    if(var->ndims > 0) {
        if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_makeFQN(file->root_grp,dim->hdr.name,dimfqn))) goto done;
	    nclistpush(dimrefs,ncbytesextract(dimfqn));
	}
    }

    /* Create the dimrefs json object */
    NCJnew(NCJ_ARRAY,&jdimrefs);
    for(i=0;i<nclistlength(dimrefs);i++) {
	char* fqn = (char*)nclistremove(dimrefs,0);
	NCJaddstring(jdimrefs,NCJ_STRING,fqn);
    }
    /* Insert dimension_references  */
    NCJcheck(NCJinsert(jnczarray,"dimension_references",jdimrefs)); jdimrefs = NULL;

    /* Add the _Storage flag */
    /* Record if this is a scalar */
    if(var->ndims == 0) {
        NCJcheck(NCJinsertint(jnczarray,"scalar",1));
    }

    /* everything looks like it is chunked */
    NCJcheck(NCJinsertstring(jnczarray,"storage","chunked"));

    if(jnczarrayp) {*jnczarrayp = jnczarray; jnczarray = NULL;}
done:
    nclistfreeall(dimrefs);
    NCJreclaim(jnczarray);
    NCJreclaim(jdimrefs);
    return THROW(stat);
}

int
ZF2_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jattsp, NClist* filtersj, NCjson** jvarp)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    NCjson* jvar = NULL;
    NCjson* jshape = NULL;
    NCjson* jchunks = NULL;
    NCjson* jfill = NULL;
    size64_t shape[NC_MAX_VAR_DIMS];
    char number[1024];
    size_t zarr_rank = 0;
    size_t i;
    NCjson* jcompressor = NULL;
    NCjson* jfilters = NULL;
    
    NCJnew(NCJ_DICT,&jvar);

    /* if scalar */
    if(var->ndims == 0) {
        shape[0] = 1;
	zarr_rank = 1;
    } else {
        zarr_rank = var->ndims;
	for(i=0;i<var->ndims;i++) { /* Collect the shape vector */
	    NC_DIM_INFO_T* dim = var->dim[i];
	    shape[i] = dim->len;
	}
    }

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    /* Create the list */
    NCJnew(NCJ_ARRAY,&jshape);
    for(i=0;i<zarr_rank;i++) {
	snprintf(number,sizeof(number),"%llu",shape[i]);
	NCJaddstring(jshape,NCJ_INT,number);
    }
    NCJcheck(NCJinsert(jvar,"shape",jshape)); jshape = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    {	/* Add the type name */
	int endianness = var->type_info->endianness;
	int atomictype = var->type_info->hdr.id;
	char* dtypename = NULL;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = nctype2dtype(file,atomictype,endianness,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename,NULL))) goto done;
	NCJcheck(NCJinsertstring(jvar,"dtype",dtypename));
	nullfree(dtypename); dtypename = NULL;
    }

    /* chunks key */
    /* The zarr format does not support the concept
       of contiguous (or compact), so it will never appear in the read case.
    */
    /* Create the list of chunksizes */
    NCJnew(NCJ_ARRAY,&jchunks);
    if(zvar->scalar) {
	NCJaddstring(jchunks,NCJ_INT,"1"); /* one chunk of size 1 */
    } else for(i=0;i<zarr_rank;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(number,sizeof(number),"%lld",len);
	NCJaddstring(jchunks,NCJ_INT,number);
    }
    NCJcheck(NCJinsert(jvar,"chunks",jchunks)); jchunks = NULL;

    /* fill_value key */
    if(var->no_fill) {
	NCJnew(NCJ_NULL,&jfill);
    } else {/*!var->no_fill*/
	int atomictype = var->type_info->hdr.id;
        if(var->fill_value == NULL) {
	     if((stat = NCZ_ensure_fill_value(var))) goto done;
	}
        /* Convert var->fill_value to a string */
	if((stat = NCZ_stringconvert(atomictype,1,var->fill_value,&jfill))) goto done;
	assert(jfill->sort != NCJ_ARRAY);
    }
    NCJcheck(NCJinsert(jvar,"fill_value",jfill)); jfill = NULL;

    /* order key */
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */
    NCJcheck(NCJinsertstring(jvar,"order","C"));

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Compressor and Filters */
    if(nclistlength(filtersj) > 0) {
	jcompressor = (NCjson*)nclistremove(filtersj,nclistlength(filtersj)-1);
    } else
#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/
    { /* no filters at all; default compressor to null */
        NCJnew(NCJ_NULL,&jcompressor);
    }
    NCJcheck(NCJinsert(jvar,"compressor",jcompressor)); jcompressor = NULL;

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if(nclistlength(filtersj) >= 2) {
	size_t k;
	/* filters holds the array of encoded filters */
	NCJnew(NCJ_ARRAY,&jfilters);
	for(k=0;k<nclistlength(filtersj)-1;k++) {
	    NCjson* jfilter = (NCjson*)nclistremove(filtersj,0);
	    NCJcheck(NCJappend(jfilters,jfilter)); jfilter = NULL;
	}
    } else
#endif
    {
        NCJnew(NCJ_NULL,&jfilters); /* no filters at all */
    }
    NCJcheck(NCJinsert(jvar,"filters",jfilters)); jfilters = NULL;

    /* dimension_separator key */
    /* Single char defining the separator in chunk keys */
    if(zvar->dimension_separator != DFALT_DIM_SEPARATOR_V2) {
	char sep[2];
	sep[0] = zvar->dimension_separator;/* make separator a string*/
	sep[1] = '\0';
        NCJcheck(NCJinsertstring(jvar,"dimension_separator",sep));
    }

    if(jvarp) {*jvarp = jvar; jvar = NULL;}

done:
    NCJreclaim(jvar);
    NCJreclaim(jshape);
    NCJreclaim(jchunks);
    NCJreclaim(jfill);
    NCJreclaim(jcompressor);
    NCJreclaim(jfilters);
    return THROW(stat);
}

int
ZF2_encode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jnczconp, NCjson** jattsp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    size_t i;
    NCindex* atts = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jdata = NULL;
    NCjson* jnczatt = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* d2name = NULL;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
    const char* nczname = NULL;
    int purezarr = 0;
    size_t typesize = 0;

    TESTPUREZARR;

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	atts = var->att;
	nczname = NCZ_GROUP;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
	atts = grp->att;
	nczname = NCZ_ARRAY;
    }
    
    if(ncindexsize(atts) > 0) {
        NCJnew(NCJ_DICT,&jatts);
        NCJnew(NCJ_DICT,&jtypes);

        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(atts);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(atts,i);
	    char* d2attr = NULL;

	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE) {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid != NC_STRING)
		typesize = (size_t)NCZ_get_maxstrlen(container);
	    else {
	        if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;
	    }
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;

	    /* Collect the corresponding dtype */
            if((stat = nctype2dtype(file,a->nc_typeid,endianness,typesize,NULL,&d2name))) goto done;

	    /* Insert the attribute; optionally consumes jdata and d2name */
	    if((stat = ncz_insert_attr(jatts,jtypes,a->hdr.name,&jdata,d2name))) goto done;

	    /* cleanup */
	    NCJreclaim(jdata); jdata = NULL;
	    nullfree(d2name); d2name = NULL;
            nullfree(d2attr); d2attr = NULL;
        }
    }

    /* Finalize the contents of jtypes and jatts */
    if(!purezarr) {
        if(jtypes == NULL) NCJnew(NCJ_DICT,&jtypes);
        if(jatts == NULL) NCJnew(NCJ_DICT,&jatts);
        /* Build _nczarr_attrs */
	NCJnew(NCJ_DICT,&jnczatt);
	NCJcheck(NCJinsert(jnczatt,"types",jtypes));
	/* WARNING, jtypes may undergo further changes */
        /* Insert _nczarr_attrs + type */	
	if((stat=ncz_insert_attr(jatts,jtypes,NCZ_ATTR,&jnczatt,"|J1"))) goto done;
        /* Insert _nczarr_group|_nczarr_var + type */
	if((stat = ncz_insert_attr(jatts,jtypes,nczname,jnczconp,"|J1"))) goto done;
	jtypes = NULL;
	assert(*jnczconp == NULL && jnczatt == NULL && jtypes == NULL);
    }

    if(jattsp) {*jattsp = jatts; jatts = NULL;}

done:
    nullfree(d2name);
    NCJreclaim(jdata);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
    return THROW(stat);
}

/*Filter Processing*/
static int
ZF2_encode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter, NCjson** jfilterp)
{
    int stat = NC_NOERR;
    NCjson* jfilter = NULL;
    
    /* assumptions */
    assert(filter->flags & FLAG_WORKING);

    /* Convert the HDF5 id + parameters to the codec form */

    /* We need to ensure the the current visible parameters are defined and had the opportunity to come
       from the working parameters */
    assert((filter->flags & (FLAG_VISIBLE | FLAG_WORKING)) == (FLAG_VISIBLE | FLAG_WORKING));

    /* Convert the visible parameters back to codec */
    /* Clear any previous codec */
    nullfree(filter->codec.id); filter->codec.id = NULL;
    nullfree(filter->codec.codec); filter->codec.codec = NULL;
    filter->codec.id = strdup(filter->plugin->codec.codec->codecid);
    if(filter->plugin->codec.codec->NCZ_hdf5_to_codec) {
	if((stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(NCplistzarrv2,filter->hdf5.id,filter->hdf5.visible.nparams,filter->hdf5.visible.params,&filter->codec.codec))) goto done;
    } else
        {stat = NC_EFILTER; goto done;}

    /* Parse the codec as the return */
    NCJcheck(NCJparse(filter->codec.codec,0,&jfilter));
    if(jfilterp) {*jfilterp = jfilter; jfilter = NULL;}

done:
    NCJreclaim(jfilter);
    return THROW(stat);
}

static int
ZF2_decode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jfilter, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    const NCjson* jvalue = NULL;
    NCZ_Plugin* plugin = NULL;
    NCZ_Codec codec = NCZ_codec_empty();
    NCZ_HDF5 hdf5 = NCZ_hdf5_empty();

    if(var->filters == NULL) var->filters = nclistnew();

    /* Get the id of this codec filter */
    NCJcheck(NCJdictget(jfilter,"id",(NCjson**)&jvalue));
    if(NCJsort(jvalue) != NCJ_STRING) {stat = THROW(NC_ENOFILTER); goto done;}

    /* Save the codec */
    if((codec.id = strdup(NCJstring(jvalue)))==NULL) {stat = NC_ENOMEM; goto done;}
    NCJcheck(NCJunparse(jfilter,0,&codec.codec));

    /* Find the plugin for this filter */
    if((stat = NCZ_plugin_lookup(codec.id,&plugin))) goto done;
    assert(filter->plugin == NULL);
    filter->plugin = plugin; plugin = NULL; /* Might be NULL */

    /* Will always have a filter; possibly unknown */ 
    if((filter = calloc(1,sizeof(NCZ_Filter)))==NULL) {stat = NC_ENOMEM; goto done;}		

    if(plugin != NULL) {
	/* Save the hdf5 id */
	hdf5.id = plugin->codec.codec->hdf5id;
	/* Convert the codec to hdf5 form visible parameters */
        if(plugin->codec.codec->NCZ_codec_to_hdf5) {
            if((stat = plugin->codec.codec->NCZ_codec_to_hdf5(NCplistzarrv2,codec.codec,&hdf5.id,&hdf5.visible.nparams,&hdf5.visible.params)))
	        goto done;
	}
	filter->flags |= FLAG_VISIBLE;
	filter->hdf5 = hdf5; hdf5 = NCZ_hdf5_empty();
	filter->codec = codec; codec = NCZ_codec_empty();
        filter->plugin = plugin; plugin = NULL;
    } else {
        /* Create a fake filter so we do not forget about this codec */
	filter->hdf5 = NCZ_hdf5_empty();
	filter->codec = codec; codec = NCZ_codec_empty();
    }

done:
    ncz_hdf5_clear(&hdf5);
    ncz_codec_clear(&codec);
    return THROW(stat);
}

/*Filter Conversion*/

int
ZF2_hdf2codec (NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_codec2hdf(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/*Search*/
int
ZF2_searchobjects(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrpnames)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* grpkey = NULL;
    char* varkey = NULL;
    char* zarray = NULL;
    NClist* matches = nclistnew();

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_listall(zfile->map,grpkey,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
	const char* name = nclistget(matches,i);
	if(name[0] == NCZM_DOT) continue; /* zarr/nczarr specific */
	/* See if name/.zarray exists */
	if((stat = nczm_concat(grpkey,name,&varkey))) goto done;
	if((stat = nczm_concat(varkey,Z2ARRAY,&zarray))) goto done;
	if((stat = nczmap_exists(zfile->map,zarray)) == NC_NOERR)
	    nclistpush(varnames,strdup(name));
	stat = NC_NOERR;
	nullfree(varkey); varkey = NULL;
	nullfree(zarray); zarray = NULL;
    }

done:
    nullfree(grpkey);
    nullfree(varkey);
    nullfree(zarray);
    nclistfreeall(matches);
    return stat;
}

/*Chunkkeys*/

/*
From Zarr V2 Specification:
"The compressed sequence of bytes for each chunk is stored under
a key formed from the index of the chunk within the grid of
chunks representing the array.	To form a string key for a
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

int
ZF2_encode_chunkkey(NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_decode_chunkkey(NC_FILE_INFO_T* file, char* dimsep, char* chunkname, size_t* rankp, size64_t** chunkindicesp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/
/* Support Functions */

static int
decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims, NClist* dimdefs)
{
    int stat = NC_NOERR;
    size_t i;
    struct NCZ_DimInfo* dimdef = NULL;

    assert(NCJsort(jdims) == NCJ_DICT);
    for(i=0;i<NCJdictlength(jdims);i++) {
	const NCjson* jname= NCJdictkey(jdims,i);
	const NCjson* jdim = NCJdictvalue(jdims,i);
	struct NCJconst cvt;

	memset(&cvt,0,sizeof(cvt));
	if((dimdef = (struct NCZ_DimInfo*)calloc(1,sizeof(struct NCZ_DimInfo)))==NULL) goto done;

	/* Verify name legality */
	if((stat = nc4_check_name(NCJstring(jname), dimdef->norm_name))) {stat = NC_EBADNAME; goto done;}

	if(NCJisatomic(jdim)) { /* old-style length only dimension spec */
	    NCJcheck(NCJcvt(jdim,NCJ_INT,&cvt));	    
	    dimdef->shape = (size64_t)cvt.ival;
	    dimdef->unlimited = 0;
	} else {
	    const NCjson* jsize = NULL;
	    const NCjson* junlim = NULL;
	    assert(NCJsort(jdim) == NCJ_DICT);
	    NCJcheck(NCJdictget(jdim,"size",(NCjson**)&jsize));
	    NCJcheck(NCJdictget(jdim,"unlimited",(NCjson**)&junlim));
	    NCJcheck(NCJcvt(jsize,NCJ_INT,&cvt));
	    dimdef->shape = (size64_t)cvt.ival;
	    memset(&cvt,0,sizeof(cvt));
	    NCJcheck(NCJcvt(junlim,NCJ_INT,&cvt));
	    dimdef->unlimited = (cvt.ival == 0 ? 0 : 1);
	}
	nclistpush(dimdefs,dimdef); dimdef = NULL;
    }	 

done:
    NCZ_reclaim_diminfo(dimdef);
    return THROW(stat);
}

/* Type Converters */

/**
@internal Zarr V2: Given an nc_type+endianness+purezarr+MAXSTRLEN, produce the corresponding Zarr type string.
@param file	  - [in]
@param nctype	  - [in] nc_type
@param endianness - [in] endianness
@param typesize   - [in] max string length
@param dtypep	  - [out] hold the corresponding dtype
@param dattrtypep - [out] hold the corresponding dtype if used as an attribute
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

static int
nctype2dtype(NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typesize, char** dtypep, char** dattrtypep)
{
    char dtype[64];
    char dattrtype[64];
    const char* dtemplate = NULL;
    const char* dattrtemplate = NULL;

    if(nctype <= NC_NAT || nctype > N_NCZARR_TYPES) return NC_EINVAL;
    dtemplate = znamesv2[nctype].dtype;
    dattrtemplate = znamesv2[nctype].dtypeattr;
    if(dattrtemplate == NULL) dattrtemplate = dtemplate;
    snprintf(dtype,sizeof(dtype),dtemplate,typesize);
    snprintf(dattrtype,sizeof(dattrtype),dattrtemplate,typesize);
    /* Set endianness */
    switch (nctype) {
    case NC_STRING:
    case NC_CHAR:
    case NC_JSON:
	break;	  
    default:
	switch (endianness) {
	case NC_ENDIAN_LITTLE: dtype[0] = '<'; break;
	case NC_ENDIAN_BIG: dtype[0] = '>'; break;
	case NC_ENDIAN_NATIVE: default: break;
	}
	dattrtype[0] = dtype[0];
    }
    if(dtypep) *dtypep = strdup(dtype);
    if(dattrtypep) *dattrtypep = strdup(dattrtype);
    return NC_NOERR;		
}

/*
@internal Convert a numcodecs Zarr v2 dtype spec to a corresponding nc_type.
@param file	  - [in]
@param dtype	  - [in] dtype the dtype to convert
@param isattr	  - [in] 1 => type came from an attribute
@param nctypep	  - [out] hold corresponding type
@param endianp	  - [out] hold corresponding endianness
@param maxstrlenp - [out] hold corresponding type size (for fixed length strings)
@return NC_NOERR
@return NC_EINVAL
@author Dennis Heimbigner
*/

static int
dtype2nctype(NC_FILE_INFO_T* file, const char* dtype, int isattr, nc_type* nctypep, int* endianp, size_t* maxstrlenp)
{
    int stat = NC_NOERR;
    size_t typelen = 0;
    size_t maxstrlen = 0;
    char tchar;
    nc_type nctype = NC_NAT;
    int endianness = -1;
    const char* p;
    int n,count;

    if(nctypep) *nctypep = NC_NAT;
    if(endianp) *endianp = NC_ENDIAN_NATIVE;
    if(maxstrlenp) *maxstrlenp = 0;

    if(dtype == NULL) {stat = NC_ENCZARR; goto done;}

    /* Handle special cases */
    if(strcmp(dtype,"|J0")==0) {
	nctype = NC_JSON;
	typelen = 1;
	goto exit;
    } else if(strcmp(dtype,">S1")==0) {
	nctype = NC_CHAR;
	typelen = 1;
	goto exit;
    } else if(memcmp(dtype,"|S",2)==0) {
	nctype = NC_STRING;
	sscanf(dtype,"|S%zu",&maxstrlen);
	goto exit;
    }

    /* Parse the dtype; should be a numeric type by now */
    p = dtype;
    switch (*p++) {
    case '<': endianness = NC_ENDIAN_LITTLE; break;
    case '>': endianness = NC_ENDIAN_BIG; break;
    case '|': endianness = NC_ENDIAN_NATIVE; break;
    default: p--; endianness = NC_ENDIAN_NATIVE; break;
    }
    tchar = *p++; /* get the base type */
    /* Decode the type length */
    count = sscanf(p,"%zu%n",&typelen,&n);
    if(count == 0) {stat = NC_ENCZARR; goto done;}
    p += n;

    /* Numeric cases */
    switch(typelen) {
    case 1:
	switch (tchar) {
	case 'i': nctype = NC_BYTE; break;
	case 'u': nctype = NC_UBYTE; break;
	default: {stat = NC_ENCZARR; goto done;}
	}
	break;
    case 2:
	switch (tchar) {
	case 'i': nctype = NC_SHORT; break;
	case 'u': nctype = NC_USHORT; break;
	default: {stat = NC_ENCZARR; goto done;}
	}
	break;
    case 4:
	switch (tchar) {
	case 'i': nctype = NC_INT; break;
	case 'u': nctype = NC_UINT; break;
	case 'f': nctype = NC_FLOAT; break;
	default: {stat = NC_ENCZARR; goto done;}
	}
	break;
    case 8:
	switch (tchar) {
	case 'i': nctype = NC_INT64; break;
	case 'u': nctype = NC_UINT64; break;
	case 'f': nctype = NC_DOUBLE; break;
	default: {stat = NC_ENCZARR; goto done;}
	}
	break;
    default: {stat = NC_ENCZARR; goto done;}
    }

exit:
    if(nctypep) *nctypep = nctype;
    if(endianp) *endianp = endianness;
    if(maxstrlenp) *maxstrlenp = maxstrlen;

done:
    return stat;
}


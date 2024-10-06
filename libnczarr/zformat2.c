/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"
#include "znc4.h"
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
#include "netcdf_filter_build.h"
#endif

/**************************************************/
/* Forward */

static int ZF2_initialize(void);
static int ZF2_finalize(void);
static int ZF2_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF2_close(NC_FILE_INFO_T* file);
static int ZF2_download_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj);
static int ZF2_download_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj);
static int ZF2_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* jgroup, NCjson** jzgrpp, NCjson** jzsuperp);
static int ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, const NCjson* jsuper, int* zarrformat, int* nczarrformat);
static int ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jnczgrp, NClist* vars, NClist* subgrps);
static int ZF2_decode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* jvar, NCjson** jnczvarp, NClist* jfilters);
static int ZF2_decode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const struct ZOBJ* zobj);
static int ZF2_decode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jncvar, const NCjson* jatts, NCjson** jtypesp);
static int ZF2_upload_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jgrp, NCjson** jatts);
static int ZF2_upload_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jvar, NCjson** jatts);
static int ZF2_encode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp);
static int ZF2_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jatts, NCjson* jtypes, NCjson** jzgroupp);
static int ZF2_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jsuper, const NCjson* jzgroup, NCjson** jatts, NCjson** jgroupp);
static int ZF2_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jtypes, NCjson** jzvarp);
static int ZF2_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jzvar, NCjson** jatts, NClist* jfilters, NCjson** jvarp);
static int ZF2_encode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jnczvar, NCjson** jattsp, NCjson** jtypesp);
static int ZF2_encode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCZ_Filter* filter, NCjson** jfilterp);
static int ZF2_decode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter);
static int ZF2_hdf2codec(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF2_codec2hdf(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCZ_Filter* filter);
static int ZF2_searchobjects(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrpnames);
static int ZF2_encode_chunkkey(NC_FILE_INFO_T* file, size_t rank, const size64_t* chunkindices, char dimsep, char** keyp);
static int ZF2_decode_chunkkey(NC_FILE_INFO_T* file, char* dimsep, char* chunkname, size_t* rankp, size64_t** chunkindicesp);

static int decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims, NClist* dimdefs);
static int dtype2nctype(NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp);
static int nctype2dtype(NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typesize, char** dtypep, char** dattrp);

/**************************************************/
/* Format dispatch table */

static const NCZ_Formatter NCZ_FORMATTER2_table =
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
    ZF2_decode_nczarr_array,
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
    if((stat = NCZ_grpkey(grp,fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2GROUP,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jobj))) goto done;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jatts))) goto done;
    jsonz->constatts = 0;    

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
    char* norm_name = NULL;
    char* fullpath = NULL;
    char* key = NULL;

    /* Download .zgroup and .zattrs */
    if((stat = NCZ_varkey(var,fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jobj))) goto done;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&zobj->jatts))) goto done;
    jsonz->constatts = 0;    

done:
    nullfree(norm_name);
    nullfree(key);
    nullfree(fullpath);
    return THROW(stat);
}

int
ZF2_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj, NCjson** jzgrpp, NCjson** jzsuperp)
{
    int stat = NC_NOERR;
    /* Extract _nczarr_group from zobj->attr */
    NCJcheck(NCJdictget(zobj->jatts,NCZ_GROUP,jzgrpp));
    /* Extract _nczarr_superblock from zobj->attr */
    NCJcheck(NCJdictget(zobj->jatts,NCZ_SUPERBLOCK,jzgrpp));
    return THROW(stat);
}

int
ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, const NCjson* jsuper, int* zformatp, int* nczformatp)
{
    int stat = NC_NOERR;
    NCjson* format = NULL;
    int zformat = 0;
    int nczformat = 0;

    if(zformatp) *zformatp = 0;
    if(nczformatp) *nczformatp = 0;
    
    /* Extract the zarr format number and the nczarr format number */
    NCJcheck(NCJdictget(jsuper,"zarr_format",&format));
    if(format != NULL) {
        if(NCJsort(format) != NCJ_INT) {stat = NC_ENOTZARR; goto done;}
        if(1!=sscanf(NCJstring(format),ZARR_FORMAT_VERSION_TEMPLATE,&zformat)) {stat = NC_ENOTZARR; goto done;
    }
    NCJcheck(NCJdictget(jsuper,"nczarr_format",&format));
    if(format != NULL) {
        if(NCJsort(format) != NCJ_INT) {stat = NC_ENOTZARR; goto done;}
        if(1!=sscanf(NCJstring(format),NCZARR_FORMAT_VERSION_TEMPLATE,&nczformat)) {stat = NC_ENOTZARR; goto done;
    }
    
done:
    if(zformatp) *zformatp = zformat;
    if(nczformatp) *nczformatp = nczformat;
    return THROW(stat);
}

int
ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jnczgrp, NClist* vars, NClist* subgrps)
{
    int stat = NC_NOERR;
    size_t i;
    const NCjson* jvalue = NULL;
    const NCjson* jgroup = jsonz->jobj;
    struct DimInfo* dimdef = NULL;
    NClist* dimsdefs = nclistnew(); /* NClist<struct DimInfo> */

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    if((stat=NCZ_dictgetalt2(jnczgrp,&jvalue,"dimensions","dims"))) goto done;
    if(jvalue != NULL) {
	if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Decode the dimensions defined in this group */
	if((stat = decode_grp_dims(file,grp,jvalue,dimdefs))) goto done;
    }

    if((stat=NCZ_dictgetalt2(jgroup,&jvalue,"arrays","vars"))) goto done;
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

    if((stat = NCJdictget(jgroup,"groups",&jvalue))<0) {stat = NC_EINVAL; goto done;}
    if(jvalue != NULL) {
	if((stat=decode_grp_dims(file,grp,
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
        struct DimInfo* di = (struct DimInfo*)nclistget(dimdefs,i);
	if((stat = ncz4_create_dim(file,grp,di))) goto done;
    }

done:
    NCZ_reclaim_diminfo_list(dimdefs);
    return ZUNTRACE(THROW(stat));
}

int
ZF2_decode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* zobj, NCjson** jnczvarp, NClist* jfilters)
{
    int stat = NC_NOERR;
    size_t j;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int purezarr = 0;
    int xarray = 0;
    /* per-variable info */
    NCZ_VAR_INFO_T* zvar = NULL;
    const NCjson* jncvar = NULL;
    const NCjson* jdimrefs = NULL;
    const NCjson* jvalue = NULL;
    char* varpath = NULL;
    char* key = NULL;
    size64_t* shapes = NULL;
    NClist* dimnames = NULL;
    int varsized = 0;
    int suppress = 0; /* Abort processing of this variable */
    nc_type vtype = NC_NAT;
    size_t vtypelen = 0;
    size_t netcdf_rank = 0;  /* true rank => scalar => 0 */
    size_t zarr_rank = 0; /* |shape| */
    struct NCZ_AttrInfo ainfo;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    const NCjson* jfilter = NULL;
    int chainindex = 0;
#endif

    memset(&ainfo,0,sizeof(ainfo));

    jvar = zobj->jobj;
    assert(jvar != NULL);
    jatts = zobj->jatts;

    /* Verify the format */
    {
	int format;
	NCJcheck(NCJdictget(jvar,"zarr_format",&jvalue));
	sscanf(NCJstring(jvalue),ZARR_FORMAT_VERSION_TEMPLATE,&format);
	if(format != zinfo->zarr.zarr_format) {stat = (THROW(NC_ENCZARR)); goto done;}
    }

    /* Set the type and endianness of the variable */
    {
	int endianness;
	if((stat = NCJdictget(jvar,"dtype",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	/* Convert dtype to nc_type + endianness */
	if((stat = NCZF_dtype2nctype(file,NCJstring(jvalue),NC_NAT,&vtype,&endianness,&vtypelen))) goto done;
	if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
	    /* Locate the NC_TYPE_INFO_T object */
	    if((stat = ncz_gettype(file,var->container,vtype,&var->type_info)))
		goto done;
	} else {stat = NC_EBADTYPE; goto done;}
#if 0 /* leave native in place */
	if(endianness == NC_ENDIAN_NATIVE)
	    endianness = zinfo->native_endianness;
	if(endianness == NC_ENDIAN_NATIVE)
	    endianness = (NCZ_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
	if(endianness == NC_ENDIAN_LITTLE || endianness == NC_ENDIAN_BIG) {
	    var->endianness = endianness;
	} else {stat = NC_EBADTYPE; goto done;}
#else
	var->endianness = endianness;
#endif
	var->type_info->endianness = var->endianness; /* Propagate */
	if(vtype == NC_STRING) {
	    zvar->maxstrlen = vtypelen;
	    vtypelen = sizeof(char*); /* in-memory len */
	    if(zvar->maxstrlen <= 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
	}
    }

    if(!purezarr) {
	if(jatts == NULL) {stat = NC_ENCZARR; goto done;}
	/* Extract the _NCZARR_ARRAY values */
	/* Do this first so we know about storage esp. scalar */
	/* Extract the NCZ_V2_ARRAY dict */
	if((stat = NCZ_getnczarrkey(file,jsonz,NCZ_ARRAY,&jncvar))) goto done;
	if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
	assert((NCJsort(jncvar) == NCJ_DICT));
	/* Extract scalar flag */
	if((stat = NCJdictget(jncvar,"scalar",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL) {
	    var->storage = NC_CHUNKED;
	    zvar->scalar = 1;
	}
	/* Extract storage flag */
	if((stat = NCJdictget(jncvar,"storage",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL)
	    var->storage = NC_CHUNKED;
	/* Extract dimrefs list	 */
	if((stat = NCZ_dictgetalt2(jncvar,&jdimrefs,"dimension_references","dimrefs"))) goto done;
	if(jdimrefs != NULL) { /* Extract the dimref names */
	    assert((NCJsort(jdimrefs) == NCJ_ARRAY));
	    if(zvar->scalar) {
		assert(NCJarraylength(jdimrefs) == 1);
	    } else {
		zarr_rank = NCJarraylength(jdimrefs);
		for(j=0;j<zarr_rank;j++) {
		    const NCjson* dimpath = NCJith(jdimrefs,j);
		    assert(NCJsort(dimpath) == NCJ_STRING);
		    nclistpush(dimnames,strdup(NCJstring(dimpath)));
		}
	    }
	    jdimrefs = NULL; /* avoid double free */
	} /* else  simulate it from the shape of the variable */
    }

    /* Capture dimension_separator (must precede chunk cache creation) */
    {
	NCglobalstate* ngs = NC_getglobalstate();
	assert(ngs != NULL);
	zvar->dimension_separator = 0;
	if((stat = NCJdictget(jvar,"dimension_separator",&jvalue))<0) {stat = NC_EINVAL; goto done;}
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
	NCJcheck(NCJdictget(jvar,"fill_value",&jvalue));
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

    /* shape */
    {
	if((stat = NCJdictget(jvar,"shape",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(NCJsort(jvalue) != NCJ_ARRAY) {stat = (THROW(NC_ENCZARR)); goto done;}
	
	/* Process the rank */
	zarr_rank = NCJarraylength(jvalue);
	if(zarr_rank == 0) {
	    /* suppress variable */
	    ZLOG(NCLOGWARN,"Empty shape for variable %s suppressed",var->hdr.name);
	    suppress = 1;
	    goto suppressvar;
	}

	if(zvar->scalar) {
	    netcdf_rank = 0;
	    zarr_rank = 1; /* Zarr does not support scalars */
	} else 
	    netcdf_rank = (zarr_rank = NCJarraylength(jvalue));

	if(zarr_rank > 0) {
	    /* Save the rank of the variable */
	    if((stat = nc4_var_set_ndims(var, netcdf_rank))) goto done;
	    /* extract the shapes */
	    if((shapes = (size64_t*)malloc(sizeof(size64_t)*(size_t)zarr_rank)) == NULL)
		{stat = (THROW(NC_ENOMEM)); goto done;}
	    if((stat = decodeints(jvalue, shapes))) goto done;
	}
    }

    /* chunks */
    {
	size64_t chunks[NC_MAX_VAR_DIMS];
	if((stat = NCJdictget(jvar,"chunks",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_ARRAY)
	    {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Verify the rank */
	if(zvar->scalar || zarr_rank == 0) {
	    if(var->ndims != 0)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    zvar->chunkproduct = 1;
	    zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	    /* Create the cache */
	    if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
	} else {/* !zvar->scalar */
	    if(zarr_rank == 0) {stat = NC_ENCZARR; goto done;}
	    var->storage = NC_CHUNKED;
	    if(var->ndims != netcdf_rank)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    if((var->chunksizes = malloc(sizeof(size_t)*(size_t)zarr_rank)) == NULL)
		{stat = NC_ENOMEM; goto done;}
	    if((stat = decodeints(jvalue, chunks))) goto done;
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
    }
    /* Capture row vs column major; currently, column major not used*/
    {
	if((stat = NCJdictget(jvar,"order",&jvalue))<0) {stat = NC_EINVAL; goto done;}
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
	chainindex = 0; /* track location of filter in the chain */
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"filters",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_NULL) {
	    int k;
	    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
	    for(k=0;;k++) {
		jfilter = NULL;
		jfilter = NCJith(jvalue,k);
		if(jfilter == NULL) break; /* done */
		if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
		if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
	    }
	}
    }
#endif

    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    { 
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"compressor",&jfilter))<0) {stat = NC_EINVAL; goto done;}
	if(jfilter != NULL && NCJsort(jfilter) != NCJ_NULL) {
	    if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
	    if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
	}
    }
    /* Suppress variable if there are filters and var is not fixed-size */
    if(varsized && nclistlength((NClist*)var->filters) > 0)
	suppress = 1;
#endif

    if(zarr_rank > 0) {
	/* Convert dimrefs to specific dimensions */
	if((stat = computedimrefs(file, var, netcdf_rank, dimnames, shapes, var->dim))) goto done;
	if(!zvar->scalar) {
	    /* Extract the dimids */
	    for(j=0;j<netcdf_rank;j++)
		var->dimids[j] = var->dim[j]->hdr.id;
	}
    }

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if(!suppress) {
	/* At this point, we can finalize the filters */
	if((stat = NCZ_filter_setup(var))) goto done;
    }
#endif

suppressvar:
    if(suppress) {
	/* Reclaim NCZarr variable specific info */
	(void)NCZ_zclose_var1(var);
	/* Remove from list of variables and reclaim the top level var object */
	(void)nc4_var_list_del(grp, var);
	var = NULL;
    }

done:
    NCZ_clearAttrInfo(&ainfo);
    nclistfreeall(dimnames); dimnames = NULL;
    nullfree(varpath); varpath = NULL;
    nullfree(shapes); shapes = NULL;
    nullfree(key); key = NULL;
    return THROW(stat);
}

int
ZF2_decode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const struct ZOBJ* zobj)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_decode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jncvar, const NCjson* jatts, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_upload_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jvar, NCjson** jatts)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_upload_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jvar, NCjson** jatts)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/*Write JSON Metadata*/
int
ZF2_encode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jatts, NCjson* jtypes, NCjson** jzgroupp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jsuper, const NCjson* jzgroup, NCjson** jatts, NCjson** jgroupp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson* jtypes, NCjson** jzvarp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jzvar, NCjson** jatts, NClist* jfilters, NCjson** jvarp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_encode_attributes(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jncvar, NCjson** jattsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/*Filter Processing*/
int
ZF2_encode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCZ_Filter* filter, NCjson** jfilterp)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

int
ZF2_decode_filter(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jfilter, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
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
    return THROW(stat);
}

/*Chunkkeys*/

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

/**************************************************
/* Support Functions */

static int
decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims, NClist* dimdefs)
{
    int stat = NC_NOERR;
    size_t i;
    struct DimInfo* dimdef = NULL;

    assert(NCJsort(jdims) == NCJ_DICT);
    for(i=0;i<NCJdictlength(jdims);i++) {
	size64_t len;
	int unlim;
	const NCjson* jname= NCJdictkey(jdims,i);
	const NCjson* jdim = NCJdictvalue(jdims,i);
	struct NCJconst cvt;

        memset(&cvt,0,sizeof(cvt));
	if((dimdef = (struct DimInfo*)calloc(1,sizeof(struct DimInfo)))==NULL) goto done;

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
	    NCJcheck(NCJdictget(jdim,"size",&jsize));
	    NCJcheck(NCJdictget(jdim,"unlimited",&junlim));
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

static int
hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
{
    int stat = NC_NOERR;

    /* Convert the HDF5 id + visible parameters to the codec form */

    /* Clear any previous codec */
    nullfree(filter->codec.id); filter->codec.id = NULL;
    nullfree(filter->codec.codec); filter->codec.codec = NULL;
    filter->codec.id = strdup(filter->plugin->codec.codec->codecid);
    if(filter->plugin->codec.codec->NCZ_hdf5_to_codec) {
        stat = filter->plugin->codec.codec->NCZ_hdf5_to_codec(NCplistzarrv2,filter->hdf5.id,filter->hdf5.visible.nparams,filter->hdf5.visible.params,&filter->codec.codec);
#ifdef DEBUGF
        fprintf(stderr,">>> DEBUGF: NCZ_hdf5_to_codec: visible=%s codec=%s\n",printnczparams(filter->hdf5.visible),filter->codec.codec);
#endif
        if(stat) goto done;
    } else
        {stat = NC_EFILTER; goto done;}

done:
    return THROW(stat);
}

static int
codec2hdf(const NC_FILE_INFO_T* file, NCZ_Filter* filter)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_HDF5 hdf5;

    memset(&hdf5,0,sizeof(hdf5));

    /* Save the hdf5 id */
	hdf5.id = plugin->codec.codec->hdf5id;
	/* Convert the codec to hdf5 form visible parameters */
        if(plugin->codec.codec->NCZ_codec_to_hdf5) {
            stat = plugin->codec.codec->NCZ_codec_to_hdf5(pzarrcodec.codec,&hdf5.visible.nparams,&hdf5.visible.params);
#ifdef DEBUGF
	    fprintf(stderr,">>> DEBUGF: NCZ_codec_to_hdf5: codec=%s, hdf5=%s\n",printcodec(codec),printhdf5(hdf5));
#endif
	    if(stat) goto done;
	}


done:
    return THROW(stat);
}


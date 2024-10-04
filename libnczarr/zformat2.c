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

/* Forward */
static int decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims, NClist* dimdefs);
static int parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create);
static int locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp);
static int decodeints(const NCjson* jshape, size64_t* shapes);
static int computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int netcdf_rank, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);

/**************************************************/
/* Functions to build json from nc4internal structures and upload */

static int
ZF2_encode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jgroup = NULL;
    char version[64];
    NCJnew(NCJ_DICT,&jgroup);
    snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_format);
    NCJcheck(NCJinsertstring(jgroup,"zarr_format",version));
    if(jgroupp) {*jgroupp = jgroup; jgroup = NULL;}
done:
    NCJreclaim(jgroup);
    return THROW(stat);
}

static int
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

/* Currently the same for V2 and V3 */
static int
ZF2_encode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* dims, NCjson** jdimsp)
{
    int stat = NC_NOERR;
    size_t i;
    NCjson* jdims = NULL;
    NCjson* jdimargs = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJnew(NCJ_DICT,&jdims);

    for(i=0; i<ncindexsize(grp->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);

        NCJnew(NCJ_DICT,&jdimargs);

        NCJcheck(NCJinsertint(jdimargs,"size", (long long)dim->len));
        NCJcheck(NCJinsertint(jdimargs,"unlimited", 1));

        NCJinsert(jdims,dim->hdr.name,jdimargs);
	jdimargs = NULL;
    }

    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
done:
    NCJreclaim(jdims);
    NCJreclaim(jdimargs);
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_encode_grp_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* vars, NCjson** jvarsp)
{
    int stat = NC_NOERR;
    size_t i;
    NCjson* jvars = NULL;
    /* Create vars list */
    NCJnew(NCJ_ARRAY,&jvars);
    for(i=0; i<ncindexsize(grp->vars); i++) {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	NCJcheck(NCJaddstring(jvars,NCJ_STRING,var->hdr.name));
    }
    if(jvarsp) {*jvarsp = jvars; jvars = NULL;}
done:
    NCJreclaim(jvars);
    return THROW(stat);
}

static int
ZF2_encode_grp_subgroups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* subgrps, NCjson** jsubgrpsp)
{
    int stat = NC_NOERR;
    size_t i;
    NCjson* jsubgrps = NULL;
    /* Create subgroups list */
    NCJnew(NCJ_ARRAY,&jsubgrps);
    for(i=0; i<ncindexsize(grp->children); i++) {
        NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	NCJcheck(NCJaddstring(jsubgrps,NCJ_STRING,g->hdr.name));
    }
    if(jsubgrpsp) {*jsubgrpsp = jsubgrps; jsubgrps = NULL;}
done:
    NCJreclaim(jsubgrps);
    return THROW(stat);
}

/*
Consumes jdims, jvars, and jsubgrps.
*/
static int
ZF2_encode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp)
{
    int stat = NC_NOERR;
    NCjson* jnczgrp = NULL;

    /* Create the NCZ_GROUP dict */
    NCJnew(NCJ_DICT,&jnczgrp);
    /* Insert the various dicts and arrays */
    NCJcheck(NCJinsert(jnczgrp,"dimensions",jdims));
    jdims = NULL; /* avoid memory problems */
    NCJcheck(NCJinsert(jnczgrp,"arrays",jvars));
    jvars = NULL; /* avoid memory problems */
    NCJcheck(NCJinsert(jnczgrp,"groups",jsubgrps));
    jsubgrps = NULL; /* avoid memory problems */
    if(jnczgrpp) {*jnczgrpp = jnczgrp; jnczgrp = NULL;}
done:
    NCJreclaim(jnczgrp);
    return THROW(stat);
}

static int
ZF2_encode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jattsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    size_t i;
    NCindex* atts = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jdata = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* tname = NULL;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	atts = var->att;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
	atts = grp->att;
    }
    
    NCJnew(NCJ_DICT,&jatts);
    NCJnew(NCJ_DICT,&jtypes);

    if(ncindexsize(atts) > 0) {
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(atts);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(atts,i);
	    size_t typesize = 0;
	    char* talias = NULL;
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE) {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
	        typesize = (size_t)NCZ_get_maxstrlen(container);
	    else
	        {if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;

	    /* Collect the corresponding dtype */
            if((stat = NCZF_nctype2dtype(file,a->nc_typeid,endianness,typesize,&tname,&talias))) goto done;

	    /* Insert the attribute; consumes jdata*/
	    if((stat = ncz_insert_attr(jatts,jtypes,a->hdr.name,jdata,tname))) goto done;
	    jdata = NULL;

	    /* cleanup */
            nullfree(tname); tname = NULL;
        }
    }
    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jtypesp) {*jtypesp = jtypes; jtypes = NULL;}

done:
#if XXX
    NCJreclaim(jdata);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
#endif
    return THROW(stat);
}

static int
ZF2_encode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvarp)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    NCjson* jvar = NULL;
    NCjson* jshape = NULL;
    NCjson* jchunks = NULL;
    NCjson* jfill = NULL;
    NCjson* jcompressor = NULL;
    NCjson* jfilters = NULL;
    NCjson* jfilter = NULL;
    NClist* filterchain = NULL;
    size64_t shape[NC_MAX_VAR_DIMS];
    char number[1024];
    size_t i;
    
    NCJnew(NCJ_DICT,&jvar);

    /* Collect the shape vector */
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	shape[i] = dim->len;
    }
    /* but might be scalar */
    if(var->ndims == 0) shape[0] = 1;

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    /* Create the list */
    NCJnew(NCJ_ARRAY,&jshape);
    if(zvar->scalar) {
	NCJaddstring(jshape,NCJ_INT,"1");
    } else for(i=0;i<var->ndims;i++) {
	snprintf(number,sizeof(number),"%llu",shape[i]);
	NCJaddstring(jshape,NCJ_INT,number);
    }
    NCJcheck(NCJinsert(jvar,"shape",jshape));
    jshape = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    {	/* Add the type name */
	int endianness = var->type_info->endianness;
	int atomictype = var->type_info->hdr.id;
	char* dtypename = NULL;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = NCZF_nctype2dtype(file,atomictype,endianness,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename,NULL))) goto done;
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
    } else for(i=0;i<var->ndims;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(number,sizeof(number),"%lld",len);
	NCJaddstring(jchunks,NCJ_INT,number);
    }
    NCJcheck(NCJinsert(jvar,"chunks",jchunks));
    jchunks = NULL;

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
    NCJcheck(NCJinsert(jvar,"fill_value",jfill));
    jfill = NULL;

    /* order key */
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */
    NCJcheck(NCJinsertstring(jvar,"order","C"));

    /* Compressor and Filters */
    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"compressor"));
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    filterchain = (NClist*)var->filters;
    if(nclistlength(filterchain) > 0) {
	struct NCZ_Filter* compressor = (struct NCZ_Filter*)nclistget(filterchain,nclistlength(filterchain)-1);
        /* encode up the compressor */
        if((stat = NCZ_filter_jsonize(file,var,compressor,&jcompressor))) goto done;
    } else
#endif
    { /* no filters at all; default to null */
        NCJnew(NCJ_NULL,&jcompressor);
    }
    NCJcheck(NCJinsert(jvar,"compressor",jcompressor));
    jcompressor = NULL;

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
    NCJcheck(NCJaddstring(jvar,NCJ_STRING,"filters"));
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if(nclistlength(filterchain) > 1) {
	size_t k;
	/* jfilters holds the array of filters */
	NCJnew(NCJ_ARRAY,&jfilters);
	for(k=0;k<nclistlength(filterchain)-1;k++) {
 	    struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,k);
	    /* encode up the filter as a string */
	    if((stat = NCZ_filter_jsonize(file,var,filter,&jfilter))) goto done;
	    NCJcheck(NCJappend(jfilters,jfilter));
	    jfilter = NULL;
	}
    } else
#endif
    /* no filters at all */
    NCJnew(NCJ_NULL,&jfilters);

    NCJcheck(NCJinsert(jvar,"filters",jfilters));
    jfilters = NULL;

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
    NCJreclaim(jfilter);
    return THROW(stat);
}

static int
ZF2_encode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
{
    int stat = NC_NOERR;
    NClist* dimrefs = NULL;
    NCjson* jnczarray = NULL;
    NCjson* jdimrefs = NULL;
    char* dimpath = NULL;
    size_t i;

    NCJnew(NCJ_DICT,&jnczarray);

    if(var->ndims > 0) {
        if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_dimkey(dim,&dimpath))) goto done;
	    nclistpush(dimrefs,dimpath);
	    dimpath = NULL;
	}
    }

    /* Create the dimrefs json object */
    NCJnew(NCJ_ARRAY,&jdimrefs);
    for(i=0;i<nclistlength(dimrefs);i++) {
	const char* dim = nclistget(dimrefs,i);
	NCJaddstring(jdimrefs,NCJ_STRING,dim);
    }

    /* Insert dimension_references  */
    NCJcheck(NCJinsert(jnczarray,"dimension_references",jdimrefs));
    jdimrefs = NULL; /* Avoid memory problems */

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


static int
ZF2_upload_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jgroup, const NCjson* jatts)
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
    if((stat=NCZ_uploadjson(zinfo->map,key,jgroup))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nullfree(fullpath);
    nullfree(key);
    return THROW(stat);
}

static int
ZF2_upload_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jvar, const NCjson* jatts)
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
    if((stat=NCZ_uploadjson(zinfo->map,key,jvar))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nullfree(fullpath);
    nullfree(key);
    return THROW(stat);
}

/**************************************************/
/* Functions to import download and decode json to nc4internal structures */

static int
ZF2_decode_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jgroup, NCjson* jatts, NCjson** jsuperp, NClist* dims, NClist* vars, NClist* subgrps)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_UNUSED(zinfo);
    return THROW(stat);
}

static int
ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_UNUSED(zinfo);
    return THROW(stat);
}

static int
ZF2_decode_grp_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname, NC_VAR_INFO_T** jvarp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_UNUSED(zinfo);
    return THROW(stat);
}

static int
ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const struct ZOBJ* jsonz, NClist* dimdefs, NClist* vars, NClist* subgrps)
{
    int stat = NC_NOERR;
    size_t i;
    const NCjson* jvalue = NULL;
    const NCjson* jgroup = jsonz->jobj;
    struct DimInfo* dimdef = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    if((stat=NCZ_dictgetalt2(jsonz->jobj,&jvalue,"dimensions","dims"))) goto done;
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

done:
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_decode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, const NCjson** jtypesp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_VAR_INFO_T* zvar = NULL;
    size_t i;
    const NCjson* jtypes = NULL;
    const NCjson* jnczatts = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    nc_type typehint = NC_NAT;
    struct NCZ_AttrInfo ainfo;
    NC_ATT_INFO_T* fillvalueatt = NULL;

    memset(&ainfo,0,sizeof(ainfo));

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
        zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }

    /* Get _nczarr_attr */
    NCJcheck(NCJdictget(jatts,NCZ_ATTR,&jnczatts));
    if(jnczatts != NULL) {
	/* Get types */
	NCJcheck(NCJdictget(jnczatts,"types",&jtypes));
    }

    /* Iterate over the attributes to create the in-memory attributes */
    /* Watch for special cases: _FillValue and  _ARRAY_DIMENSIONS (xarray), etc. */
    for(i=0;i<NCJdictlength(jatts);i++) {
	const NCjson* key = NCJdictkey(jatts,i);
        const NCjson* value = NCJdictvalue(jatts,i);
	const NC_reservedatt* ra = NULL;
	int isfillvalue = 0;
    	int isdfaltmaxstrlen = 0;
       	int ismaxstrlen = 0;
	const char* aname = NCJstring(key);
	NC_ATT_INFO_T* newatt = NULL;

        /* See if this is a notable attribute */
	if(var != NULL && strcmp(aname,NC_ATT_FILLVALUE)==0) isfillvalue = 1;
	if(grp != NULL && grp->parent == NULL && strcmp(aname,NC_NCZARR_DEFAULT_MAXSTRLEN_ATTR)==0)
	    isdfaltmaxstrlen = 1;
	if(var != NULL && strcmp(aname,NC_NCZARR_MAXSTRLEN_ATTR)==0)
	    ismaxstrlen = 1;
        /* See if this is reserved attribute */
	ra = NC_findreserved(aname);
	if(ra != NULL) {
	    /* case 1: name = _NCProperties, grp=root, varid==NC_GLOBAL */
	    if(strcmp(aname,NCPROPS)==0 && grp != NULL && file->root_grp == grp) {
	        /* Setup provenance */
		if(NCJsort(value) != NCJ_STRING)
		    {stat = (THROW(NC_ENCZARR)); goto done;} /*malformed*/
		if((stat = NCZ_read_provenance(file,aname,NCJstring(value)))) goto done;
	    }
	    /* case 2: name = _ARRAY_DIMENSIONS, sort==NCVAR, flags & HIDDENATTRFLAG */
	    if(strcmp(aname,NC_XARRAY_DIMS)==0 && var != NULL && (ra->flags & HIDDENATTRFLAG)) {
  	        /* store for later */
		size_t i;
		assert(NCJsort(value) == NCJ_ARRAY);
		if((zvar->dimension_names = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
		for(i=0;i<NCJarraylength(value);i++) {
		    const NCjson* k = NCJith(value,i);
		    assert(k != NULL && NCJsort(k) == NCJ_STRING);
		    nclistpush(zvar->dimension_names,strdup(NCJstring(k)));
		}
	    }
	    /* case other: if attribute is hidden */
	    if(ra->flags & HIDDENATTRFLAG) continue; /* ignore it */
	}
        typehint = NC_NAT;
	if(isfillvalue)
	    typehint = var->type_info->hdr.id ; /* if unknown use the var's type for _FillValue */
	/* Create the attribute */
	/* Collect the attribute's type and value  */
	ainfo.name = aname;
	ainfo.jtypes = jtypes;
	ainfo.typehint = typehint;
	ainfo.jdata = value;
	if((stat = NCZ_computeattrinfo(file,&ainfo))) goto done;
	if((stat = ncz_makeattr(file,container,ainfo.name,ainfo.nctype,ainfo.datalen,ainfo.data,&newatt))) goto done;
	/* No longer need this copy of the data */
   	if((stat = NC_reclaim_data_all(file->controller,ainfo.nctype,ainfo.data,ainfo.datalen))) goto done;	    	    
	if(isfillvalue) fillvalueatt = newatt;
	if(ismaxstrlen && newatt->nc_typeid == NC_INT)
	    zvar->maxstrlen = ((size_t*)newatt->data)[0];
	if(isdfaltmaxstrlen && newatt->nc_typeid == NC_INT)
	    zinfo->default_maxstrlen = ((size_t*)newatt->data)[0];
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue(file,(NC_VAR_INFO_T*)container))) goto done;
    }

    if(jtypesp) {*jtypesp = ainfo.jtypes; ainfo.jtypes = NULL;}

done:
    NCZ_clearAttrInfo(&ainfo);
    return THROW(stat);
}

static int
ZF2_decode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* jsonz)
{
    int stat = NC_NOERR;
    size_t j;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int purezarr = 0;
    int xarray = 0;
    /* per-variable info */
    NCZ_VAR_INFO_T* zvar = NULL;
    const NCjson* jvar = NULL;
    const NCjson* jatts = NULL; /* corresponding to jvar */
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

    jvar = jsonz->jobj;
    assert(jvar != NULL);
    jatts = jsonz->jatts;

    /* Verify the format */
    {
	int format;
	NCJcheck(NCJdictget(jvar,"zarr_format",&jvalue));
	sscanf(NCJstring(jvalue),"%d",&format);
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

static int
ZF2_decode_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_download_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* fullpath = NULL;
    char* key = NULL;

    /* Download .zgroup and .zattrs */
    if((stat = NCZ_grpkey(grp,fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2GROUP,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&jsonz->jobj))) goto done;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&jsonz->jatts))) goto done;
    jsonz->constatts = 0;    

done:
    nullfree(key);
    nullfree(fullpath);
    return THROW(stat);
}

static int
ZF2_download_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZOBJ* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* norm_name = NULL;
    char* fullpath = NULL;
    char* key = NULL;

    /* Download .zgroup and .zattrs */
    if((stat = NCZ_varkey(var,fullpath))) goto done;
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&jsonz->jobj))) goto done;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat = NCZ_downloadjson(zinfo->map,key,&jsonz->jatts))) goto done;
    jsonz->constatts = 0;    

done:
    nullfree(norm_name);
    nullfree(key);
    nullfree(fullpath);
    return THROW(stat);
}

/**************************************************/
/* Misc Dispatch Functions */

static int
ZF2_dtype2nctype(const NC_FILE_INFO_T* file, const char* dtype, nc_type typehint, nc_type* nctypep, int* endianp, size_t* typelenp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_nctype2dtype(const NC_FILE_INFO_T* file, nc_type nctype, int endianness, size_t typelen, char** dtypep, char** daliasp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_hdf2codec(const NC_FILE_INFO_T* file, const NC_VAR_INFO_T* var, NCZ_Filter* filter)
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
ZF2_codec2hdf(const NC_FILE_INFO_T* file, NCZ_Filter* filter)
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

static int
ZF2_buildchunkkey(const NC_FILE_INFO_T* file, size_t netcdf_rank, const size64_t* chunkindices, char dimsep, char** keyp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_reclaim_json_atts(const NC_FILE_INFO_T* file, const NCjson* jatts)
{
    NCJreclaim((NCjson*)jatts);
    return THROW(NC_NOERR);
}

static int
ZF2_searchvars(const NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)zfile->format_file_info;
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
	if((stat = nczm_concat(varkey,ZARRAY,&zarray))) goto done;
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

static int
ZF2_searchsubgrps(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    char* grpkey = NULL;
    char* subkey = NULL;
    char* zgroup = NULL;
    NClist* matches = nclistnew();

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_search(zfile->map,grpkey,matches))) goto done;
    for(i=0;i<nclistlength(matches);i++) {
	const char* name = nclistget(matches,i);
	if(name[0] == NCZM_DOT) continue; /* zarr/nczarr specific */
	/* See if name/.zgroup exists */
	if((stat = nczm_concat(grpkey,name,&subkey))) goto done;
	if((stat = nczm_concat(subkey,ZGROUP,&zgroup))) goto done;
	if((stat = nczmap_exists(zfile->map,zgroup)) == NC_NOERR)
	    nclistpush(subgrpnames,strdup(name));
	stat = NC_NOERR;
	nullfree(subkey); subkey = NULL;
	nullfree(zgroup); zgroup = NULL;
    }

done:
    nullfree(grpkey);
    nullfree(subkey);
    nullfree(zgroup);
    nclistfreeall(matches);
    return stat;
}

/**************************************************/
/* Utilities */

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
	char norm_name[NC_MAX_NAME+1];

        memset(&cvt,0,sizeof(cvt));
	if((dimdef = (struct DimInfo*)calloc(1,sizeof(struct DimInfo)))==NULL) goto done;

	/* Verify name legality */
	if((stat = nc4_check_name(NCJstring(jname), norm_name))) {stat = NC_EBADNAME; goto done;}
	dimdef->name = strdup(norm_name);

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

/**
Insert _nczarr_attr into .zattrs
Take control of jtypes
@param jatts
@param jtypes
*/
static int
insert_nczarr_attr(NCjson* jatts, NCjson* jtypes)
{
    NCjson* jdict = NULL;
    if(jatts != NULL && jtypes != NULL) {

{	NCJinsertstring(jtypes,NCZ_V2_ATTR,"|J0"); /* type for _nczarr_attr */
        NCJnew(NCJ_DICT,&jdict);
        NCJinsert(jdict,"types",jtypes);
        NCJinsert(jatts,NCZ_V2_ATTR,jdict);
        jdict = NULL;
    }
    return NC_NOERR;
}

/* Compute the set of dim refs for this variable, taking purezarr and xarray into account */
static int
computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int netcdf_rank, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int stat = NC_NOERR;
    size_t i;
    int createdims = 0; /* 1 => we need to create the dims in root if they do not already exist */
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
    NCjson* jatts = NULL;
    int purezarr = 0;
    int xarray = 0;

    ZTRACE(3,"file=%s var=%s purezarr=%d xarray=%d ndims=%d shape=%s",
    	file->controller->path,var->hdr.name,purezarr,xarray,(int)ndims,nczprint_vector(ndims,shapes));
    assert(zfile && zvar);

    if(zfile->flags & FLAG_PUREZARR) purezarr = 1;
    if(zfile->flags & FLAG_XARRAYDIMS) xarray = 1;

    if(purezarr && xarray) {/* Read in the attributes to get xarray dimdef attribute; Note that it might not exist */
	/* Note that if xarray && !purezarr, then xarray will be superceded by the nczarr dimensions key */
        char zdimname[4096];
        if(zvar->dimension_names != NULL) {
	    assert(nclistlength(dimnames) == 0);
	    if((stat = ncz_read_atts(file,(NC_OBJ*)var,jatts))) goto done;
	}
	if(zvar->dimension_names != NULL) {
	    /* convert xarray to the dimnames */
	    for(i=0;i<nclistlength(zvar->dimension_names);i++) {
	        snprintf(zdimname,sizeof(zdimname),"/%s",(const char*)nclistget(zvar->dimension_names,i));
	        nclistpush(dimnames,strdup(zdimname));
	    }
	}
	createdims = 1; /* may need to create them */
    }

    /* If pure zarr and we have no dimref names, then fake it */
    if(purezarr && nclistlength(dimnames) == 0) {
	int i;
	createdims = 1;
        for(i=0;i<ndims;i++) {
	    /* Compute the set of absolute paths to dimrefs */
            char zdimname[4096];
	    snprintf(zdimname,sizeof(zdimname),"/%s_%llu",NCDIMANON,shapes[i]);
	    nclistpush(dimnames,strdup(zdimname));
	}
    }

    /* Now, use dimnames to get the dims; create if necessary */
    if((stat = parsedimrefs(file,dimnames,shapes,dims,createdims)))
        goto done;

done:
    NCJreclaim(jatts);
    return ZUNTRACE(THROW(stat));
}

static int
parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* segments = NULL;

    for(i=0;i<nclistlength(dimnames);i++) {
	NC_GRP_INFO_T* g = NULL;
	NC_DIM_INFO_T* d = NULL;
	size_t j;
	const char* dimpath = nclistget(dimnames,i);
	const char* dimname = NULL;

	/* Locate the corresponding NC_DIM_INFO_T* object */
	nclistfreeall(segments);
	segments = nclistnew();
	if((stat = ncz_splitkey(dimpath,segments))) goto done;
	if((stat=locategroup(file,nclistlength(segments)-1,segments,&g))) goto done;
	/* Lookup the dimension */
	dimname = nclistget(segments,nclistlength(segments)-1);
	d = NULL;
	dims[i] = NULL;
	for(j=0;j<ncindexsize(g->dim);j++) {
	    d = (NC_DIM_INFO_T*)ncindexith(g->dim,j);
	    if(strcmp(d->hdr.name,dimname)==0) {
		dims[i] = d;
		break;
	    }
	}
	if(dims[i] == NULL && create) {
	    /* If not found and create then create it */
	    if((stat = createdim(file, dimname, shape[i], &dims[i])))
	        goto done;
	} else {
	    /* Verify consistency */
	    if(dims[i]->len != shape[i])
	        {stat = NC_EDIMSIZE; goto done;}
	}
	assert(dims[i] != NULL);
    }
done:
    nclistfreeall(segments);
    return THROW(stat);
}

/*
Given a list of segments, find corresponding group.
*/
static int
locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp)
{
    size_t i, j;
    int found, stat = NC_NOERR;
    NC_GRP_INFO_T* grp = NULL;

    grp = file->root_grp;
    for(i=0;i<nsegs;i++) {
	const char* segment = nclistget(segments,i);
	char norm_name[NC_MAX_NAME];
	found = 0;
	if((stat = nc4_check_name(segment,norm_name))) goto done;
	for(j=0;j<ncindexsize(grp->children);j++) {
	    NC_GRP_INFO_T* subgrp = (NC_GRP_INFO_T*)ncindexith(grp->children,j);
	    if(strcmp(subgrp->hdr.name,norm_name)==0) {
		grp = subgrp;
		found = 1;
		break;
	    }
	}
	if(!found) {stat = NC_ENOGRP; goto done;}
    }
    /* grp should be group of interest */
    if(grpp) *grpp = grp;

done:
    return THROW(stat);
}

/* This code is a subset of NCZ_def_dim */
static int
createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp)
{
    int stat = NC_NOERR;
    NC_GRP_INFO_T* root = file->root_grp;
    NC_DIM_INFO_T* thed = NULL;
    if((stat = nc4_dim_list_add(root, name, (size_t)dimlen, -1, &thed)))
        goto done;
    assert(thed != NULL);
    /* Create struct for NCZ-specific dim info. */
    if (!(thed->format_dim_info = calloc(1, sizeof(NCZ_DIM_INFO_T))))
	{stat = NC_ENOMEM; goto done;}
    ((NCZ_DIM_INFO_T*)thed->format_dim_info)->common.file = file;
    *dimp = thed; thed = NULL;
done:
    return stat;
}

/* Convert a list of integer strings to 64 bit dimension sizes (shapes) */
static int
decodeints(const NCjson* jshape, size64_t* shapes)
{
    int stat = NC_NOERR;
    size_t i;

    for(i=0;i<NCJarraylength(jshape);i++) {
	struct ZCVT zcvt;
	nc_type typeid = NC_NAT;
	NCjson* jv = NCJith(jshape,i);
	if((stat = NCZ_json2cvt(jv,&zcvt,&typeid))) goto done;
	switch (typeid) {
	case NC_INT64:
	if(zcvt.int64v < 0) {stat = (THROW(NC_ENCZARR)); goto done;}
	    shapes[i] = (size64_t)zcvt.int64v;
	    break;
	case NC_UINT64:
	    shapes[i] = (size64_t)zcvt.uint64v;
	    break;
	default: {stat = (THROW(NC_ENCZARR)); goto done;}
	}
    }

done:
    return THROW(stat);
}

/**************************************************/
/* Format dispatch table */

static const NCZ_Formatter NCZ_FORMATTER2_table = {
    NCZARRFORMAT2,
    ZARRFORMAT2,
    NCZ_FORMATTER_VERSION,

    ZF2_create,
    ZF2_open,
    ZF2_close,

    /* Convert NetCDF4 Internal object to JSON */
    ZF2_encode_group,
    ZF2_encode_superblock,
    ZF2_encode_grp_dims,
    ZF2_encode_grp_vars,
    ZF2_encode_grp_subgroups,
    ZF2_encode_nczarr_group,
    ZF2_encode_attributes_json,
    ZF2_encode_var_json,
    ZF2_encode_nczarr_array,

    /* Write JSON to storage */
    ZF2_upload_grp_json,
    ZF2_upload_var_json,

    /* Convert JSON to NetCDF4 Internal objects */
    ZF2_decode_group,
    ZF2_decode_superblock,
    ZF2_decode_grp_dims,
    ZF2_decode_grp_vars,
    ZF2_decode_grp_subgroups,
    ZF2_decode_nczarr_group,
    ZF2_decode_attributes_json,
    ZF2_decode_var_json,
    ZF2_decode_nczarr_array,

    /* Read JSON to storage */
    ZF2_download_grp_json,
    ZF2_download_var_json,

    /* Type conversion actions */
    ZF2_dtype2nctype,
    ZF2_nctype2dtype,

    /* Misc. Actions */
    ZF2_hdf2codec,
    ZF2_buildchunkkey,
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


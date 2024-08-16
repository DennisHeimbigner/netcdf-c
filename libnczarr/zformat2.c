/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 *
 * @author Dennis Heimbigner
 */

#include "zincludes.h"
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
#include "netcdf_filter_build.h"
#endif

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
	jdimsize = NULL;
        NCJcheck(NCJinsertint(jdimargs,"unlimited", 1));

        NCJinsert(jdims,dim->hdr.name,jdimargs);
	jdimargs = NULL;
    }

    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
    NCJreclaim(jdims);
    NCJreclaim(jdimsize);
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
    NCJreclaim(jnczgrp);
    return THROW(stat);
}

static int
ZF2_encode_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jattsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCindex* atts = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jdict = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    char* content = NULL;
    char* dimpath = NULL;
    int isxarray = 0;
    int inrootgroup = 0;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* tname = NULL;
    int purezarr = 0;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL) inrootgroup = 1;
	atts = var->att;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
	atts = grp->att;
    }
    
    if(zinfo->flags & FLAG_PUREZARR) purezarr = 1;
    if(zinfo->flags & FLAG_XARRAYDIMS) isxarray = 1;

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
    size64_t shape[NC_MAX_VAR_DIMS];
    char number[1024];
    
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
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = NCZF_nctype2dtype(file,atomictype,endianness,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename,&talias))) goto done;
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
    if(zvar->dimension_separator != DFALT_DIM_SEPARATOR) {
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

    /* Insert dimension_referencess  */
    NCJcheck(NCJinsert(jnczarray,"dimension_references",jdimrefs);
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
    nclistfreeal(dimrefs);
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
    if((stat = nczm_concat(fullpath,ZGROUP,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,jgroup))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,ZATTRS,&key))) goto done;
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
    if((stat = nczm_concat(fullpath,ZARRAY,&key))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(zinfo->map,key,jvar))) goto done;
    nullfree(key); key = NULL;

    /* build ZATTRS path */
    if((stat = nczm_concat(fullpath,ZATTRS,&key))) goto done;
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
done:
    return THROW(stat);
}

static int
ZF2_decode_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_decode_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jdims)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;

    assert(NCJsort(jdims) == NCJ_DICT);
    for(i=0;i<NCJdictlength(jdims);i++) {
	size64_t len;
	int unlim;
	const NCjson* jkey = NCJdictkey(jdims,i);
	const NCjson* jdim = NCJdictvalue(jdims,i);
	struct NCJconst cvt = NCJconst_empty;

	if(NCJisatomic(jdim)) {
	    NCJcheck(NCJcvt(jdim,NCJ_INT,&cvt));	    
	    len = (size64_t)cvt.ival;
	    unlim = 0;
	} else {
	    const NCjson* jsize = NULL;
            const NCjson* junlim = NULL;
	    assert(NCJsort(jdim) == NCJ_DICT);
	    NCJcheck(NCJdictget(jdim,"size",&jsize));
	    NCJcheck(NCJdictget(jdim,"unlimited",&junlim));
	    NCJcheck(NCJcvt(jsize,NCJ_INT,&cvt));
	    len = (size64_t)cvt.ival;
	    cvt = NCJconst_empty;
	    NCJcheck(NCJcvt(junlim,NCJ_INT,&cvt));
	    unlim = (ival == 0 ? 0 : 1);
	}
	if((stat = ncz4_create_dim(file,grp,NCJstring(jkey),len,unlim,NULL))) goto done;
    }    

done:
    return THROW(stat);
}

static int
ZF2_decode_grp_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname, NC_VAR_INFO_T** jvarp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_decode_grp_subgroup(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* subgrpname, NC_GRP_INFO_T** subgrpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
done:
    return THROW(stat);
}

static int
ZF2_decode_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const struct ZJSON* jsonz, NClist* vars, NClist* subgrpp);
{
    int stat = NC_NOERR;
    size_t i;
    const NCjson* jvalue = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    if((stat=dictgetalt(jgroup,"dimensions","dims",&jvalue))) goto done;
    if(jvalue != NULL) {
	if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Extract the dimensions defined in this group */
	for(i=0;i<NCJlength(jvalue);i+=2) {
	    const NCjson* jname = NCJith(jvalue,i);
	    const NCjson* jleninfo = NCJith(jvalue,i+1);
    	    const NCjson* jtmp = NULL;
       	    const char* slen = "0";
       	    const char* sunlim = "0";
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    /* check the length */
            if(NCJsort(jleninfo) == NCJ_DICT) {
		if((stat = NCJdictget(jleninfo,"size",&jtmp))<0) {stat = NC_EINVAL; goto done;}
		if(jtmp== NULL)
		    {stat = NC_EBADNAME; goto done;}
		slen = NCJstring(jtmp);
		/* See if unlimited */
		if((stat = NCJdictget(jleninfo,"unlimited",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	        if(jtmp == NULL) sunlim = "0"; else sunlim = NCJstring(jtmp);
            } else if(jleninfo != NULL && NCJsort(jleninfo) == NCJ_INT) {
		slen = NCJstring(jleninfo);		
	    } else
		{stat = NC_ENCZARR; goto done;}
	    /* The dimdefs list is a set of triples of the form (name,len,isunlim) */
	    nclistpush(dimdefs,strdup(norm_name));
	    nclistpush(dimdefs,strdup(slen));
    	    nclistpush(dimdefs,strdup(sunlim));
	}
    }

    if((stat=dictgetalt(jgroup,"arrays","vars",&jvalue))) goto done;
    if(jvalue != NULL) {
	/* Extract the variable names in this group */
	for(i=0;i<NCJlength(jvalue);i++) {
	    NCjson* jname = NCJith(jvalue,i);
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    nclistpush(varnames,strdup(norm_name));
	}
    }

    if((stat = NCJdictget(jgroup,"groups",&jvalue))<0) {stat = NC_EINVAL; goto done;}
    if(jvalue != NULL) {
	/* Extract the subgroup names in this group */
	for(i=0;i<NCJlength(jvalue);i++) {
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
    size_t i;
    const NCjson* jtypes = NULL;
    const NCjson* jnczatts = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NCindex* atts = NULL;

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	atts = var->att;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
	atts = grp->att;
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
	NCjson* key = NCJdictkey(jatts,i);
        NCjson* value = NCJdictvalue(jatts,i);
	const NC_reservedatt* ra = NULL;
	int isfillvalue = 0;
    	int isdfaltmaxstrlen = 0;
       	int ismaxstrlen = 0;
	const char* aname = NCJstring(key);
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
	if((stat = computeattrinfo(file,aname,jtypes,typehint,purezarr,value, &typeid,&typelen,&len,&data))) goto done;
	if((stat = ncz_makeattr(file,container,aname,typeid,len,data,&att))) goto done;
	/* No longer need this copy of the data */
   	if((stat = NC_reclaim_data_all(file->controller,att->nc_typeid,data,len))) goto done;	    	    
	data = NULL;
	if(isfillvalue) fillvalueatt = att;
	if(ismaxstrlen && att->nc_typeid == NC_INT)
	    zvar->maxstrlen = ((size_t*)att->data)[0];
	if(isdfaltmaxstrlen && att->nc_typeid == NC_INT)
	    zinfo->default_maxstrlen = ((size_t*)att->data)[0];
    }
    if(jtypesp) jtypesp = jtypes;

done:
    return THROW(stat);
}

static int
ZF2_decode_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZJSON* jsonz)
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
    int vtypelen = 0;
    size_t netcdf_rank = 0;  /* true rank => scalar => 0 */
    size_t zarr_rank = 0; /* |shape| */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    const NCjson* jfilter = NULL;
    int chainindex = 0;
#endif

    jvar = jsonz->obj;
    assert(jvar != NULL);
    jatts = jsonz->atts;

    /* Verify the format */
    {
	int version;
	NCJcheck(NCJdictget(jvar,"zarr_format",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	sscanf(NCJstring(jvalue),"%d",&version);
	if(version != zinfo->zarr.zarr_version) {stat = (THROW(NC_ENCZARR)); goto done;}
    }

    /* Set the type and endianness of the variable */
    {
	int endianness;
	if((stat = NCJdictget(jvar,"dtype",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	/* Convert dtype to nc_type + endianness */
	if((stat = ncz_dtype2nctype(NCJstring(jvalue),NC_NAT,purezarr,&vtype,&endianness,&vtypelen)))
	    goto done;
	if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
	    /* Locate the NC_TYPE_INFO_T object */
	    if((stat = ncz_gettype(file,grp,vtype,&var->type_info)))
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
	if((stat = getnczarrkey((NC_OBJ*)var,NCZ_V2_ARRAY,&jncvar))) goto done;
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
	if((stat = dictgetalt(jncvar,"dimension_references","dimrefs",&jdimrefs))) goto done;
	if(jdimrefs != NULL) { /* Extract the dimref names */
	    assert((NCJsort(jdimrefs) == NCJ_ARRAY));
	    if(zvar->scalar) {
		assert(NCJlength(jdimrefs) == 1);
	    } else {
		rank = NCJlength(jdimrefs);
		for(j=0;j<rank;j++) {
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
	if((stat = NCJdictget(jvar,"fill_value",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue == NULL || NCJsort(jvalue) == NCJ_NULL)
	    var->no_fill = 1;
	else {
	    size_t fvlen;
	    nc_type atypeid = vtype;
	    var->no_fill = 0;
	    if((stat = computeattrdata(var->type_info->hdr.id, &atypeid, jvalue, NULL, &fvlen, &var->fill_value)))
		goto done;
	    assert(atypeid == vtype);
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
	zarr_rank = NCJlength(jvalue);
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
	    netcdf_rank = (zarr_rank = NCJlength(jvalue));

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
    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* Do filters key before compressor key so final filter chain is in correct order */
    {
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if(zvar->incompletefilters == NULL) zvar->incompletefilters = (void*)nclistnew();
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
#endif
    }

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
#if XXX
   nclistfreeall(dimnames); dimnames = NULL;
    nullfree(varpath); varpath = NULL;
    nullfree(shapes); shapes = NULL;
    nullfree(key); key = NULL;
#endif
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
ZF2_create_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* gname, NC_GRP_INFO_T** grpp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_GRP_INFO_T* grp = NULL;

    if((stat=ncz4_create_grp(file,parent,gname,&grp)) goto done;
    if(grpp) *grpp = grp;
done:
    return THROW(stat);
}

static int
ZF2_download_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZJSON* jsonz)
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
ZF2_create_var(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* varname, NC_VAR_INFO_T** varp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NC_VAR_INFO_T* var = NULL;

    if((stat=ncz4_create_var(file,parent,varname,&var)) goto done;
    if(varp) *varp = var;
done:
    return THROW(stat);
}

static int
ZF2_download_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, struct ZJSON* jsonz)
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
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
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


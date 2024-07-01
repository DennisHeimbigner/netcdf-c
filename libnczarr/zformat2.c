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

static int
ZF2_build_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jgroupp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jgroup = NULL;
    char version[64];
    NCJnew(NCJ_DICT,&jgroup);
    snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
    NCJcheck(NCJinsertstring(jgroup,"zarr_format",version));
    if(jgroupp) {*jgroupp = jgroup; jgroup = NULL;}
done:
    NCJreclaim(jgroup);
    return THROW(stat);
}

static int
ZF2_build_superblock(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NCjson** jsuperp)
{
    int stat = NC_NOERR;
    char version[64];
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jsuper = NULLl
    /* create superblock */
    snprintf(version,sizeof(version),"%lu.%lu.%lu",
		 zinfo->zarr.nczarr_version.major,
		 zinfo->zarr.nczarr_version.minor,
		 zinfo->zarr.nczarr_version.release);
    NCJnew(NCJ_DICT,&jsuper);
    NCJcheck(NCJinsertstring(jsuper,"version",version));
    if(jsuperp) {*jsuperp = jsuper; jsuper = NULL;}
done:
    NCJreclaim(jsuper);
    return THROW(stat);
}

static int
ZF2_build_grp_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* dims, NCjson** jdimsp)
{
    int stat = NC_NOERR;
    size_t i;
    NCjson* jdims = NULL;
    NCjson* jdimsize = NULL;
    NCjson* jdimargs = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJnew(NCJ_DICT,&jdims);
    for(i=0; i<ncindexsize(grp->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
	char slen[128];

        snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
	NCJnewstring(NCJ_INT,slen,&jdimsize);

        NCJnew(NCJ_DICT,&jdimargs);
        NCJcheck(NCJinsert(jdimargs,"size", jdimsize));
	jdimsize = NULL;
        NCJcheck(NCJinsertint(jdimargs,"unlimited", 1));

        NCJinsert(jdims,dim->hdr.name,jdimargs);
	jdimargs = NULL;
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
done:
    NCJreclaim(jdims);
    NCJreclaim(jdimsize);
    NCJreclaim(jdimargs);
    return ZUNTRACE(THROW(stat));
}

static int
ZF2_build_grp_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* vars, NCjson** jvarsp)
{
    int stat = NC_NOERR;
    NCjson* jvars = NULL;
    /* Create vars list */
    NCJnew(NCJ_ARRAY,&jvars);
    for(i=0; i<ncindexsize(grp->vars); i++) {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	NCJcheck(NCJappendstring(jvars,NCJ_STRING,var->hdr.name));
    }
    if(jvarsp) {*jvarsp = jvars; jvars = NULL;}
done:
    NCJreclaim(jvars);
    return THROW(stat);
}

static int
ZF2_build_grp_subgroups(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NClist* subgrps, NCjson** jsubgrpsp)
{
    int stat = NC_NOERR;
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
ZF2_build_nczarr_group(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims, NCjson* jvars, NCjson* jsubgrps,  NCjson** jnczgrpp)
{
    int stat = NC_NOERR;
    NCjson* jnczgroup = NULL;

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
ZF2_build_attributes_json(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson** jattsp, NCjson** jtypesp)
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

    NC_UNUSED(isclose);
    
    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL) inrootgroup = 1;
	atts = var->att;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
	atts = grp->att;
    }
    
    if(zinfo->controls.flags & FLAG_PUREZARR) purezarr = 1;
    if(zinfo->controls.flags & FLAG_XARRAYDIMS) isxarray = 1;

    NCJnew(NCJ_DICT,&jatts);
    NCJnew(NCJ_DICT,&jtypes);

    if(ncindexsize(atts) > 0) {
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(atts);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(atts,i);
	    size_t typesize = 0;
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE) {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
	        typesize = (size_t)NCZ_get_maxstrlen(container);
	    else
	        {if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;

	    /* Collect the corresponding dtype */
            if((stat = ncz_nctype2dtype(file,a->nc_typeid,endianness,purezarr,typesize,&tname))) goto done;

	    /* Insert the attribute; consumes jdata*/
	    if((stat = insert_attr(jatts,jtypes,a->hdr.name,jdata,tname))) goto done;
	    jdata = NULL;

	    /* cleanup */
            nullfree(tname); tname = NULL;
        }
    }
    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jtypesp) {*jtypesp = jtypes; jtypes = NULL;}

done:
    NCJreclaim(jdata);
    NCJreclaim(jatts);
    NCKreclaim(jtypes);
    return THROW(stat);
}

static int
ZF2_build_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jvarp)
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
	if((stat = ncz_nctype2dtype(atomictype,endianness,purezarr,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename))) goto done;
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
    if((stat = NCJaddstring(jvar,NCJ_STRING,"compressor"))<0) {stat = NC_EINVAL; goto done;}
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
    if((stat = NCJaddstring(jvar,NCJ_STRING,"filters"))<0) {stat = NC_EINVAL; goto done;}
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
ZF2_build_nczarr_array(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, NCjson** jnczarrayp)
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
ZF2_write_grp_json(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const NCjson* jgroup, const NCjson* jatts)
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
ZF2_write_var_json(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const NCjson* jvar, const NCjson* jatts)
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
	NCJinsertstring(jtypes,NCZ_V2_ATTR,"|J0"); /* type for _nczarr_attr */
        NCJnew(NCJ_DICT,&jdict);
        NCJinsert(jdict,"types",jtypes);
        NCJinsert(jatts,NCZ_V2_ATTR,jdict);
        jdict = NULL;
    }
    return NC_NOERR;
}


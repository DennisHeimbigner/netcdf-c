/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#ifdef ENABLE_NCZARR_FILTERS
#include "zfilter.h"
#endif

/**************************************************/

/*Forward*/
static int ZF3_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF3_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map);
static int ZF3_writemeta(NC_FILE_INFO_T* file);
static int ZF3_readmeta(NC_FILE_INFO_T* file);
static int ZF3_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container);
static int ZF3_close(NC_FILE_INFO_T* file);

static int write_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp);
static int write_var_meta(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);
static int write_var(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var);
static int write_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_OBJ* container, NCindex* attlist);

static int read_superblock(NC_FILE_INFO_T* file, int* nczarrvp);
static int read_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_GRP_INFO_T* grp);
static int read_dims(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* diminfo);
static int read_vars(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* varnames);
static int read_subgrps(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* subgrpnames);

static int NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp);
static int NCZ_parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps);
static int NCZ_parse_group_content_pure(NCZ_FILE_INFO_T*  zinfo, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps);
static int NCZ_read_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_OBJ* container);
static int NCZ_searchvars(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames);
static int NCZ_searchsubgrps(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* subgrpnames);
static int NCZ_decodeints(NCjson* jshape, size64_t* shapes);
static int NCZ_computeattrinfo(const char* name, NClist* atypes, nc_type typehint, int purezarr, NCjson* values, nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap);
static int NCZ_computeattrdata(nc_type typehint, nc_type* typeidp, NCjson* values, size_t* typelenp, size_t* countp, void** datap);
static int NCZ_computedimrefs(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zinfo, NCZMAP* map, NC_VAR_INFO_T* var, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);
static int NCZ_load_jatts(NCZMAP* map, NC_OBJ* container, NCjson** jattrsp, NClist** atypesp);
static int NCZ_json_convention_read(NCjson* json, NCjson** jtextp);
static int NCZ_attr_convert(NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst);
static int NCZ_parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create);
static int NCZ_jtypes2atypes(NCjson* jtypes, NClist* atypes);
static int NCZ_charify(NCjson* src, NCbytes* buf);
static int NCZ_locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int NCZ_createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp);

/**************************************************/

/**
 * @internal Synchronize file metadata from internal to map.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ZF3_create(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

static int
ZF3_open(NC_FILE_INFO_T* file, NCURI* uri, NCZMAP* map)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;

    ZTRACE(4,"file=%s",file->controller->path);
    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Internal->Map */

/**
 * @internal Synchronize file metadata from internal => map.
 * Disabled for V1.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error
 *         ::NC_EXXX errors
 * @author Dennis Heimbigner
 */
static int
ZF3_writemeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    ZTRACE(4,"file=%s",file->controller->path);

    zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    assert(zfile != NULL);
    map = zfile->map;
    assert(map != NULL);

    /* Write out root group recursively */
    if((stat = write_grp(file, zfile, map, file->root_grp)))
        goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Recursively synchronize group from memory to map.
 *
 * @param file Pointer to file struct
 * @param zfile
 * @param map
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp)
{
    int i,stat = NC_NOERR;
    char version[1024];
    int purezarr = 0;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrps = NULL;
    NCjson* jsuper = NULL;
    NCjson* jtmp = NULL;

    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;

    /* Construct grp key */
    if((stat = NCZ_grpkey(grp,&fullpath)))
	goto done;

    if(!purezarr) {
        /* Create dimensions dict */
        if((stat = NCZ_collect_dims(file,grp,&jdims))) goto done;

        /* Create vars list */
        if((stat = NCJnew(NCJ_ARRAY,&jvars)))
	    goto done;
        for(i=0; i<ncindexsize(grp->vars); i++) {
	    NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	    if((stat = NCJaddstring(jvars,NCJ_STRING,var->hdr.name))) goto done;
        }

        /* Create subgroups list */
        if((stat = NCJnew(NCJ_ARRAY,&jsubgrps)))
    	    goto done;
        for(i=0; i<ncindexsize(grp->children); i++) {
	    NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	    if((stat = NCJaddstring(jsubgrps,NCJ_STRING,g->hdr.name))) goto done;
        }

        /* Create the "_nczarr_group" dict */
        if((stat = NCJnew(NCJ_DICT,&json)))
	    goto done;
        /* Insert the various dicts and arrays */
        if((stat = NCJinsert(json,"dims",jdims))) goto done;
        jdims = NULL; /* avoid memory problems */
        if((stat = NCJinsert(json,"vars",jvars))) goto done;
        jvars = NULL; /* avoid memory problems */
        if((stat = NCJinsert(json,"groups",jsubgrps))) goto done;
        jsubgrps = NULL; /* avoid memory problems */
    }

    /* build ZGROUP contents */
    if((stat = NCJnew(NCJ_DICT,&jgroup)))
	goto done;
    snprintf(version,sizeof(version),"%d",zfile->zarr.zarr_format);
    if((stat = NCJaddstring(jgroup,NCJ_STRING,"zarr_format"))) goto done;
    if((stat = NCJaddstring(jgroup,NCJ_INT,version))) goto done;
    if(!purezarr && grp->parent == NULL) { /* Root group */
	/* This is for back compatibility */
        snprintf(version,sizeof(version),"%u.0.0",(unsigned) zfile->zarr.nczarr_format);
	if((stat = NCJnew(NCJ_DICT,&jsuper))) goto done;
	if((stat=NCJnewstring(NCJ_STRING,version,&jtmp))) goto done;
	if((stat = NCJinsert(jsuper,"version",jtmp))) goto done;
	jtmp = NULL;
        snprintf(version,sizeof(version),"%u", (unsigned)zfile->zarr.nczarr_format);
	if((stat=NCJnewstring(NCJ_INT,version,&jtmp))) goto done;
	if((stat = NCJinsert(jsuper,"format",jtmp))) goto done;
	jtmp = NULL;
	if((stat = NCJinsert(jgroup,NCZ_V2_SUPERBLOCK,jsuper))) goto done;
	jsuper = NULL;
    }

    if(!purezarr) {
        /* Insert the "_NCZARR_GROUP" dict */
        if((stat = NCJinsert(jgroup,NCZ_V2_GROUP,json))) goto done;
        json = NULL;
    }

    /* build Z2GROUP path */
    if((stat = nczm_concat(fullpath,Z2GROUP,&key)))
	goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jgroup)))
	goto done;
    nullfree(key); key = NULL;

    /* Build the Z2ATTRS object */
    assert(grp->att);
    if((stat = write_atts(file,zfile,map,(NC_OBJ*)grp, grp->att)))
	goto done;

    /* Now write all the variables */
    for(i=0; i<ncindexsize(grp->vars); i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	if((stat = write_var(file,zfile,map,var))) goto done;
    }

    /* Now recurse to synchronize all the subgrps */
    for(i=0; i<ncindexsize(grp->children); i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = write_grp(file,zfile,map,g))) goto done;
    }

done:
    NCJreclaim(jtmp);
    NCJreclaim(jsuper);
    NCJreclaim(json);
    NCJreclaim(jgroup);
    NCJreclaim(jdims);
    NCJreclaim(jvars);
    NCJreclaim(jsubgrps);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_var_meta(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var)
{
    int i,stat = NC_NOERR;
     char number[1024];
     char* fullpath = NULL;
    char* key = NULL;
    char* dimpath = NULL;
    NClist* dimrefs = NULL;
    NCjson* jvar = NULL;
    NCjson* jncvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jtmp = NULL;
    NCjson* jfill = NULL;
    char* dtypename = NULL;
    int purezarr = 0;
    size64_t shape[NC_MAX_VAR_DIMS];
    NCZ_VAR_INFO_T* zvar = var->format_var_info;
#ifdef ENABLE_NCZARR_FILTERS
    NClist* filterchain = NULL;
    NCjson* jfilter = NULL;
#endif

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    zfile = file->format_file_info;
    map = zfile->map;

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;

    /* Make sure that everything is established */
    /* ensure the fill value */
    if((stat = NCZ_ensure_fill_value(var))) goto done; /* ensure var->fill_value is set */
    assert(var->no_fill || var->fill_value != NULL);
    /* ensure the chunk cache */
    if((stat = NCZ_adjust_var_cache(var))) goto done;
    /* rebuild the fill chunk */
    if((stat = NCZ_ensure_fill_chunk(zvar->cache))) goto done;
#ifdef ENABLE_NCZARR_FILTERS
    /* Build the filter working parameters for any filters */
    if((stat = NCZ_filter_setup(var))) goto done;
#endif

    /* Construct var path */
    if((stat = NCZ_varkey(var,&fullpath)))
	goto done;

    /* Create the .zarray json object */
    if((stat = NCJnew(NCJ_DICT,&jvar)))
	goto done;

    /* zarr_format key */
    snprintf(number,sizeof(number),"%d",zfile->zarr.zarr_format);
    if((stat = NCJaddstring(jvar,NCJ_STRING,"zarr_format"))) goto done;
    if((stat = NCJaddstring(jvar,NCJ_INT,number))) goto done;

    /* Collect the shape vector */
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	shape[i] = dim->len;
    }
    /* but might be scalar */
    if(var->ndims == 0)
        shape[0] = 1;

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    /* Create the list */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp))) goto done;
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1");
    } else for(i=0;i<var->ndims;i++) {
	snprintf(number,sizeof(number),"%llu",shape[i]);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    if((stat = NCJinsert(jvar,"shape",jtmp))) goto done;
    jtmp = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"dtype"))) goto done;
    {	/* Add the type name */
	int endianness = var->type_info->endianness;
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = ncz_nctype2dtype(atomictype,endianness,purezarr,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename))) goto done;
	if((stat = NCJaddstring(jvar,NCJ_STRING,dtypename))) goto done;
	nullfree(dtypename); dtypename = NULL;
    }

    /* chunks key */
    /* The zarr format does not support the concept
       of contiguous (or compact), so it will never appear in the read case.
    */
    /* list of chunk sizes */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"chunks"))) goto done;
    /* Create the list */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp))) goto done;
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1"); /* one chunk of size 1 */
    } else for(i=0;i<var->ndims;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(number,sizeof(number),"%lld",len);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    if((stat = NCJappend(jvar,jtmp))) goto done;
    jtmp = NULL;

    /* fill_value key */
    if(var->no_fill) {
	if((stat=NCJnew(NCJ_NULL,&jfill))) goto done;
    } else {/*!var->no_fill*/
	int atomictype = var->type_info->hdr.id;
        if(var->fill_value == NULL) {
	     if((stat = NCZ_ensure_fill_value(var))) goto done;
	}
        /* Convert var->fill_value to a string */
	if((stat = NCZ_stringconvert(atomictype,1,var->fill_value,&jfill))) goto done;
	assert(jfill->sort != NCJ_ARRAY);
    }
    if((stat = NCJinsert(jvar,"fill_value",jfill))) goto done;
    jfill = NULL;

    /* order key */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"order"))) goto done;
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"C"))) goto done;

    /* Compressor and Filters */
    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"compressor"))) goto done;
#ifdef ENABLE_NCZARR_FILTERS
    filterchain = (NClist*)var->filters;
    if(nclistlength(filterchain) > 0) {
	struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,nclistlength(filterchain)-1);
        /* encode up the compressor */
        if((stat = NCZ_filter_jsonize(file,var,filter,&jtmp))) goto done;
    } else
#endif
    { /* no filters at all */
        /* Default to null */
        if((stat = NCJnew(NCJ_NULL,&jtmp))) goto done;
    }
    if(jtmp && (stat = NCJappend(jvar,jtmp))) goto done;
    jtmp = NULL;

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"filters"))) goto done;
#ifdef ENABLE_NCZARR_FILTERS
    if(nclistlength(filterchain) > 1) {
	int k;
	/* jtmp holds the array of filters */
	if((stat = NCJnew(NCJ_ARRAY,&jtmp))) goto done;
	for(k=0;k<nclistlength(filterchain)-1;k++) {
 	    struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,k);
	    /* encode up the filter as a string */
	    if((stat = NCZ_filter_jsonize(file,var,filter,&jfilter))) goto done;
	    if((stat = NCJappend(jtmp,jfilter))) goto done;
	}
    } else
#endif
    { /* no filters at all */
        if((stat = NCJnew(NCJ_NULL,&jtmp))) goto done;
    }
    if((stat = NCJappend(jvar,jtmp))) goto done;
    jtmp = NULL;

    /* dimension_separator key */
    /* Single char defining the separator in chunk keys */
    if(zvar->dimension_separator != DFALT_DIM_SEPARATOR) {
	char sep[2];
	sep[0] = zvar->dimension_separator;/* make separator a string*/
	sep[1] = '\0';
        if((stat = NCJnewstring(NCJ_STRING,sep,&jtmp))) goto done;
        if((stat = NCJinsert(jvar,"dimension_separator",jtmp))) goto done;
        jtmp = NULL;
    }

    /* Capture dimref names as FQNs */
    if(var->ndims > 0) {
        if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_dimkey(dim,&dimpath))) goto done;
	    nclistpush(dimrefs,dimpath);
	    dimpath = NULL;
	}
    }

    /* Build the NCZ_V2_ARRAY dict entry */
    {
	/* Create the dimrefs json object */
	if((stat = NCJnew(NCJ_ARRAY,&jdimrefs)))
	    goto done;
	for(i=0;i<nclistlength(dimrefs);i++) {
	    const char* dim = nclistget(dimrefs,i);
	    NCJaddstring(jdimrefs,NCJ_STRING,dim);
	}
	if((stat = NCJnew(NCJ_DICT,&jncvar)))
	    goto done;

	/* Insert dimrefs  */
	if((stat = NCJinsert(jncvar,"dimrefs",jdimrefs)))
	    goto done;
	jdimrefs = NULL; /* Avoid memory problems */

	/* Add the _Storage flag */
	/* Record if this is a scalar */
	if(var->ndims == 0) {
	    if((stat = NCJnewstring(NCJ_INT,"1",&jtmp)))goto done;
	    if((stat = NCJinsert(jncvar,"scalar",jtmp))) goto done;
	    jtmp = NULL;
	}
	/* everything looks like it is chunked */
	if((stat = NCJnewstring(NCJ_STRING,"chunked",&jtmp)))goto done;
	if((stat = NCJinsert(jncvar,"storage",jtmp))) goto done;
	jtmp = NULL;

	if(!purezarr) {
	    if((stat = NCJinsert(jvar,NCZ_V2_ARRAY,jncvar))) goto done;
	    jncvar = NULL;
	}
    }

    /* build .zarray path */
    if((stat = nczm_concat(fullpath,Z2ARRAY,&key)))
	goto done;

    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jvar)))
	goto done;
    nullfree(key); key = NULL;

    var->created = 1;

    /* Build .zattrs object */
    assert(var->att);
    if((stat = write_atts(file,zfile,map,(NC_OBJ*)var, var->att)))
	goto done;

done:
    nclistfreeall(dimrefs);
    nullfree(fullpath);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jncvar);
    NCJreclaim(jtmp);
    NCJreclaim(jfill);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data and data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_var(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    if((stat = write_var_meta(file,zfile,map,var))) goto done;

#if 0
    /* flush only chunks that have been written */
    if(zvar->cache) {
        if((stat = NCZ_flush_chunk_cache(zvar->cache)))
	    goto done;
    }
#endif

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize attribute data from memory to map.
 *
 * @param container Pointer to grp|var struct containing the attributes
 * @param key the name of the map entry
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
write_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_OBJ* container, NCindex* attlist)
{
    int i,stat = NC_NOERR;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCjson* jtype = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jdict = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    char* content = NULL;
    char* dimpath = NULL;
    int isxarray = 0;
    int purezarr = 0;
    int inrootgroup = 0;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* tname = NULL;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    ZTRACE(3,"file=%s container=%s |attlist|=%u",file->controller->path,container->name,(unsigned)ncindexsize(attlist));
    
    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL)
	    inrootgroup = 1;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;

    if(zfile->controls.flags & FLAG_XARRAYDIMS) isxarray = 1;

    /* Create the attribute dictionary */
    if((stat = NCJnew(NCJ_DICT,&jatts))) goto done;

    if(ncindexsize(attlist) > 0) {
        /* Create the jncattr.types object */
        if((stat = NCJnew(NCJ_DICT,&jtypes)))
	    goto done;
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(attlist);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	    size_t typesize = 0;
#if 0
	    const NC_reservedatt* ra = NC_findreserved(a->hdr.name);
	    /* If reserved and hidden, then ignore */
	    if(ra && (ra->flags & HIDDENATTRFLAG)) continue;
#endif
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE)
	        {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
	        typesize = NCZ_get_maxstrlen(container);
	    else
	        {if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;
	    if((stat = NCJinsert(jatts,a->hdr.name,jdata))) goto done;
	    jdata = NULL;

	    /* Collect the corresponding dtype */
	    {
	        if((stat = ncz_nctype2dtype(a->nc_typeid,endianness,purezarr,typesize,&tname))) goto done;
  	        if((stat = NCJnewstring(NCJ_STRING,tname,&jtype))) goto done;
	        nullfree(tname); tname = NULL;
	        if((stat = NCJinsert(jtypes,a->hdr.name,jtype))) goto done; /* add {name: type} */
	        jtype = NULL;
	    }
        }
    }

    /* Construct container path */
    if(container->sort == NCGRP)
	stat = NCZ_grpkey(grp,&fullpath);
    else
	stat = NCZ_varkey(var,&fullpath);
    if(stat)
	goto done;

    if(container->sort == NCVAR) { 
        if(inrootgroup && isxarray) {
	    int dimsinroot = 1;
	    /* Insert the XARRAY _ARRAY_ATTRIBUTE attribute */
	    if((stat = NCJnew(NCJ_ARRAY,&jdimrefs)))
	        goto done;
	    /* Fake the scalar case */
	    if(var->ndims == 0) {
	        NCJaddstring(jdimrefs,NCJ_STRING,XARRAYSCALAR);
	    } else /* Walk the dimensions and capture the names */
	    for(i=0;i<var->ndims;i++) {
	        NC_DIM_INFO_T* dim = var->dim[i];
		/* Verify that the dimension is in the root group */
		if(dim->container && dim->container->parent != NULL) {
		    dimsinroot = 0; /* dimension is not in root */
		    break;
		}
	    }
	    if(dimsinroot) {
		/* Walk the dimensions and capture the names */
		for(i=0;i<var->ndims;i++) {
		    char* dimname;
	            NC_DIM_INFO_T* dim = var->dim[i];
		    dimname = strdup(dim->hdr.name);
		    if(dimname == NULL) {stat = NC_ENOMEM; goto done;}
	            NCJaddstring(jdimrefs,NCJ_STRING,dimname);
   	            nullfree(dimname); dimname = NULL;
		}
	        /* Add the _ARRAY_DIMENSIONS attribute */
	        if((stat = NCJinsert(jatts,NC_XARRAY_DIMS,jdimrefs))) goto done;
	        jdimrefs = NULL;
	    }
        }
    }
    /* Add Quantize Attribute */
    if(container->sort == NCVAR && var && var->quantize_mode > 0) {    
	char mode[64];
	snprintf(mode,sizeof(mode),"%d",var->nsd);
        if((stat = NCJnewstring(NCJ_INT,mode,&jint)))
	        goto done;
	/* Insert the quantize attribute */
	switch (var->quantize_mode) {
	case NC_QUANTIZE_BITGROOM:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITGROOM_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_GRANULARBR:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_GRANULARBR_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_BITROUND:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITROUND_ATT_NAME,jint))) goto done;	
	    jint = NULL;
	    break;
	default: break;
	}
    }

    if(NCJlength(jatts) > 0) {
        if(!purezarr) {
	    /* Insert the NCZ_V2_ATTR attribute */
            if((stat = NCJnew(NCJ_DICT,&jdict)))
                goto done;
	    if(jtypes != NULL)
                {if((stat = NCJinsert(jdict,"types",jtypes))) goto done;}
            jtypes = NULL;
	    if(jdict != NULL)
                {if((stat = NCJinsert(jatts,NCZ_V2_ATTR,jdict))) goto done;}
            jdict = NULL;
	}
        /* write .zattrs path */
        if((stat = nczm_concat(fullpath,Z2ATTRS,&key)))
            goto done;
        /* Write to map */
        if((stat=NCZ_uploadjson(map,key,jatts)))
            goto done;
        nullfree(key); key = NULL;
    }

done:
    nullfree(fullpath);
    nullfree(key);
    nullfree(content);
    nullfree(dimpath);
    nullfree(tname);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
    NCJreclaim(jtype);
    NCJreclaim(jdimrefs);
    NCJreclaim(jdict);
    NCJreclaim(jint);
    NCJreclaim(jdata);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Map->Internal */


/**
 * @internal Read file data from map to memory.
 *
 * @param file Pointer to file info struct.
 *
 * @return NC_NOERR If no error.
 *         NC_EXXX for error returns
 *
 * @author Dennis Heimbigner
 */
static int
ZF3_readmeta(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    int purezarr = 0;
    int  nczarr_format = 0;
    NCZ_FILE_INFO_T* zfile = NULL;
    NCZMAP* map = NULL;

    ZTRACE(3,"file=%s",file->controller->path);
    
    zfile = file->format_file_info;
    map = zfile->map;

    purezarr = (zfile->controls.flags & FLAG_PUREZARR);

    /* Ok, try to read superblock */
    switch(stat = read_superblock(file,&nczarr_format)) {
    case NC_NOERR: break;
    case NC_EEMPTY:
        if(!purezarr) {stat = NC_ENOTZARR; goto done;}
    default: goto done;
    }

    /* Now load the groups starting with root */
    if((stat = read_grp(file,zfile,map,file->root_grp)))
	goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
Used to support lazy attribute read.
@param file - [in] the containing file (annotation)
@param container - [in] the containing object (var or grp)
@return NC_NOERR|NC_EXXX

@author Dennis Heimbigner
*/
static int
ZF3_readattrs(NC_FILE_INFO_T* file, NC_OBJ* container)
{
    int stat = NC_NOERR;
    int i;
    char* fullpath = NULL;
    char* key = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NCZMAP* map = NULL;
    NC_ATT_INFO_T* att = NULL;
    NCindex* attlist = NULL;
    NCjson* jattrs = NULL;
    NClist* atypes = NULL;
    nc_type typeid;
    size_t len, typelen;
    void* data = NULL;
    NC_ATT_INFO_T* fillvalueatt = NULL;
    nc_type typehint = NC_NAT;
    int purezarr;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    zinfo = file->format_file_info;
    map = zinfo->map;

    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;
 
    if(container->sort == NCGRP) {	
	grp = ((NC_GRP_INFO_T*)container);
	attlist =  grp->att;
    } else {
	var = ((NC_VAR_INFO_T*)container);
        zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
	attlist =  var->att;
    }

    switch ((stat = NCZ_load_jatts(map, container, &jattrs, &atypes))) {
    case NC_NOERR: break;
    case NC_EEMPTY:  /* container has no attributes */
        stat = NC_NOERR;
	break;
    default: goto done; /* true error */
    }

    if(jattrs != NULL) {
	/* Iterate over the attributes to create the in-memory attributes */
	/* Watch for special cases: _FillValue and  _ARRAY_DIMENSIONS (xarray), etc. */
	for(i=0;i<NCJlength(jattrs);i+=2) {
	    NCjson* key = NCJith(jattrs,i);
	    NCjson* value = NCJith(jattrs,i+1);
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
		    if((stat = NCZ_read_provenance(file,aname,NCJstring(value))))
			goto done;
		}
		/* case 2: name = _ARRAY_DIMENSIONS, sort==NCVAR, flags & HIDDENATTRFLAG */
		if(strcmp(aname,NC_XARRAY_DIMS)==0 && var != NULL && (ra->flags & HIDDENATTRFLAG)) {
  	            /* store for later */
		    int i;
		    assert(NCJsort(value) == NCJ_ARRAY);
		    if((zvar->xarray = nclistnew())==NULL)
		        {stat = NC_ENOMEM; goto done;}
		    for(i=0;i<NCJlength(value);i++) {
			const NCjson* k = NCJith(value,i);
			assert(k != NULL && NCJsort(k) == NCJ_STRING);
			nclistpush(zvar->xarray,strdup(NCJstring(k)));
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
	    if((stat = NCZ_computeattrinfo(aname,atypes,typehint,purezarr,value,
				   &typeid,&typelen,&len,&data)))
		goto done;
	    if((stat = ncz_makeattr(container,attlist,aname,typeid,len,data,&att)))
		goto done;
	    /* No longer need this copy of the data */
   	    if((stat = NC_reclaim_data_all(file->controller,att->nc_typeid,data,len))) goto done;	    	    
	    data = NULL;
	    if(isfillvalue)
	        fillvalueatt = att;
	    if(ismaxstrlen && att->nc_typeid == NC_INT)
	        zvar->maxstrlen = ((int*)att->data)[0];
	    if(isdfaltmaxstrlen && att->nc_typeid == NC_INT)
	        zinfo->default_maxstrlen = ((int*)att->data)[0];
	}
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue((NC_VAR_INFO_T*)container)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
	((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
	((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    if(data != NULL)
        stat = NC_reclaim_data(file->controller,att->nc_typeid,data,len);
    NCJreclaim(jattrs);
    nclistfreeall(atypes);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

static int
ZF3_close(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/
/**
 * @internal Read superblock data from map to memory
 *
 * @param file Pointer to file struct
 * @param nczarrvp (out) the nczarr version
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_superblock(NC_FILE_INFO_T* file, int* nczarrvp)
{
    int stat = NC_NOERR;
    int nczarr_format = 0;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCjson* jblock = NULL;
    NCjson* jtmp = NULL;

    ZTRACE(3,"file=%s",file->controller->path);

    /* Get the zarr root group */
    switch(stat = NCZ_downloadjson(zfile->map, Z3METAROOT, &jblock)) {
    case NC_EEMPTY: /* not there */
	nczarr_format = NCZARRFORMAT0; /* apparently pure zarr */
	goto done;
    case NC_NOERR:
	if((stat = NCJdictget(jblock,"nczarr_format",&jtmp))) goto done;
	if(jtmp == NULL) {stat = NC_ENCZARR; goto done;}
	sscanf(NCJstring(jtmp),"%d",&nczarr_format);
	break;
    default: goto done;
    }
    if(nczarrvp) *nczarrvp = nczarr_format;
done:
    NCJreclaim(jblock);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Read group data from map to memory
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_grp(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    char* fullpath = NULL;
    char* key = NULL;
    int purezarr = 0;
    NCjson* json = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdict = NULL;
    NClist* dimdefs = nclistnew();
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);
    
    purezarr = (zfile->controls.flags & FLAG_PUREZARR);

    /* Construct grp path */
    if((stat = NCZ_grpkey(grp,&fullpath)))
	goto done;

    if(purezarr) {
	if((stat = NCZ_parse_group_content_pure(zfile,grp,varnames,subgrps)))
	    goto done;
    } else { /*!purezarr*/
        /* build NCZGROUP path */
	if((stat = nczm_concat(fullpath,NCZGROUP,&key))) goto done;
        /* Read */
	jdict = NULL;
	stat=NCZ_downloadjson(map,key,&jdict);
	nullfree(key); key = NULL;
	if(!jdict) {stat = NC_ENOTZARR; goto done;}
        /* Pull out lists about group content */
        if((stat = NCZ_parse_group_content(jdict,dimdefs,varnames,subgrps))) goto done;
	/* Define dimensions */
	if((stat = read_dims(file,zfile,map,grp,dimdefs))) goto done;
    }

    /* Define vars taking xarray into account */
    if((stat = read_vars(file,zfile,map,grp,varnames))) goto done;

    /* Read sub-groups */
    if((stat = read_subgrps(file,zfile,map,grp,subgrps))) goto done;

done:
    NCJreclaim(jdict);
    NCJreclaim(json);
    NCJreclaim(jgroup);
    nclistfreeall(dimdefs);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param diminfo List of (name,length,isunlimited) triples
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_dims(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* diminfo)
{
    int i,stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |diminfo|=%u",file->controller->path,grp->hdr.name,nclistlength(diminfo));

    /* Reify each dim in turn */
    for(i = 0; i < nclistlength(diminfo); i+=3) {
	NC_DIM_INFO_T* dim = NULL;
	size64_t len = 0;
	const char* name = nclistget(diminfo,i);
	const char* slen = nclistget(diminfo,i+1);

	/* Create the NC_DIM_INFO_T object */
	sscanf(slen,"%lld",&len); /* Get length */
	if((stat = nc4_dim_list_add(grp, name, (size_t)len, -1, &dim)))
	    goto done;
	if((dim->format_dim_info = calloc(1,sizeof(NCZ_DIM_INFO_T))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	((NCZ_DIM_INFO_T*)dim->format_dim_info)->common.file = file;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize vars into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varnames List of names of variables in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_vars(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    int i,j;
    char* varpath = NULL;
    char* key = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NCjson* jvar = NULL;
    NCjson* jncvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jvalue = NULL;
    int purezarr = 0;
    nc_type vtype;
    int vtypelen;
    size64_t* shapes = NULL;
    int rank = 0;
    int zarr_rank = 1; /* Need to watch out for scalars */
    NClist* dimnames = nclistnew();
#ifdef ENABLE_NCZARR_FILTERS
    NCjson* jfilter = NULL;
    int chainindex;
#endif

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    zfile = file->format_file_info;
    map = zfile->map;

    if(zfile->controls.flags & FLAG_PUREZARR) purezarr = 1;

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	const char* varname = nclistget(varnames,i);
	if((stat = nc4_var_list_add2(grp, varname, &var)))
	    goto done;

	/* And its annotation */
	if((zvar = calloc(1,sizeof(NCZ_VAR_INFO_T)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
	var->format_var_info = zvar;
	zvar->common.file = file;

        /* pretend it was created */
	var->created = 1;

	/* Indicate we do not have quantizer yet */
	var->quantize_mode = -1;

	/* Construct var path */
	if((stat = NCZ_varkey(var,&varpath)))
	    goto done;

	/* Construct the path to the zarray object */
	if((stat = nczm_concat(varpath,Z2ARRAY,&key)))
	    goto done;
	/* Download the zarray object */
	if((stat=NCZ_readdict(map,key,&jvar)))
	    goto done;
	nullfree(key); key = NULL;
	assert(NCJsort(jvar) == NCJ_DICT);

        /* Extract the .zarray info from jvar */

	/* Verify the format */
	{
	    int version;
	    if((stat = NCJdictget(jvar,"zarr_format",&jvalue))) goto done;
	    sscanf(NCJstring(jvalue),"%d",&version);
	    if(version != zfile->zarr.zarr_format)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	}
	/* Set the type and endianness of the variable */
	{
	    int endianness;
	    if((stat = NCJdictget(jvar,"dtype",&jvalue))) goto done;
	    /* Convert dtype to nc_type + endianness */
	    if((stat = ncz_dtype2nctype(NCJstring(jvalue),NC_NAT,purezarr,&vtype,&endianness,&vtypelen)))
		goto done;
	    if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
		/* Locate the NC_TYPE_INFO_T object */
		if((stat = ncz_gettype(file,grp,vtype,&var->type_info)))
		    goto done;
	    } else {stat = NC_EBADTYPE; goto done;}
	    var->endianness = endianness;
	    var->type_info->endianness = var->endianness; /* Propagate */
	    if(vtype == NC_STRING) {
		zvar->maxstrlen = vtypelen;
		vtypelen = sizeof(char*); /* in-memory len */
		if(zvar->maxstrlen <= 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
	    }
	}

	if(!purezarr) {
  	    /* Extract the NCZARRAY values */
	    /* Do this first so we know about storage esp. scalar */
	    /* Construct the path to the zarray object */
	    if((stat = nczm_concat(varpath,NCZARRAY,&key)))
		goto done;
	    /* Download the nczarray object */
	    if((stat=NCZ_readdict(map,key,&jncvar)))
		goto done;
	    nullfree(key); key = NULL;
	    if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
   	    assert((NCJsort(jncvar) == NCJ_DICT));
	    /* Extract scalar flag */
	    if((stat = NCJdictget(jncvar,"scalar",&jvalue)))
		goto done;
	    if(jvalue != NULL) {
	        var->storage = NC_CHUNKED;
		zvar->scalar = 1;
	    }
	    /* Extract storage flag */
	    if((stat = NCJdictget(jncvar,"storage",&jvalue)))
		goto done;
	    if(jvalue != NULL) {
		var->storage = NC_CHUNKED;
	    }
	    /* Extract dimrefs list  */
	    switch ((stat = NCJdictget(jncvar,"dimrefs",&jdimrefs))) {
	    case NC_NOERR: /* Extract the dimref names */
		assert((NCJsort(jdimrefs) == NCJ_ARRAY));
		if(zvar->scalar) {
	   	    assert(NCJlength(jdimrefs) == 0);		   
		} else {
		    rank = NCJlength(jdimrefs);
		    for(j=0;j<rank;j++) {
		        const NCjson* dimpath = NCJith(jdimrefs,j);
		        assert(NCJsort(dimpath) == NCJ_STRING);
		        nclistpush(dimnames,strdup(NCJstring(dimpath)));
		    }
		}
		jdimrefs = NULL; /* avoid double free */
		break;
	    case NC_EEMPTY: /* will simulate it from the shape of the variable */
		stat = NC_NOERR;
		break;
	    default: goto done;
	    }
	    jdimrefs = NULL;
	}

	/* shape */
	{
	    if((stat = NCJdictget(jvar,"shape",&jvalue))) goto done;
	    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = (THROW(NC_ENCZARR)); goto done;}
            if(zvar->scalar) {
	        rank = 0;
		zarr_rank = 1; /* Zarr does not support scalars */
	    } else 
		rank = (zarr_rank = NCJlength(jvalue));
	    /* Save the rank of the variable */
	    if((stat = nc4_var_set_ndims(var, rank))) goto done;
	    /* extract the shapes */
	    if((shapes = (size64_t*)malloc(sizeof(size64_t)*zarr_rank)) == NULL)
	        {stat = (THROW(NC_ENOMEM)); goto done;}
	    if((stat = NCZ_decodeints(jvalue, shapes))) goto done;
	}

	/* Capture dimension_separator (must precede chunk cache creation) */
	{
	    NCglobalstate* ngs = NC_getglobalstate();
	    assert(ngs != NULL);
	    zvar->dimension_separator = 0;
	    if((stat = NCJdictget(jvar,"dimension_separator",&jvalue))) goto done;
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
	    if((stat = NCJdictget(jvar,"fill_value",&jvalue))) goto done;
	    if(jvalue == NULL || NCJsort(jvalue) == NCJ_NULL)
		var->no_fill = 1;
	    else {
		size_t fvlen;
		nc_type atypeid = vtype;
		var->no_fill = 0;
		if((stat = NCZ_computeattrdata(var->type_info->hdr.id, &atypeid, jvalue, NULL, &fvlen, &var->fill_value)))
		    goto done;
		assert(atypeid == vtype);
		/* Note that we do not create the _FillValue
		   attribute here to avoid having to read all
		   the attributes and thus foiling lazy read.*/
	    }
	}

	/* chunks */
	{
	    size64_t chunks[NC_MAX_VAR_DIMS];
	    if((stat = NCJdictget(jvar,"chunks",&jvalue))) goto done;
	    if(jvalue != NULL && NCJsort(jvalue) != NCJ_ARRAY)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    /* Verify the rank */
	    assert (zarr_rank == NCJlength(jvalue));
	    if(zvar->scalar) {
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
		if(var->ndims != rank)
		    {stat = (THROW(NC_ENCZARR)); goto done;}
		if((var->chunksizes = malloc(sizeof(size_t)*zarr_rank)) == NULL)
		    {stat = NC_ENOMEM; goto done;}
		if((stat = NCZ_decodeints(jvalue, chunks))) goto done;
		/* validate the chunk sizes */
		zvar->chunkproduct = 1;
		for(j=0;j<rank;j++) {
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
	    if((stat = NCJdictget(jvar,"order",&jvalue))) goto done;
	    if(strcmp(NCJstring(jvalue),"C")==1)
		((NCZ_VAR_INFO_T*)var->format_var_info)->order = 1;
	    else ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 0;
	}
        /* filters key */
        /* From V2 Spec: A list of JSON objects providing codec configurations,
           or null if no filters are to be applied. Each codec configuration
           object MUST contain a "id" key identifying the codec to be used. */
	/* Do filters key before compressor key so final filter chain is in correct order */
	{
#ifdef ENABLE_NCZARR_FILTERS
	    if(var->filters == NULL) var->filters = (void*)nclistnew();
   	    if(zvar->incompletefilters == NULL) zvar->incompletefilters = (void*)nclistnew();
	    chainindex = 0; /* track location of filter in the chain */
	    if((stat = NCZ_filter_initialize())) goto done;
	    if((stat = NCJdictget(jvar,"filters",&jvalue))) goto done;
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
#ifdef ENABLE_NCZARR_FILTERS
	{ 
	    if(var->filters == NULL) var->filters = (void*)nclistnew();
	    if((stat = NCZ_filter_initialize())) goto done;
	    if((stat = NCJdictget(jvar,"compressor",&jfilter))) goto done;
	    if(jfilter != NULL && NCJsort(jfilter) != NCJ_NULL) {
	        if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
		if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
	    }
	}
#endif
	if((stat = NCZ_computedimrefs(file, zfile, map, var, rank, dimnames, shapes, var->dim)))
	    goto done;

	if(!zvar->scalar) {
	    /* Extract the dimids */
	    for(j=0;j<rank;j++)
	        var->dimids[j] = var->dim[j]->hdr.id;
	}

#ifdef ENABLE_NCZARR_FILTERS
        /* At this point, we can finalize the filters */
        if((stat = NCZ_filter_setup(var))) goto done;
#endif

	/* Clean up from last cycle */
	nclistfreeall(dimnames); dimnames = nclistnew();
        nullfree(varpath); varpath = NULL;
        nullfree(shapes); shapes = NULL;
        NCJreclaim(jncvar); jncvar = NULL;
        NCJreclaim(jvar); jvar = NULL;
        var = NULL;
    }

done:
    nullfree(shapes);
    nullfree(varpath);
    nullfree(key);
    nclistfreeall(dimnames);
    NCJreclaim(jvar);
    NCJreclaim(jncvar);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize subgroups into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param subgrpnames List of names of subgroups in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
read_subgrps(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int i,stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |subgrpnames|=%u",file->controller->path,grp->hdr.name,nclistlength(subgrpnames));

    /* Load each subgroup name in turn */
    for(i = 0; i < nclistlength(subgrpnames); i++) {
	NC_GRP_INFO_T* g = NULL;
	const char* gname = nclistget(subgrpnames,i);
	char norm_name[NC_MAX_NAME];
	/* Check and normalize the name. */
	if((stat = nc4_check_name(gname, norm_name)))
	    goto done;
	if((stat = nc4_grp_list_add(file, grp, norm_name, &g)))
	    goto done;
	if(!(g->format_grp_info = calloc(1, sizeof(NCZ_GRP_INFO_T))))
	    {stat = NC_ENOMEM; goto done;}
	((NCZ_GRP_INFO_T*)g->format_grp_info)->common.file = file;
    }

    /* Recurse to fill in subgroups */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = read_grp(file,zfile,map,g)))
	    goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Extract attributes from storage group or var and return
          the corresponding atypes list and the attributes json.
@param map - [in] the map object for storage
@param container - [in] the containing object
@param jattrsp - [out] the json for .zattrs
@param atypesp - [out] the attribute type list
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
NCZ_load_jatts(NCZMAP* map, NC_OBJ* container, NCjson** jattrsp, NClist** atypesp)
{
    int stat = NC_NOERR;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* jnczarr = NULL;
    NCjson* jattrs = NULL;
    NCjson* jncattr = NULL;
    NClist* atypes = NULL; /* envv list */

    ZTRACE(3,"map=%p container=%s nczarrv1=%d",map,container->name,nczarrv1);

    /* alway return (possibly empty) list of types */
    atypes = nclistnew();

    if(container->sort == NCGRP) {
	NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)container;
	/* Get grp's fullpath name */
	if((stat = NCZ_grpkey(grp,&fullpath)))
	    goto done;
    } else {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)container;
	/* Get var's fullpath name */
	if((stat = NCZ_varkey(var,&fullpath)))
	    goto done;
    }

    /* Construct the path to the .zattrs object */
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key)))
	goto done;

    /* Download the .zattrs object: may not exist */
    switch ((stat=NCZ_downloadjson(map,key,&jattrs))) {
    case NC_NOERR: break;
    case NC_EEMPTY: stat = NC_NOERR; break; /* did not exist */
    default: goto done; /* failure */
    }
    nullfree(key); key = NULL;

    if(jattrs != NULL) {
	/* Get _nczarr_attrs from .zattrs */
        stat = NCJdictget(jattrs,NCZ_V2_ATTR,&jncattr);
	if(!stat && jncattr == NULL)
	    {stat = NCJdictget(jattrs,NCZ_V2_ATTR_UC,&jncattr);}
	nullfree(key); key = NULL;
	switch (stat) {
	case NC_NOERR: break;
	case NC_EEMPTY: stat = NC_NOERR; jncattr = NULL; break;
	default: goto done; /* failure */
	}
	if(jncattr != NULL) {
	    NCjson* jtypes = NULL;
	    /* jncattr attribute should be a dict */
	    if(NCJsort(jncattr) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	    /* Extract "types; may not exist if only hidden attributes are defined */
	    if((stat = NCJdictget(jncattr,"types",&jtypes))) goto done;
	    if(jtypes != NULL) {
	        if(NCJsort(jtypes) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	        /* Convert to an envv list */
		if((stat = NCZ_jtypes2atypes(jtypes,atypes))) goto done;
	    }
	}
    }
    if(jattrsp) {*jattrsp = jattrs; jattrs = NULL;}
    if(atypesp) {*atypesp = atypes; atypes = NULL;}

done:
    if(stat) {
	NCJreclaim(jnczarr);
	nclistfreeall(atypes);
    }
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Potentially shared functions */

/**
 * @internal Synchronize dimension data from memory to map.
 *
 * @param grp Pointer to grp struct containing the dims.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
NCZ_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp)
{
    int i, stat=NC_NOERR;
    NCjson* jdims = NULL;
    NCjson* jdimsize = NULL;
    NCjson* jdimargs = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJnew(NCJ_DICT,&jdims);
    for(i=0; i<ncindexsize(grp->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
	char slen[128];

        snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
	if((stat = NCJnewstring(NCJ_INT,slen,&jdimsize))) goto done;

	/* If dim is not unlimited, then write in the old format to provide
           maximum back compatibility.
        */
	if(dim->unlimited) {
	    NCJnew(NCJ_DICT,&jdimargs);
	    if((stat = NCJaddstring(jdimargs,NCJ_STRING,"size"))) goto done;
	    if((stat = NCJappend(jdimargs,jdimsize))) goto done;
	    jdimsize = NULL;
  	    if((stat = NCJaddstring(jdimargs,NCJ_STRING,"unlimited"))) goto done;
	    if((stat = NCJaddstring(jdimargs,NCJ_INT,"1"))) goto done;
	} else { /* !dim->unlimited */
	    jdimargs = jdimsize;
	    jdimsize = NULL;
	}
	if((stat = NCJaddstring(jdims,NCJ_STRING,dim->hdr.name))) goto done;
	if((stat = NCJappend(jdims,jdimargs))) goto done;
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
done:
    NCJreclaim(jdims);
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps)
{
    int i,stat = NC_NOERR;
    NCjson* jvalue = NULL;

    ZTRACE(3,"jcontent=|%s| |dimdefs|=%u |varnames|=%u |subgrps|=%u",NCJtotext(jcontent),(unsigned)nclistlength(dimdefs),(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    if((stat=NCJdictget(jcontent,"dims",&jvalue))) goto done;
    if(jvalue != NULL) {
	if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Extract the dimensions defined in this group */
	for(i=0;i<NCJlength(jvalue);i+=2) {
	    NCjson* jname = NCJith(jvalue,i);
	    NCjson* jleninfo = NCJith(jvalue,i+1);
    	    NCjson* jtmp = NULL;
       	    const char* slen = "0";
       	    const char* sunlim = "0";
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    /* check the length */
            if(NCJsort(jleninfo) == NCJ_DICT) {
		if((stat = NCJdictget(jleninfo,"size",&jtmp))) goto done;
		if(jtmp== NULL)
		    {stat = NC_EBADNAME; goto done;}
		slen = NCJstring(jtmp);
		/* See if unlimited */
		if((stat = NCJdictget(jleninfo,"unlimited",&jtmp))) goto done;
	        if(jtmp == NULL) sunlim = "0"; else sunlim = NCJstring(jtmp);
            } else if(jleninfo != NULL && NCJsort(jleninfo) == NCJ_INT) {
		slen = NCJstring(jleninfo);		
	    } else
		{stat = NC_ENCZARR; goto done;}
	    nclistpush(dimdefs,strdup(norm_name));
	    nclistpush(dimdefs,strdup(slen));
    	    nclistpush(dimdefs,strdup(sunlim));
	}
    }

    if((stat=NCJdictget(jcontent,"vars",&jvalue))) goto done;
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

    if((stat=NCJdictget(jcontent,"groups",&jvalue))) goto done;
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
NCZ_parse_group_content_pure(NCZ_FILE_INFO_T*  zfile, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;

    ZTRACE(3,"zfile=%s grp=%s |varnames|=%u |subgrps|=%u",zfile->common.file->controller->path,grp->hdr.name,(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    nclistclear(varnames);
    if((stat = NCZ_searchvars(zfile,grp,varnames))) goto done;
    nclistclear(subgrps);
    if((stat = NCZ_searchsubgrps(zfile,grp,subgrps))) goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
@param zfile - [in] the containing file (annotation)
@param container - [in] the containing object
@param jatypes - [in] the json from either .nczattrs or _nczarr_attrs representing the attribute types
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
NCZ_read_atts(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_OBJ* container)
{
    int stat = NC_NOERR;
    int i;
    char* fullpath = NULL;
    char* key = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_ATT_INFO_T* att = NULL;
    NCindex* attlist = NULL;
    NClist* atypes = NULL;
    NCjson* jattrs = NULL;
    nc_type typeid;
    size_t len, typelen;
    void* data = NULL;
    NC_ATT_INFO_T* fillvalueatt = NULL;
    nc_type typehint = NC_NAT;
    int purezarr;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    zfile = file->format_file_info;
    map = zfile->map;

    purezarr = (zfile->controls.flags & FLAG_PUREZARR)?1:0;
 
    if(container->sort == NCGRP) {	
	grp = ((NC_GRP_INFO_T*)container);
	attlist =  grp->att;
    } else {
	var = ((NC_VAR_INFO_T*)container);
        zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
	attlist =  var->att;
    }

    switch ((stat = NCZ_load_jatts(map, container, &jattrs, &atypes))) {
    case NC_NOERR: break;
    case NC_EEMPTY:  /* container has no attributes */
        stat = NC_NOERR;
	break;
    default: goto done; /* true error */
    }

    if(jattrs != NULL) {
	/* Iterate over the attributes to create the in-memory attributes */
	/* Watch for special cases: _FillValue and  _ARRAY_DIMENSIONS (xarray), etc. */
	for(i=0;i<NCJlength(jattrs);i+=2) {
	    NCjson* key = NCJith(jattrs,i);
	    NCjson* value = NCJith(jattrs,i+1);
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
		    if((stat = NCZ_read_provenance(file,aname,NCJstring(value))))
			goto done;
		}
		/* case 2: name = _ARRAY_DIMENSIONS, sort==NCVAR, flags & HIDDENATTRFLAG */
		if(strcmp(aname,NC_XARRAY_DIMS)==0 && var != NULL && (ra->flags & HIDDENATTRFLAG)) {
  	            /* store for later */
		    int i;
		    assert(NCJsort(value) == NCJ_ARRAY);
		    if((zvar->xarray = nclistnew())==NULL)
		        {stat = NC_ENOMEM; goto done;}
		    for(i=0;i<NCJlength(value);i++) {
			const NCjson* k = NCJith(value,i);
			assert(k != NULL && NCJsort(k) == NCJ_STRING);
			nclistpush(zvar->xarray,strdup(NCJstring(k)));
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
	    if((stat = NCZ_computeattrinfo(aname,atypes,typehint,purezarr,value,
				   &typeid,&typelen,&len,&data)))
		goto done;
	    if((stat = ncz_makeattr(container,attlist,aname,typeid,len,data,&att)))
		goto done;
	    /* No longer need this copy of the data */
   	    if((stat = NC_reclaim_data_all(file->controller,att->nc_typeid,data,len))) goto done;	    	    
	    data = NULL;
	    if(isfillvalue)
	        fillvalueatt = att;
	    if(ismaxstrlen && att->nc_typeid == NC_INT)
	        zvar->maxstrlen = ((int*)att->data)[0];
	    if(isdfaltmaxstrlen && att->nc_typeid == NC_INT)
	        zfile->default_maxstrlen = ((int*)att->data)[0];
	}
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue((NC_VAR_INFO_T*)container)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
	((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
	((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    if(data != NULL)
        stat = NC_reclaim_data(file->controller,att->nc_typeid,data,len);
    NCJreclaim(jattrs);
    nclistfreeall(atypes);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_searchvars(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int i,stat = NC_NOERR;
    char* grpkey = NULL;
    char* varkey = NULL;
    char* zarray = NULL;
    NClist* matches = nclistnew();

    /* Compute the key for the grp */
    if((stat = NCZ_grpkey(grp,&grpkey))) goto done;
    /* Get the map and search group */
    if((stat = nczmap_search(zfile->map,grpkey,matches))) goto done;
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

static int
NCZ_searchsubgrps(NCZ_FILE_INFO_T* zfile, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int i,stat = NC_NOERR;
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
	if((stat = nczm_concat(subkey,Z2GROUP,&zgroup))) goto done;
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

/* Convert a list of integer strings to 64 bit dimension sizes (shapes) */
static int
NCZ_decodeints(NCjson* jshape, size64_t* shapes)
{
    int i, stat = NC_NOERR;

    for(i=0;i<NCJlength(jshape);i++) {
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

/*
Extract type and data for an attribute
*/
static int
NCZ_computeattrinfo(const char* name, NClist* atypes, nc_type typehint, int purezarr, NCjson* values,
		nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap)
{
    int stat = NC_NOERR;
    int i;
    size_t len, typelen;
    void* data = NULL;
    nc_type typeid;

    ZTRACE(3,"name=%s |atypes|=%u typehint=%d purezarr=%d values=|%s|",name,nclistlength(atypes),typehint,purezarr,NCJtotext(values));

    /* Get type info for the given att */
    typeid = NC_NAT;
    for(i=0;i<nclistlength(atypes);i+=2) {
	const char* aname = nclistget(atypes,i);
	if(strcmp(aname,name)==0) {
	    const char* atype = nclistget(atypes,i+1);
	    if((stat = ncz_dtype2nctype(atype,typehint,purezarr,&typeid,NULL,NULL))) goto done;
	    break;
	}
    }
    if(typeid > NC_MAX_ATOMIC_TYPE)
	{stat = NC_EINTERNAL; goto done;}
    /* Use the hint if given one */
    if(typeid == NC_NAT)
        typeid = typehint;

    if((stat = NCZ_computeattrdata(typehint, &typeid, values, &typelen, &len, &data))) goto done;

    if(typeidp) *typeidp = typeid;
    if(lenp) *lenp = len;
    if(typelenp) *typelenp = typelen;
    if(datap) {*datap = data; data = NULL;}

done:
    nullfree(data);
    return ZUNTRACEX(THROW(stat),"typeid=%d typelen=%d len=%u",*typeidp,*typelenp,*lenp);
}

/*
Extract data for an attribute
*/
static int
NCZ_computeattrdata(nc_type typehint, nc_type* typeidp, NCjson* values, size_t* typelenp, size_t* countp, void** datap)
{
    int stat = NC_NOERR;
    NCbytes* buf = ncbytesnew();
    size_t typelen;
    nc_type typeid = NC_NAT;
    NCjson* jtext = NULL;
    int reclaimvalues = 0;
    int isjson = 0; /* 1 => attribute value is neither scalar nor array of scalars */
    int count = 0; /* no. of attribute values */

    ZTRACE(3,"typehint=%d typeid=%d values=|%s|",typehint,*typeidp,NCJtotext(values));

    /* Get assumed type */
    if(typeidp) typeid = *typeidp;
    if(typeid == NC_NAT && !isjson) {
        if((stat = NCZ_inferattrtype(values,typehint, &typeid))) goto done;
    }

    /* See if this is a simple vector (or scalar) of atomic types */
    isjson = NCZ_iscomplexjson(values,typeid);

    if(isjson) {
	/* Apply the JSON attribute convention and convert to JSON string */
	typeid = NC_CHAR;
	if((stat = NCZ_json_convention_read(values,&jtext))) goto done;
	values = jtext; jtext = NULL;
	reclaimvalues = 1;
    } 

    if((stat = NC4_inq_atomic_type(typeid, NULL, &typelen)))
        goto done;

    /* Convert the JSON attribute values to the actual netcdf attribute bytes */
    if((stat = NCZ_attr_convert(values,typeid,typelen,&count,buf))) goto done;

    if(typelenp) *typelenp = typelen;
    if(typeidp) *typeidp = typeid; /* return possibly inferred type */
    if(countp) *countp = count;
    if(datap) *datap = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    if(reclaimvalues) NCJreclaim(values); /* we created it */
    return ZUNTRACEX(THROW(stat),"typelen=%d count=%u",(typelenp?*typelenp:0),(countp?*countp:-1));
}

/* Compute the set of dim refs for this variable, taking purezarr and xarray into account */
static int
NCZ_computedimrefs(NC_FILE_INFO_T* file, NCZ_FILE_INFO_T* zfile, NCZMAP* map, NC_VAR_INFO_T* var, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int stat = NC_NOERR;
    int i;
    int createdims = 0; /* 1 => we need to create the dims in root if they do not already exist */
    int purezarr = 0;
    int xarray = 0;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
    NCjson* jatts = NULL;

    ZTRACE(3,"file=%s var=%s purezarr=%d xarray=%d ndims=%d shape=%s",
    	file->controller->path,var->hdr.name,purezarr,xarray,(int)ndims,nczprint_vector(ndims,shapes));

    if(zfile->controls.flags & FLAG_PUREZARR) purezarr = 1;
    if(zfile->controls.flags & FLAG_XARRAYDIMS) xarray = 1;

    if(purezarr && xarray) {/* Read in the attributes to get xarray dimdef attribute; Note that it might not exist */
	/* Note that if xarray && !purezarr, then xarray will be superceded by the nczarr dimensions key */
        char zdimname[4096];
	if(zvar->xarray == NULL) {
	    assert(nclistlength(dimnames) == 0);
	    if((stat = NCZ_read_atts(file,zfile,map,(NC_OBJ*)var))) goto done;
	}
	if(zvar->xarray != NULL) {
	    /* convert xarray to the dimnames */
	    for(i=0;i<nclistlength(zvar->xarray);i++) {
	        snprintf(zdimname,sizeof(zdimname),"/%s",(const char*)nclistget(zvar->xarray,i));
	        nclistpush(dimnames,strdup(zdimname));
	    }
	}
	createdims = 1; /* may need to create them */
    }

    /* If pure zarr and we have no dimref names, then fake it */
    if(purezarr && nclistlength(dimnames) == 0) {
	createdims = 1;
        for(i=0;i<ndims;i++) {
	    /* Compute the set of absolute paths to dimrefs */
            char zdimname[4096];
	    snprintf(zdimname,sizeof(zdimname),"/%s_%llu",ZDIMANON,shapes[i]);
	    nclistpush(dimnames,strdup(zdimname));
	}
    }

    /* Now, use dimnames to get the dims; create if necessary */
    if((stat = NCZ_parsedimrefs(file,dimnames,shapes,dims,createdims)))
        goto done;

done:
    NCJreclaim(jatts);
    return ZUNTRACE(THROW(stat));
}

/**
Implement the JSON convention:
Stringify it as the value and make the attribute be of type "char".
*/

static int
NCZ_json_convention_read(NCjson* json, NCjson** jtextp)
{
    int stat = NC_NOERR;
    NCjson* jtext = NULL;
    char* text = NULL;

    if(json == NULL) {stat = NC_EINVAL; goto done;}
    if(NCJunparse(json,0,&text)) {stat = NC_EINVAL; goto done;}
    if(NCJnewstring(NCJ_STRING,text,&jtext)) {stat = NC_EINVAL; goto done;}
    *jtextp = jtext; jtext = NULL;
done:
    NCJreclaim(jtext);
    nullfree(text);
    return stat;
}

/* Convert a json value to actual data values of an attribute. */
static int
NCZ_attr_convert(NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst)
{
    int stat = NC_NOERR;
    int i;
    int count = 0;
    
    ZTRACE(3,"src=%s typeid=%d typelen=%u",NCJtotext(src),typeid,typelen);
	    
    /* 3 cases:
       (1) singleton atomic value
       (2) array of atomic values
       (3) other JSON expression
    */
    switch (NCJsort(src)) {
    case NCJ_INT: case NCJ_DOUBLE: case NCJ_BOOLEAN: /* case 1 */
	count = 1;
	if((stat = NCZ_convert1(src, typeid, dst)))
	    goto done;
	break;

    case NCJ_ARRAY:
        if(typeid == NC_CHAR) {
	    if((stat = NCZ_charify(src,dst))) goto done;
	    count = ncbyteslength(dst);
        } else {
	    count = NCJlength(src);
	    for(i=0;i<count;i++) {
	        NCjson* value = NCJith(src,i);
                if((stat = NCZ_convert1(value, typeid, dst))) goto done;
	    }
	}
	break;
    case NCJ_STRING:
	if(typeid == NC_CHAR) {
	    if((stat = NCZ_charify(src,dst))) goto done;
	    count = ncbyteslength(dst);
	    /* Special case for "" */
	    if(count == 0) {
	        ncbytesappend(dst,'\0');
	        count = 1;
	    }
	} else {
	    if((stat = NCZ_convert1(src, typeid, dst))) goto done;
	    count = 1;
	}
	break;
    default: stat = (THROW(NC_ENCZARR)); goto done;
    }
    if(countp) *countp = count;

done:
    return ZUNTRACE(THROW(stat));
}

static int
NCZ_parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create)
{
    int i, stat = NC_NOERR;
    NClist* segments = NULL;

    for(i=0;i<nclistlength(dimnames);i++) {
	NC_GRP_INFO_T* g = NULL;
	NC_DIM_INFO_T* d = NULL;
	int j;
	const char* dimpath = nclistget(dimnames,i);
	const char* dimname = NULL;

	/* Locate the corresponding NC_DIM_INFO_T* object */
	nclistfreeall(segments);
	segments = nclistnew();
	if((stat = ncz_splitkey(dimpath,segments)))
	    goto done;
	if((stat=NCZ_locategroup(file,nclistlength(segments)-1,segments,&g)))
	    goto done;
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
	    if((stat = NCZ_createdim(file, dimname, shape[i], &dims[i])))
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

/* Convert an attribute "types list to an envv style list */
static int
NCZ_jtypes2atypes(NCjson* jtypes, NClist* atypes)
{
    int i, stat = NC_NOERR;
    for(i=0;i<NCJlength(jtypes);i+=2) {
	const NCjson* key = NCJith(jtypes,i);
	const NCjson* value = NCJith(jtypes,i+1);
	if(NCJsort(key) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	if(NCJsort(value) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	nclistpush(atypes,strdup(NCJstring(key)));
	nclistpush(atypes,strdup(NCJstring(value)));
    }
done:
    return stat;
}

/* Convert a JSON singleton or array of strings to a single string */
static int
NCZ_charify(NCjson* src, NCbytes* buf)
{
    int i, stat = NC_NOERR;
    struct NCJconst jstr = NCJconst_empty;

    if(NCJsort(src) != NCJ_ARRAY) { /* singleton */
        if((stat = NCJcvt(src, NCJ_STRING, &jstr))) goto done;
        ncbytescat(buf,jstr.sval);
    } else for(i=0;i<NCJlength(src);i++) {
	NCjson* value = NCJith(src,i);
	if((stat = NCJcvt(value, NCJ_STRING, &jstr))) goto done;
	ncbytescat(buf,jstr.sval);
        nullfree(jstr.sval);jstr.sval = NULL;
    }
done:
    nullfree(jstr.sval);
    return stat;
}

/*
Given a list of segments, find corresponding group.
*/
static int
NCZ_locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp)
{
    int i, j, found, stat = NC_NOERR;
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
NCZ_createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp)
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

/**************************************************/
/* Format Dispatch table */

static const NCZ_Formatter NCZ_formatter3_table = {
    NCZARRFORMAT1,
    ZARRFORMAT3,
    NCZ_FORMATTER_VERSION,

    ZF3_create,
    ZF3_open,
    ZF3_readmeta,
    ZF3_writemeta,
    ZF3_readattrs,
    ZF3_close
};

const NCZ_Formatter* NCZ_formatter3 = &NCZ_formatter3_table;

int
NCZF3_initialize(void)
{
    return NC_NOERR;
}

int
NCZF3_finalize(void)
{
    return NC_NOERR;
}

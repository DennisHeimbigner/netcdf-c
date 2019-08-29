/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"

/* Forward */
static int ncz_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp);
static int ncz_jsonize_atts(NCindex* attlist, NCjson** jattrsp, NCjson** jtypesp);
static int load_jatts(NCZMAP* map, NC_OBJ* container, NCjson** jattrsp, NCjson** jtypesp);
static int zconvert(nc_type typeid, size_t typelen, void* dst, NClist* src);
static int computeattrinfo(NC_ATT_INFO_T* att, NCjson* jtypes, NCjson* values);
static int zname2type(const char* tname);
static int parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps);
static int define_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
static int define_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo);
static int define_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
static int define_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);
static int searchvars(NC_FILE_INFO_T*, NC_GRP_INFO_T*, NClist*);
static int searchsubgrps(NC_FILE_INFO_T*, NC_GRP_INFO_T*, NClist*);
static int locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int parsedimrefs(NC_FILE_INFO_T*, NClist* dimrefs, NC_DIM_INFO_T** dims);
static int simulatedimrefs(NC_FILE_INFO_T* file, int rank, size64_t* shapes, NC_DIM_INFO_T** dims);
static int decodeints(NCjson* jshape, size64_t* shapes);

/**************************************************/
/**************************************************/
/* Synchronize functions to make map and memory
be consistent. There are two sets of functions,
1) _sync_ - push memory to map (optionally create target)
2) _read_ - pull map data into memory
These functions are generally non-recursive. It is assumed
that the recursion occurs in the caller's code.
*/

/**
 * @internal Synchronize file data from memory to map.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_file(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    char version[1024];
    NCjson* json = NULL;
    NCZMAP* map = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;

    LOG((3, "%s: file: %s", __func__, file->controller->path));

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Overwrite in all cases */
    if((stat = NCJnew(NCJ_DICT,&json)))
	goto done;

    /* fill */
    snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
    if((stat = NCJaddstring(json,NCJ_STRING,"zarr_format"))) goto done;
    if((stat = NCJaddstring(json,NCJ_INT,version))) goto done;
    if((stat = NCJaddstring(json,NCJ_STRING,"nczarr_version"))) goto done;
    {
	char ver[1024];
	snprintf(ver,sizeof(ver),"%lu.%lu.%lu",
	   zinfo->zarr.nczarr_version.major,
	   zinfo->zarr.nczarr_version.minor,
	   zinfo->zarr.nczarr_version.release);
        if((stat = NCJaddstring(json,NCJ_STRING,ver))) goto done;
    }
    /* Write back to map */
    if((stat=NCZ_uploadjson(map,ZMETAROOT,json)))
	goto done;
done:
    NCJreclaim(json);
    return stat;
}

/**
 * @internal Synchronize dimension data from memory to map.
 *
 * @param grp Pointer to grp struct containing the dims.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp)
{
    int i, stat=NC_NOERR;
    NCjson* jdims = NULL;

    LOG((3, "%s: ", __func__));

    NCJnew(NCJ_DICT,&jdims);   
    for(i=0; i<ncindexsize(grp->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
	char slen[128];
	snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
	if((stat = NCJaddstring(jdims,NCJ_STRING,dim->hdr.name))) goto done;
	if((stat = NCJaddstring(jdims,NCJ_INT,slen))) goto done;
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
done:
    NCJreclaim(jdims);
    return stat;
}

/**
 * @internal Synchronize group from memory to map.
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int i,stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    char version[1024];
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    NCjson* jcontent = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrps = NULL;

    LOG((3, "%s: dims: %s", __func__, key));

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Construct grp path */
    if((stat = NCZ_grppath(grp,&fullpath)))
	goto done;

    /* Create dimensions dict */
    if((stat = ncz_collect_dims(file,grp,&jdims))) goto done;

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

    /* Create the zcontent json object */
    if((stat = NCJnew(NCJ_DICT,&jcontent)))
	goto done;

    /* Insert the various dicts and arrays */
    if((stat = NCJinsert(jcontent,"dims",jdims))) goto done;
    if((stat = NCJinsert(jcontent,"vars",jvars))) goto done;
    if((stat = NCJinsert(jcontent,"groups",jsubgrps))) goto done;

    /* build .zgroup path */
    if((stat = nczm_suffix(fullpath,ZGROUP,&key)))
	goto done;
    if((stat = NCJnew(NCJ_DICT,&json)))
	goto done;
    snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
    if((stat = NCJaddstring(json,NCJ_STRING,"zarr_format"))) goto done;
    if((stat = NCJaddstring(json,NCJ_INT,version))) goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,json)))
	goto done;
    nullfree(key); key = NULL;

    /* build zcontent path */
    if((stat = nczm_suffix(fullpath,NCZCONTENT,&key)))
	goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jcontent)))
	goto done;
    nullfree(key); key = NULL;

done:
    NCJreclaim(json);
    NCJreclaim(jcontent);
    NCJreclaim(jdims);
    NCJreclaim(jvars);
    NCJreclaim(jsubgrps);
    nullfree(fullpath);
    nullfree(key);
    return stat;
}

/**
 * @internal Synchronize variable data from memory to map.
 *
 * @param var Pointer to var struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int i,stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    char number[1024];
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* jvar = NULL;
    NCjson* jtmp1 = NULL;
    NCjson* jtmp2 = NULL;

    LOG((3, "%s: dims: %s", __func__, key));

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Construct var path */
    if((stat = NCZ_varpath(var,&fullpath)))
	goto done;

    /* build .zarray path */
    if((stat = nczm_suffix(fullpath,ZARRAY,&key)))
	goto done;

    /* Create the zarray json object */
    if((stat = NCJnew(NCJ_DICT,&jvar)))
	goto done;

    /* zarr_format key */
    snprintf(number,sizeof(number),"%d",zinfo->zarr.zarr_version);
    if((stat = NCJaddstring(jvar,NCJ_STRING,"zarr_format"))) goto done;
    if((stat = NCJaddstring(jvar,NCJ_INT,number))) goto done;

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    if((stat = NCJaddstring(jvar,NCJ_STRING,"shape"))) goto done;
    /* Create the list */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp1))) goto done;
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	snprintf(number,sizeof(number),"%lld",(off64_t)dim->len);
	NCJaddstring(jtmp1,NCJ_INT,number);
    }
    if((stat = NCJappend(jvar,jtmp1))) goto done;
    NCJreclaim(jtmp1); jtmp1 = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"dtype"))) goto done;
    {   /* Add the type name */
	const char* dtypename;
	int endianness = var->type_info->endianness;
	int islittle = (endianness == NC_ENDIAN_LITTLE
		        || NCZ_isLittleEndian());
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE && atomictype != NC_STRING);
        if((stat = ncz_zarr_type_name(atomictype,islittle,&dtypename))) goto done;
        if((stat = NCJaddstring(jvar,NCJ_STRING,dtypename))) goto done;
    }

    /* chunks key */
    /* list of chunk sizes */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"chunks"))) goto done;
    /* Create the list */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp1))) goto done;
    for(i=0;i<var->ndims;i++) {
	snprintf(number,sizeof(number),"%lld",(off64_t)var->chunksizes[i]);
	NCJaddstring(jtmp1,NCJ_INT,number);
    }
    if((stat = NCJappend(jvar,jtmp1))) goto done;
    NCJreclaim(jtmp1); jtmp1 = NULL;
    
    /* fill_value key */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"fill_value"))) goto done;
    /* A scalar value providing the default value to use for uninitialized
       portions of the array, or ``null`` if no fill_value is to be used. */
    { /* Use the defaults defined in netdf.h */
	const char* dfalt;
	int sort;
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE && atomictype != NC_STRING);
        if((stat = ncz_default_fill_value(atomictype,&dfalt,&sort))) goto done;
        if((stat = NCJaddstring(jvar,sort,dfalt))) goto done;
    }

    /* order key */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"order"))) goto done;
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */ 
    if((stat = NCJaddstring(jvar,NCJ_STRING,"C"))) goto done;

    /* compressor key */
    /* A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"compressor"))) goto done;
    /* Default to null for now */ 
    if((stat = NCJaddstring(jvar,NCJ_NULL,"null"))) goto done;

    /* filters key */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"filters"))) goto done;
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"filters"))) goto done;
    /* Default to null for now */ 
    if((stat = NCJaddstring(jvar,NCJ_NULL,"null"))) goto done;

    /* Write back to map */
    if((stat=NCZ_uploadjson(map,key,jvar)))
	goto done;

    /* build zdimrefs path */
    nullfree(key); key = NULL;
    if((stat = nczm_suffix(fullpath,ZDIMREFS,&key)))
	goto done;
    NCJreclaim(jtmp2); jtmp2 = NULL;
    /* Create the zdimrefs json object */
    if((stat = NCJnew(NCJ_ARRAY,&jtmp2)))
	goto done;
    /* Walk the dimensions and capture the fullpath names */
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	NCJaddstring(jtmp2,NCJ_STRING,dim->hdr.name);
    }
    /* Write back to map */
    if((stat=NCZ_uploadjson(map,key,jtmp2)))
	goto done;
    NCJreclaim(jtmp2); jtmp2 = NULL;

done:
    nullfree(fullpath);
    nullfree(key);
    NCJreclaim(jvar);
    NCJreclaim(jtmp1);
    NCJreclaim(jtmp2);
    return stat;
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
int
ncz_sync_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCindex* attlist)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;

    LOG((3, "%s", __func__));

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Jsonize the attribute list */
    if((stat = ncz_jsonize_atts(attlist,&jatts,&jtypes)))
	goto done;

    /* Construct container path */
    if(container->sort == NCGRP)
        stat = NCZ_grppath((NC_GRP_INFO_T*)container,&fullpath);
    else
        stat = NCZ_varpath((NC_VAR_INFO_T*)container,&fullpath);
    if(stat)
	goto done;

    /* write .zattrs path */
    if((stat = nczm_suffix(fullpath,ZATTRS,&key)))
	goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jatts)))
	goto done;
    nullfree(key); key = NULL;

    /* write .ztypes path */
    if((stat = nczm_suffix(fullpath,ZATTRTYPES,&key)))
	goto done;
    /* Write to map */
    if((stat=NCZ_uploadjson(map,key,jtypes)))
	goto done;

done:
    nullfree(fullpath);
    nullfree(key);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
    return stat;
}

#if 0
/**
 * @internal Recursively walk the meta data and push it to the map.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_rec_write_metadata(NCZ_FILE_INFO_T* zinfo, NC_GRP_INFO_T *grp)
{
    int stat = NC_NOERR;
    NCZ_GRP_INFO* zgrp;
    int i;
    char* path = NULL;
    NCZMAP* map = zinfo->map;
    NCjson* json = NULL;

    assert(grp && NCZ_getannote((NC_OBJ*)grp));
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Do everything in preorder so for example dims are defined before use */
    zgrp = NCZ_getannote((NC_OBJ*)grp);   
    assert((zgrp != NULL));

    if(zgrp->common.synced) goto done;

wrong    /* For all groups, construct .zgroup object */
    {
	char version[1024];
	/* Construct .zgroup full path */
        if((stat = nczm_suffix("/",ZGROUP,&path)))
	    goto done;
        /* Create root group contents /.zgroup */
        if((stat = NCJnew(NCJ_DICT,&json)))
	    goto done;
	snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
	if((stat = NCJaddstring(json,NCJ_STRING,"zarr_format"))) goto done;
	if((stat = NCJaddstring(json,NCJ_INT,version))) goto done;
	/* Write to map */
        if((stat=NCZ_uploadjson(map,path,json)))
	    goto done;
    }

    /* Sync any dimensions in this group */
    if ((stat = zsync_dims(zinfo,grp)))
        goto done;

    /* Sync with global attributes. */
    if ((stat = zsync_gatts(zinfo,grp)))
        goto done;

    /* Sync with vars. */
    if ((stat = zsync_vars(zinfo,grp)))
        goto done;

    /* Sync with types. */
    if ((stat = zsync_types(zinfo,grp)))
        goto done;

    /* Recursively call this function for each child group, if any, stopping
     * if there is an error. */
    for(i=0; i<ncindexsize(grp->children); i++) {
        if ((stat = zsync_group(zinfo,(NC_GRP_INFO_T*)ncindexith(grp->children,i))))
            goto done;
    }

    /* Mark group as sync'd */
    zgrp->common.synced = 1;

    LOG((4, "%s: enddef group %s", __func__, grp->hdr.name));

done:
    nullfree(path);
    NCJreclaim(json);
    return stat;
}

/**
 * @internal Sync for global atts in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zsync_gatts(NCZ_FILE_INFO* zinfo, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NC_ATT_INFO_T *att;
    int a;

    for(a = 0; a < ncindexsize(grp->att); a++) {
        NCZ_ATT_INFO* zatt;
        att = (NC_ATT_INFO_T* )ncindexith(grp->att, a);
        assert(att && NCZ_getannote((NC_OBJ*)att));
        zatt = NCZ_getannote((NC_OBJ*)att);
	nullfree(zatt);
        NCZ_annotate((NC_OBJ*)att,NULL); /* avoid memory errors */
        zatt->common.synced = 1; /* Mark att as sync'd */
    }
    return stat;
}

/**
 * @internal Materialize vars in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zsync_vars(NCZ_FILE_INFO* zinfo, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NC_VAR_INFO_T* var;
    NCZ_VAR_INFO* zvar;
    NC_ATT_INFO_T* att;
    int a, i;

    for(i = 0; i < ncindexsize(grp->vars); i++) {
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars, i);
        assert(var && NCZ_getannote((NC_OBJ*)var));
        zvar = NCZ_getannote((NC_OBJ*)var);
        for(a = 0; a < ncindexsize(var->att); a++) {
            NCZ_ATT_INFO* zatt;
            att = (NC_ATT_INFO_T*)ncindexith(var->att, a);
            assert(att && NCZ_getannote((NC_OBJ*)att));
            zatt = NCZ_getannote((NC_OBJ*)att);
	    nullfree(zatt);
            NCZ_annotate((NC_OBJ*)att,NULL); /* avoid memory errors */
        zatt->common.synced = 1; /* Mark att as sync'd */
        }
	nullfree(zvar);
        NCZ_annotate((NC_OBJ*)var,NULL); /* avoid memory errors */
        zvar->common.synced = 1; /* Mark as sync'd */
    }
    return stat;
}

/**
 * @internal Materialize dims in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zsync_dims(NCZ_FILE_INFO* zinfo, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NCZMAP* map = zinfo->map;
    NC_DIM_INFO_T* dim = NULL;
    NCZ_DIM_INFO* zdim = NULL;
    int i;
    char* prefix = NULL;
    char* key = NULL;
    NCjson* json = NULL;

    for(i = 0; i < ncindexsize(grp->dim); i++) {
	char clen[1024];
        dim = (NC_DIM_INFO_T*)ncindexith(grp->dim, i);
        assert(dim && NCZ_getannote((NC_OBJ*)dim));
        zdim = NCZ_getannote((NC_OBJ*)dim);
	if(zdim->common.synced) continue;
        /* Construct the .zdims path */
        /* Get prefix name for the parent grp */
        if((stat = NCZ_fullpath(grp,&prefix)))
	    goto done;
        /* Find|Create .zdims path */
        if((stat = nczm_suffix(prefix,ZDIMS,&key)))
	    goto done;
        /* Create */
        if((stat = NCJnew(NCJ_DICT,&json)))
	    goto done;
        /* Add the new dimension */
        if((stat = NCJaddstring(json,NCJ_STRING,grp->hdr.name))) goto done;
        snprintf(clen,sizeof(clen),"%lld",(off64_t)dim->len);
        if((stat = NCJaddstring(json,NCJ_INT,clen))) goto done;
        /* Upload */
        if((stat = NCZ_uploadjson(map,key,json)))
  	    goto done;
        zdim->common.synced = 1; /* Mark as sync'd */
    }
done:
    NCJreclaim(json);
    nullfree(prefix);
    nullfree(key);
    return stat;
}

/**
 * @internal Sync for types in a group.  Set values to
 * 0 after closing types. Because of type reference counters, these
 * closes can be called multiple times.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_rec_write_groups_types(NCZ_FILE_INFO_T* zinfo, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    int i;
    NC_TYPE_INFO_T* type;
    NCZ_TYPE_INFO* ztype;

    for(i = 0; i < ncindexsize(grp->type); i++)
    {
        type = (NC_TYPE_INFO_T*)ncindexith(grp->type, i);
        assert(type && NCZ_getannote((NC_OBJ*)type));
        /* Get Zarr-specific type info. */
        ztype = NCZ_getannote((NC_OBJ*)type);
	nullfree(ztype);
        NCZ_annotate((NC_OBJ*)type,NULL); /* avoid memory errors */
	ztype->common.synced = 1;
    }

    return stat;
}
#endif /*0*/

/**************************************************/

/**
@internal Convert a list of attributes to corresponding json.
Note that this does not push to the file.
@param zfile - [in] the containing file (annotation)
@param container - [in] the containing object
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
ncz_jsonize_atts(NCindex* attlist, NCjson** jattrsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    int i;
    NCjson* jattrs = NULL;
    NCjson* jtypes = NULL;
    NCjson* akey = NULL;
    NCjson* tkey = NULL;
    NCjson* jdata = NULL;
    NCjson* jtype = NULL;

    if((stat = NCJnew(NCJ_DICT,&jattrs))) goto done;
    if((stat = NCJnew(NCJ_DICT,&jtypes))) goto done;

    /* Iterate over the attribute list */
    for(i=0;i<ncindexsize(attlist);i++) {
	NC_ATT_INFO_T* att = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	char name[NC_MAX_NAME];
	size_t typelen;
	/* Create the attribute dict key+value*/
        if((stat = NCJnew(NCJ_STRING,&akey))) goto done;
	akey->value = strdup(att->hdr.name);
        if((stat = NCJnew(NCJ_ARRAY,&jdata))) goto done;
	if((stat = NCZ_stringconvert(att->nc_typeid,att->len,att->data,jdata)))
	    goto done;
	nclistpush(jattrs->dict,akey);
	akey = NULL;
	nclistpush(jattrs->dict,jdata);
	jdata = NULL;
	/* Create the type dict value (key is same)*/
        if((stat = NCJnew(NCJ_STRING,&tkey))) goto done;
	tkey->value = strdup(att->hdr.name);
        if((stat = NCJnew(NCJ_STRING,&jtype))) goto done;
	if((stat = NC4_inq_type(0,att->nc_typeid,name,&typelen)))
	    goto done;
	jtype->value = strdup(name);
	nclistpush(jtypes->dict,tkey);
	tkey = NULL;
	nclistpush(jattrs->dict,jtype);
	jtype = NULL;
    }

    if(jattrsp) {*jattrsp = jattrs; jattrs = NULL;}
    if(jtypesp) {*jtypesp = jtypes; jtypes = NULL;}

done:
    NCJreclaim(akey);
    NCJreclaim(tkey);
    NCJreclaim(jdata);
    NCJreclaim(jtype);
    NCJreclaim(jattrs);
    NCJreclaim(jtypes);
    return stat;
}

/**
@internal Extract attributes from a group or var and return
the corresponding NCjson dict.
@param map - [in] the map object for storage
@param container - [in] the containing object
@param jattrsp - [out] the json for .zattrs
@param jtypesp - [out] the json for .ztypes
@return NC_NOERR
@author Dennis Heimbigner
*/
static int
load_jatts(NCZMAP* map, NC_OBJ* container, NCjson** jattrsp, NCjson** jtypesp)
{
    int stat = NC_NOERR;
    char* fullpath = NULL;
    char* akey = NULL;
    char* tkey = NULL;
    NCjson* jattrs = NULL;
    NCjson* jtypes = NULL;

    if(container->sort == NCGRP) {
        NC_GRP_INFO_T* grp = (NC_GRP_INFO_T*)container;
        /* Get grp's fullpath name */
        if((stat = NCZ_grppath(grp,&fullpath)))
	    goto done;
    } else {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)container;
        /* Get var's fullpath name */
        if((stat = NCZ_varpath(var,&fullpath)))
	    goto done;
    }

    /* Construct the path to the .zattrs object */
    if((stat = nczm_suffix(fullpath,ZATTRS,&akey)))
	goto done;

    /* Download|Create the .zattrs object */
    if((stat = NCJnew(NCJ_DICT,&jattrs)))
	goto done;

    /* Construct the path to the .ztype object */
    if((stat = nczm_suffix(fullpath,ZATTRTYPES,&tkey)))
	goto done;

    /* Download|Create the .ztypes object */
    if((stat = NCJnew(NCJ_DICT,&jtypes)))
	goto done;
    if(jattrsp) {*jattrsp = jattrs; jattrs = NULL;}
    if(jtypesp) {*jtypesp = jtypes; jtypes = NULL;}

done:
    if(stat) {
	NCJreclaim(jattrs);
	NCJreclaim(jtypes);
    }
    nullfree(fullpath);
    nullfree(akey);
    nullfree(tkey);
    return stat;
}

static int
zconvert(nc_type typeid, size_t typelen, void* dst0, NClist* src)
{
    int stat = NC_NOERR;
    int i;
    /* Work in char* space so we can do pointer arithmetic */
    char* dst = dst0;

    for(i=0;i<nclistlength(src);i++) {
	NCjson* value = nclistget(src,i);
	if((stat = NCZ_convert1(value, typeid, dst)))
	    goto done;
    }

done:
    return stat;
}

/*
Extract type and data for an attribute
*/
static int
computeattrinfo(NC_ATT_INFO_T* att, NCjson* jtypes, NCjson* values)
{
    int stat = NC_NOERR;
    int i;
    const NCjson* jtype = NULL;
    NClist* tmparray1 = NULL;
    NClist* data = NULL;
    size_t typelen;

    /* Get type info for the given att */
    for(i=0;i<nclistlength(jtypes->dict);i+=2) {
	const NCjson* aname = nclistget(jtypes->dict,i);
	const NCjson* atype = nclistget(jtypes->dict,i+1);
	if(strcmp(aname->value,att->hdr.name)==0) {
	    jtype = atype;
	    break;
	}
    }
    if(jtype == NULL) {stat = NC_EINTERNAL; goto done;}
    /* Convert jtype to an nc_type (from nc4type.c) */
    att->nc_typeid = zname2type(jtype->value);
    if(att->nc_typeid == NC_NAT) {stat = NC_EINTERNAL; goto done;}
    /* Now, collect the length of the attribute */
    switch (values->sort) {
    case NCJ_DICT: stat = NC_EINTERNAL; goto done;
    case NCJ_ARRAY:
	att->len = nclistlength(values->array);
	data = values->array;
	break;
    default:
	att->len = 1;
	tmparray1 = nclistnew();
	nclistpush(tmparray1,values); /* simulate list of length 1*/	
	data = tmparray1;
	break;
    }
    /* Allocate data space */
    if((stat = NC4_inq_atomic_type(att->nc_typeid, NULL, &typelen)))
	goto done;
    if((att->data = malloc(typelen*att->len)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    /* convert to target type */        
    if((stat = zconvert(att->nc_typeid, typelen, att->data, data)))
	goto done;

done:
    if(tmparray1) nclistfree(tmparray1);
    return stat;
}


/**
 * @internal Read file data from map to memory.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_read_file(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCjson* json = NULL;
    NCjson* jvalue = NULL;

    LOG((3, "%s: file: %s", __func__, file->controller->path));

    zinfo = file->format_file_info;

    /* Read the .zarr dict */
    if((stat = NCJnew(NCJ_DICT,&json)))
	goto done;
    /* Extract the relevant info */
    if((stat = NCJdictget(json,"zarr_format",&jvalue)))
	goto done;
    if(jvalue != NULL) {
        sscanf(jvalue->value,"%d",&zinfo->zarr.zarr_version);
    } else
	zinfo->zarr.zarr_version = ZARRVERSION;

    if((stat = NCJdictget(json,"nczarr_format",&jvalue)))
	goto done;
    else {
	const char* v = NCZ_ZARR_VERSION;
        if(jvalue != NULL)
	    v = jvalue->value;	    
        sscanf(v,"%lu.%lu.%lu",
		&zinfo->zarr.nczarr_version.major,
		&zinfo->zarr.nczarr_version.minor,
		&zinfo->zarr.nczarr_version.release);
    } 

    /* Now load the groups starting with root */
    if((stat = define_grp(file,file->root_grp)))
	goto done;

done:
    NCJreclaim(json);
    return stat;
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
define_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    NCjson* jcontent = NULL;
    NClist* dimdefs = nclistnew();
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();

    LOG((3, "%s: dims: %s", __func__, key));

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Construct grp path */
    if((stat = NCZ_grppath(grp,&fullpath)))
	goto done;

    /* build zcontent path */
    if((stat = nczm_suffix(fullpath,NCZCONTENT,&key)))
	goto done;
    /* Read nczcontent */
    switch (stat=NCZ_downloadjson(map,key,&jcontent)) {
    case NC_NOERR: break; /* we read it */
    case NC_EACCESS: /* probably pure zarr, so does not exist, use search */
	nclistclear(varnames);
	if((stat = searchvars(file,grp,varnames)))
	nclistclear(subgrps);
	if((stat = searchsubgrps(file,grp,subgrps)))
	break;
    default: goto done;
    }
    nullfree(key); key = NULL;

    /* Pull out lists about group content */
    if((stat = parse_group_content(jcontent,dimdefs,varnames,subgrps)))
	goto done;

    /* Define dimensions */
    if((stat = define_dims(file,grp,dimdefs))) goto done;

    /* Define vars */
    if((stat = define_vars(file,grp,varnames))) goto done;

    /* Define sub-groups */
    if((stat = define_subgrps(file,grp,subgrps))) goto done;

done:
    NCJreclaim(json);
    NCJreclaim(jcontent);
    nclistfreeall(dimdefs);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    nullfree(fullpath);
    nullfree(key);
    return stat;
}

/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects.
@param zfile - [in] the containing file (annotation)
@param container - [in] the containing object
@return NC_NOERR
@author Dennis Heimbigner
*/
int
ncz_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container)
{
    int stat = NC_NOERR;
    int i;
    char* fullpath = NULL;
    char* key = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCZMAP* map = zinfo->map;
    NC_ATT_INFO_T* att = NULL;
    NCZ_ATT_INFO_T* zatt = NULL;
    NCindex* attlist = NULL;
    NCjson* jattrs = NULL;
    NCjson* jtypes = NULL;

    zinfo = file->format_file_info;

    if(container->sort == NCGRP)
	attlist = ((NC_GRP_INFO_T*)container)->att;
    else
	attlist = ((NC_VAR_INFO_T*)container)->att;

    if((stat = load_jatts(map, container, &jattrs, &jtypes)))
	goto done;

    /* Iterate over the attributes to create the in-memory attributes */
    for(i=0;i<nclistlength(jattrs->dict);i+=2) {
	NCjson* key = nclistget(jattrs->dict,i);
	NCjson* value = nclistget(jattrs->dict,i+1);
	/* Create the attribute */
        if((stat=nc4_att_list_add(attlist,key->value,&att)))
	    goto done;
        if((zatt = calloc(1,sizeof(NCZ_ATT_INFO_T))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	att->container = container;
	att->format_att_info = zatt;
	/* Fill in the attribute's type and value  */
	if((stat = computeattrinfo(att,jtypes,value)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
        ((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
        ((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    NCJreclaim(jattrs);
    NCJreclaim(jtypes);
    nullfree(fullpath);
    nullfree(key);
    return stat;
}

/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param diminfo List of (name,length) pairs
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo)
{
    int i,stat = NC_NOERR;

    /* Reify each dim in turn */
    for(i = 0; i < nclistlength(diminfo); i+=2) {
	NC_DIM_INFO_T* dim = NULL;
	off64_t len = 0;
        const char* name = nclistget(diminfo,i);
        const char* value = nclistget(diminfo,i+1);

	/* Create the NC_DIM_INFO_T object */
	sscanf(value,"%lld",&len); /* Get length */
	if(len < 0) {stat = NC_EDIMSIZE; goto done;}
        if((stat = nc4_dim_list_add(grp, name, (size_t)len, -1, &dim)))
	    goto done;
        if((dim->format_dim_info = calloc(1,sizeof(NCZ_DIM_INFO_T))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
    }

done:
    return stat;
}

/**
 * @internal Materialize vars into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varnames List of names of variables in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    int i;
    char* fullpath = NULL;
    char* key = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCZMAP* map = NULL;
    NClist* segments = nclistnew();
    NCjson* jvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jvalue = NULL;
    NClist* dimrefs = nclistnew();
    int nodimrefs = 0;

    zinfo = file->format_file_info;
    map = zinfo->map;

    /* Construct grp path */
    if((stat = NCZ_grppath(grp,&fullpath)))
	goto done;

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	int j;
	NC_VAR_INFO_T* var;
        const char* varname = nclistget(varnames,i);

	/* Clean up from last cycle */
	for(j=0;j<nclistlength(dimrefs);j++)
	    nullfree((char*)nclistget(dimrefs,j));

	/* Create the NC_VAR_INFO_T object */
        if((stat = nc4_var_list_add2(grp, varname, &var)))
	    goto done;

        /* And its annotation */
	if((var->format_var_info = calloc(1,sizeof(NCZ_VAR_INFO_T)))==NULL)
	    {stat = NC_ENOMEM; goto done;}

	/* Construct the path to the zarray object */
        if((stat = nczm_suffix(fullpath,ZARRAY,&key)))
	    goto done;
	/* Download the zarray object */
	if((stat=NCZ_readdict(map,key,&jvar)))
	    goto done;
        nullfree(key); key = NULL;
        assert((jvar->sort = NCJ_DICT));

	/* Construct the path to the zdimrefs object */
        if((stat = nczm_suffix(fullpath,ZDIMREFS,&key)))
	    goto done;
	/* Download the zdimrefs object */
	switch (stat=NCZ_readdict(map,key,&jdimrefs)) {
	case NC_NOERR: /* Extract the dimref names */
	    assert((jdimrefs->sort = NCJ_ARRAY));
	    for(i=0;i<nclistlength(jdimrefs->array);i++) {
	        const char* dimpath = nclistget(jdimrefs->array,i);
		nclistpush(dimrefs,strdup(dimpath));
	    }
	    nodimrefs = 0;
	    break;
	case NC_EACCESS: /* simulate it from the shape of the variable */
	    nodimrefs = 1;
	    break;
	default: goto done;
 	}
        nullfree(key); key = NULL;

	/* Use jvar to set up the rest of the NC_VAR_INFO_T object */
	/* Verify the format */
	{
	    int version;
	    if((stat = NCJdictget(jvar,"zarr_format",&jvalue))) goto done;
	    sscanf(jvalue->value,"%d",&version);
	    if(version != zinfo->zarr.zarr_version)
		{stat = NC_EZARR; goto done;}
	}
	/* Set the type and endianness of the variable */
	{
	    nc_type vtype;
	    int endianness;
	    if((stat = NCJdictget(jvar,"dtype",&jvalue))) goto done;
	    /* Convert dtype to nc_type + endianness */
	    if((stat = NCZ_dtype2typeinfo(jvalue->value,&vtype,&endianness)))
		goto done;
	    if(vtype > NC_NAT && vtype < NC_STRING) {
	        /* Locate the NC_TYPE_INFO_T object */
		if((stat = ncz_gettype(vtype,&var->type_info)))
		    goto done;
	    } else {stat = NC_EBADTYPE; goto done;}
	    if(endianness == NC_ENDIAN_LITTLE || endianness == NC_ENDIAN_BIG) {
		var->endianness = endianness;
	    } else {stat = NC_EBADTYPE; goto done;}
	    var->type_info->endianness = var->endianness; /* Propagate */
        }
	/* shape */
	{
	    int rank;
	    if((stat = NCJdictget(jvar,"shape",&jvalue))) goto done;
	    if(jvalue->sort != NCJ_ARRAY) {stat = NC_EZARR; goto done;}
	    /* Verify the rank */
	    rank = nclistlength(jvalue->array);
	    if(!nodimrefs) { /* verify rank consistency */
		if(nclistlength(dimrefs) != rank)
		    {stat = NC_EZARR; goto done;}
	    }
	    /* Set the rank of the variable */
            if((stat = nc4_var_set_ndims(var, rank))) goto done;
	    if(nodimrefs) {
		if((stat = parsedimrefs(file,dimrefs, var->dim)))
		    goto done;
	    } else { /* simulate the dimrefs */
		size64_t shapes[NC_MAX_VAR_DIMS];
		if((stat = decodeints(jvalue, shapes))) goto done;
		if((stat = simulatedimrefs(file, rank, shapes, var->dim)))
		    goto done;
	    }
	    /* fill in the dimids */
	    for(j=0;j<rank;j++)
		var->dimids[j] = var->dim[j]->hdr.id;
	}
	/* chunks */
	{
	    int rank;
	    size64_t chunks[NC_MAX_VAR_DIMS];
	    if((stat = NCJdictget(jvar,"chunks",&jvalue))) goto done;
	    if(jvalue->sort != NCJ_ARRAY) {stat = NC_EZARR; goto done;}
	    /* Verify the rank */
	    rank = nclistlength(jvalue->array);
	    if(var->ndims != rank)
	        {stat = NC_EZARR; goto done;}
  	    if((stat = decodeints(jvalue, chunks))) goto done;
	    /* validate the chunk sizes */
	    for(j=0;j<rank;j++) {
		NC_DIM_INFO_T* d = var->dim[j]; /* matching dim */
	        if(chunks[j] == 0 || chunks[j] > d->len)
		    {stat = NC_EZARR; goto done;}
		var->chunksizes[j] = (size_t)chunks[j];
	    }
	}
	/* fill_value */
	{
	    if((stat = NCJdictget(jvar,"fill_value",&jvalue))) goto done;
	    /* ignore */
	}
	/* Capture row vs column major; currently, column major not used*/
	{
	    if((stat = NCJdictget(jvar,"order",&jvalue))) goto done;
	    if(strcmp(jvalue->value,"C")==1)
		((NCZ_VAR_INFO_T*)var->format_var_info)->order = 1;
	    else ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 0;
	}
	/* compressor ignored */
	{
	    if((stat = NCJdictget(jvar,"compressor",&jvalue))) goto done;
	    /* ignore */
	}
	/* filters ignored */
	{
	    if((stat = NCJdictget(jvar,"filters",&jvalue))) goto done;
	    /* ignore */
	}
    }
done:
    nullfree(fullpath);
    nullfree(key);
    nclistfreeall(segments);
    nclistfreeall(dimrefs);
    NCJreclaim(jvar);
    NCJreclaim(jdimrefs);
    return stat;
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
define_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    int i,stat = NC_NOERR;

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
    }

    /* Recurse to fill in subgroups */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = define_grp(file,g)))
	    goto done;
    }

done:
    return stat;
}

/**************************************************/
/* Utilities */

static int
zname2type(const char* tname)
{
    int i, stat = NC_NOERR;
    char name[NC_MAX_NAME];

    for(i=0;i<NUM_ATOMIC_TYPES;i++) {
	if((stat = NC4_inq_type(0,i,name,NULL)))
	    break;
	if(strcasecmp(name,tname)==0)
	    return i;
    }
    return NC_NAT;
}

static int
parse_group_content(NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps)
{
    int i,stat = NC_NOERR;
    NCjson* jvalue = NULL;

    if((stat=NCJdictget(jcontent,"dims",&jvalue))) goto done;
    if(jvalue->sort != NCJ_DICT) {stat = NC_EZARR; goto done;}

    /* Extract the dimensions defined in this group */
    for(i=0;i<nclistlength(jvalue->dict);i+=2) {
	NCjson* jname = nclistget(jvalue->dict,i);
	NCjson* jlen = nclistget(jvalue->dict,i+1);
        char norm_name[NC_MAX_NAME + 1];
	off64_t len;
	/* Verify name legality */
        if((stat = nc4_check_name(jname->value, norm_name)))
	    {stat = NC_EBADNAME; goto done;}
	/* check the length */
	sscanf(jlen->value,"%lld",&len);
	if(len <= 0)
	    {stat = NC_EDIMSIZE; goto done;}		
	nclistpush(dimdefs,strdup(norm_name));
	nclistpush(dimdefs,jlen->value);
    }

    /* Extract the variable names in this group */
    if((stat=NCJdictget(jcontent,"vars",&jvalue))) goto done;
    for(i=0;i<nclistlength(jvalue->array);i++) {
	NCjson* jname = nclistget(jvalue->dict,i);
        char norm_name[NC_MAX_NAME + 1];
	/* Verify name legality */
        if((stat = nc4_check_name(jname->value, norm_name)))
	    {stat = NC_EBADNAME; goto done;}
	nclistpush(varnames,strdup(norm_name));
    }

    /* Extract the subgroup names in this group */
    if((stat=NCJdictget(jcontent,"groups",&jvalue))) goto done;
    for(i=0;i<nclistlength(jvalue->array);i++) {
	NCjson* jname = nclistget(jvalue->dict,i);
        char norm_name[NC_MAX_NAME + 1];
	/* Verify name legality */
        if((stat = nc4_check_name(jname->value, norm_name)))
	    {stat = NC_EBADNAME; goto done;}
	nclistpush(subgrps,strdup(norm_name));
    }

done:
    return stat;
}

static int
searchvars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    return NC_NOERR;
}

static int
searchsubgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    return NC_NOERR;
}

/* Convert a list of integer strings to integers */
static int
decodeints(NCjson* jshape, size64_t* shapes)
{
    int i, stat = NC_NOERR;

    for(i=0;i<nclistlength(jshape->array);i++) {
	long long v;
	NCjson* jv = nclistget(jshape->array,i);
	if((stat = NCZ_convert1(jv,NC_INT64,(char*)&v))) goto done;
	if(v < 0) {stat = NC_EZARR; goto done;}
	shapes[i] = (size64_t)v;
    }

done:
    return stat;
}

static int
simulatedimrefs(NC_FILE_INFO_T* file, int rank, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int i, j, stat = NC_NOERR;
    NC_GRP_INFO_T* root = file->root_grp;
    NC_DIM_INFO_T* thed = NULL;
    int match = 0;

    for(i=0;i<rank;i++) {
	size64_t dimlen = shapes[i];
	char shapename[NC_MAX_NAME];
	match = 0;
	/* See if there is a dimension named "_dim<dimlen>", if not create */
	snprintf(shapename,sizeof(shapename),"_dim%llu",dimlen);
	for(j=0;j<ncindexsize(root->dim);j++) {
	    thed = (NC_DIM_INFO_T*)ncindexith(root->dim,j);
	    if(strcmp(thed->hdr.name,shapename)==0) {
		if(dimlen != (size64_t)thed->len)
		    {stat = NC_EZARR; goto done;} /* we have a problem */
		match = 1;
		break;
	    }
	}
	if(!match) { /* create the dimension */
	    if((stat = nc4_dim_list_add(root,shapename,(size_t)dimlen, -1, &thed)))
		goto done;
	}
	/* Save the id */
	dims[i] = thed;
    }

done:
    return stat;
}

/*
Given a list of segments, find corresponding group.
*/
static int
locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp)
{
    int i, found, stat = NC_NOERR;
    NC_GRP_INFO_T* grp = NULL;

    grp = file->root_grp;
    for(i=0;i<nsegs;i++) {
	int j;
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
    return stat;
}

static int
parsedimrefs(NC_FILE_INFO_T* file, NClist* dimrefs, NC_DIM_INFO_T** dims)
{
    int i, stat = NC_NOERR;
    NClist* segments = nclistnew();

    for(i=0;i<nclistlength(dimrefs);i++) {
	NC_GRP_INFO_T* g = NULL;
	NC_DIM_INFO_T* d = NULL;
	int j;
	const char* dimpath = nclistget(dimrefs,i);
	const char* dimname = NULL;
	/* Locate the corresponding NC_DIM_INFO_T* object */
	/* Clear the list */
	for(j=0;j<nclistlength(segments);j++)
	    nullfree((char*)nclistget(segments,j));
	if((stat = ncz_splitpath(dimpath,segments)))
	    goto done;
	if((stat=locategroup(file,nclistlength(segments)-1,segments,&g)))
	    goto done;
	/* Lookup the dimension */
	dimname = nclistget(segments,nclistlength(segments)-1);
	d = NULL;
	for(j=0;j<ncindexsize(g->dim);j++) {
	    d = (NC_DIM_INFO_T*)ncindexith(g->dim,j);
	    if(strcmp(d->hdr.name,dimname)==0) {
		dims[i] = d;/* match */
		break;
	    }
        }
    }
done:
    nclistfreeall(segments);
    return stat;
}

/**
 * @internal Get the metadata for a variable.
 *
 * @param var Pointer to var info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR HDF5 returned error.
 * @return ::NC_EVARMETA Error with var metadata.
 * @author Ed Hartnett
 */
int
ncz_get_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int retval = NC_NOERR;

    assert(file && var && var->format_var_info);
    LOG((3, "%s: var %s", __func__, var->hdr.name));

    /* Have we already read the var metadata? */
    if (var->meta_read)
        return NC_NOERR;

#ifdef LOOK
    /* Get the current chunk cache settings. */
    if ((access_pid = H5Dget_access_plist(hdf5_var->hdf_datasetid)) < 0)
        BAIL(NC_EVARMETA);

    /* Learn about current chunk cache settings. */
    if ((H5Pget_chunk_cache(access_pid, &(var->chunk_cache_nelems),
                            &(var->chunk_cache_size), &rdcc_w0)) < 0)
        BAIL(NC_EHDFERR);
    var->chunk_cache_preemption = rdcc_w0;

    /* Get the dataset creation properties. */
    if ((propid = H5Dget_create_plist(hdf5_var->hdf_datasetid)) < 0)
        BAIL(NC_EHDFERR);

    /* Get var chunking info. */
    if ((retval = get_chunking_info(propid, var)))
        BAIL(retval);

    /* Get filter info for a var. */
    if ((retval = get_filter_info(propid, var)))
        BAIL(retval);

    /* Get fill value, if defined. */
    if ((retval = get_fill_info(propid, var)))
        BAIL(retval);

    /* Is this a deflated variable with a chunksize greater than the
     * current cache size? */
    if ((retval = nc4_adjust_var_cache(var->container, var)))
        BAIL(retval);

    if (var->coords_read && !var->dimscale)
        if ((retval = get_attached_info(var, hdf5_var, var->ndims, hdf5_var->hdf_datasetid)))
            return retval;
#endif

    /* Remember that we have read the metadata for this var. */
    var->meta_read = NC_TRUE;

    return retval;
}


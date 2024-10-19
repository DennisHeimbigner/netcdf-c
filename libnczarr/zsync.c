/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/


#include "zincludes.h"
#include "zfilter.h"
#include "znc4.h"

#ifndef nulldup
#define nulldup(x) ((x)?strdup(x):(x))
#endif

#undef FILLONCLOSE

/*mnemonics*/
#define DICTOPEN '{'
#define DICTCLOSE '}'

/* Forward */
static int ncz_encode_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, int isclose);
static int ncz_encode_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose);
static int ncz_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose);
static int ncz_flush_var(NC_VAR_INFO_T* var);
static int ncz_decode_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* subgrpnames);
static int ncz_decode_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj0);
static int ncz_decode_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* varname);
static int ncz_decode_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* varnames);
static int ncz_decode_atts(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, const NCjson* jnczarr);
static int get_group_content_pure(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps);

/**************************************************/
/* Synchronize functions to make map and memory
be consistent. There are two sets of functions,
1) _encode_ - push nc4internal.h structures to storage
2) _decode_ - pull storage structures and create corresponding nc4internal.h structures.
            These functions rely on the format specific decode functions to extract
	    and return the relevant info from the format json.
*/
/**************************************************/

/*
 * @internal Push nc4internal.h structures to storage
 *
 * @param file Pointer to file info struct.
 * @param isclose 1 => we closing file as opposed to sync'ing it.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_encode_file(NC_FILE_INFO_T* file, int isclose)
{
    int stat = NC_NOERR;

    NC_UNUSED(isclose);

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s isclose=%d",file->controller->path,isclose);

    /* Write out root group recursively */
    if((stat = ncz_encode_grp(file, file->root_grp, isclose)))
        goto done;

done:
#if 0
    NCJreclaim(json);
#endif
    return ZUNTRACE(stat);
}

/**
 * @internal Recursively synchronize group from memory to map.
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_encode_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, int isclose)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int purezarr = 0;
    char* key = NULL;
    NCjson* jgroup = NULL;
    NCjson* jnczgrp = NULL;
    NCjson* jsuper = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    struct ZOBJ zobj;

    LOG((3, "%s: dims: %s", __func__, key));
    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    zinfo = file->format_file_info;

    TESTPUREZARR;

    if(!purezarr) {
        if(grp->parent == NULL) { /* Root group */
	    if((stat=NCZF_encode_superblock(file,grp,&jsuper))) goto done;
	}
        /* encode _nczarr_group */
	if((stat=NCZF_encode_nczarr_group(file,grp,&jnczgrp))) goto done;
    }

    /* Assemble JSON'ized attributes */
    if((stat = NCZF_encode_attributes(file,(NC_OBJ*)grp,jnczgrp,&jatts,&jtypes))) goto done;

    /* Assemble group JSON object */
    /* Watch out &jatts is passed so that it can be NULL'd if consumed */
    if((stat=NCZF_encode_group(file,grp,jsuper,jnczgrp,&jatts,&jgroup))) goto done;

    /* upload group json and (depending on version) the group attributes */
    zobj.jobj = jgroup;
    zobj.jatts = jatts;
    if((stat = NCZF_upload_grp(file,grp,&zobj))) goto done;

    /* encode and upload the vars in this group and sync the data */
    for(i=0;i<ncindexsize(grp->vars);i++) {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	if((stat = ncz_encode_var(file,var,isclose))) goto done;
    }
    
    /* encode and upload the sub-groups in this group */
    for(i=0;i<ncindexsize(grp->children);i++) {
        NC_GRP_INFO_T* subgrp = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = ncz_encode_grp(file,subgrp,isclose))) goto done;
    }
    
done:
#if 0
    NCJreclaim(jsuper);
    NCJreclaim(jgroup);
    NCJreclaim(jdims);
    NCJreclaim(jvars);
    NCJreclaim(jsubgrp);
    NCJreclaim(jnczgrp);
    NCJreclaim(jtypes);
    NCJreclaim(jatts);
    nullfree(fullpath);
    nullfree(key);
#endif
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
ncz_encode_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCjson* jvar = NULL;
    NCjson* jatts = NULL;
    NCjson* jnczvar = NULL;
    NCjson* jtypes = NULL;
    int purezarr = 0;
    NCZ_VAR_INFO_T* zvar = var->format_var_info;
    NClist* jfilters = nclistnew();
    struct ZOBJ zobj;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    zinfo = file->format_file_info;

    TESTPUREZARR;

    /* Make sure that everything is established */
    /* ensure the fill value */
    if((stat = NCZ_ensure_fill_value(var))) goto done; /* ensure var->fill_value is set */
    assert(var->no_fill || var->fill_value != NULL);
    /* ensure the chunk cache */
    if((stat = NCZ_adjust_var_cache(var))) goto done;
    /* rebuild the fill chunk */
    if((stat = NCZ_ensure_fill_chunk(zvar->cache))) goto done;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Build the filter working parameters for any filters */
    if((stat = NCZ_filter_setup(var))) goto done;
#endif

    /* There is a sort of cycle between _nczarr_array and the attributes in that
       the attributes must contain _nczarr_array as an attribute and _nczar_array
       must contain the attribute types (including _nczarr_array).
       We break this by building _nczarr_array first, then building the attributes.
    */     
    /* Build the _nczarr_array object */
    if(!purezarr) {
        if((stat=NCZF_encode_nczarr_array(file,var,&jnczvar))) goto done;
    }
    if((stat=NCZF_encode_attributes(file,(NC_OBJ*)var,jnczvar,&jatts,&jtypes))) goto done;

#if 0
    if(!purezarr && jnczvar != NULL) {
        /* Insert _nczarr_array */
        if((stat=ncz_insert_attr(jatts,jtypes,NCZ_ARRAY,jnczvar,"|J0"))) goto done;
	jnczvar = NULL;
    }

    /* As a last mod to jatts, optionally insert the jtypes as an attribute and create and add _nczarr_attr */
    if(!purezarr && jtypes != NULL) {
	if((stat = insert_nczarr_attr(jatts,jtypes))) goto done;
	jtypes = NULL;
    }
#endif

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Encode the filters */
#endif

    /* encode the var JSON including (optionally) the attributes */
    if((stat=NCZF_encode_var(file,var,jnczvar,&jatts,jfilters,&jvar))) goto done;

    /* Write out the the var JSON and the corresponding attributes and chunks */
    zobj.jobj = jvar;
    zobj.jatts = jatts;
    if((stat = NCZF_upload_var(file,var,&zobj))) goto done;

    var->created = 1;

done:
#if 0
    nclistfreeall(dimrefs);
    nullfree(fullpath);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jnczvar);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
#endif
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
ncz_encode_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    if(isclose) {
	if((stat = ncz_encode_var_meta(file,var,isclose))) goto done;
    }

    /* flush chunks */
    if((stat = ncz_flush_var(var))) goto done;

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}

/*
Flush all modified chunks to disk. Create any that are missing
and fill as needed.
*/
static int
ncz_flush_var(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    ZTRACE(3,"var=%s",var->hdr.name);

    /* Flush the cache */
    if(zvar->cache) {
        if((stat = NCZ_flush_chunk_cache(zvar->cache))) goto done;
    }

#ifdef FILLONCLOSE
    /* If fill is enabled, then create missing chunks */
    if(!var->no_fill) {
        int i;
    NCZOdometer* chunkodom =  NULL;
    NC_FILE_INFO_T* file = var->container->nc4_info;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZMAP* map = zfile->map;
    size64_t start[NC_MAX_VAR_DIMS];
    size64_t stop[NC_MAX_VAR_DIMS];
    size64_t stride[NC_MAX_VAR_DIMS];
    char* key = NULL;

    if(var->ndims == 0) { /* scalar */
	start[i] = 0;
	stop[i] = 1;
        stride[i] = 1;
    } else {
        for(i=0;i<var->ndims;i++) {
	    size64_t nchunks = ceildiv(var->dim[i]->len,var->chunksizes[i]);
	    start[i] = 0;
	    stop[i] = nchunks;
	    stride[i] = 1;
        }
    }

    {
	if(zvar->scalar) {
	    if((chunkodom = nczodom_new(1,start,stop,stride,stop))==NULL)
	} else {
	    /* Iterate over all the chunks to create missing ones */
	    if((chunkodom = nczodom_new(var->ndims,start,stop,stride,stop))==NULL)
	        {stat = NC_ENOMEM; goto done;}
	}
	for(;nczodom_more(chunkodom);nczodom_next(chunkodom)) {
	    size64_t* indices = nczodom_indices(chunkodom);
	    /* Convert to key */
	    if((stat = NCZ_buildchunkpath(zvar->cache,indices,&key))) goto done;
	    switch (stat = nczmap_exists(map,key)) {
	    case NC_NOERR: goto next; /* already exists */
	    case NC_EEMPTY: break; /* does not exist, create it with fill */
	    default: goto done; /* some other error */
	    }
            /* If we reach here, then chunk does not exist, create it with fill */
	    assert(zvar->cache->fillchunk != NULL);
	    if((stat=nczmap_write(map,key,0,zvar->cache->chunksize,zvar->cache->fillchunk))) goto done;
next:
	    nullfree(key);
	    key = NULL;
	}
    }
    nczodom_free(chunkodom);
    nullfree(key);
    }
#endif /*FILLONCLOSE*/

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/*
 * @internal pull storage structures and create corresponding nc4internal.h structures
 */

/**

 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_decode_file(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NC_GRP_INFO_T* root = NULL;
    struct ZOBJ zobj = emptyzobj;
    const NCjson* jsuper = NULL;
    const NCjson* jnczgrp = NULL;
    NClist* varnames = nclistnew();
    NClist* subgroupnames = nclistnew();
    int zarr_format = 0;
    int nczarr_format = 0;

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s",file->controller->path);
    
    /* Download the root group object and associated attributes  */
    root = file->root_grp;
    if((stat = NCZF_download_grp(file, root, &zobj))) goto done;

    /* Decode the root group */
    if((stat = NCZF_decode_group(file,root,&zobj,(NCjson* const*)&jnczgrp,(NCjson* const*)&jsuper))) goto done;

    /* Ok, process superblock */
    if((stat = NCZF_decode_superblock(file,root,jsuper,&zarr_format,&nczarr_format))) goto done;

    /* Decode the _nczarr_group; also creates the dimension decls */
    if((stat = NCZF_decode_nczarr_group(file,root,jnczgrp,varnames,subgroupnames))) goto done;

    /* Fill in the root object */
    if((stat = ncz_decode_grp(file,root,&zobj))) goto done;

    /* Create and fill the subgroups for this group */
    if((stat = ncz_decode_subgrps(file,root,subgroupnames))) goto done;

done:
#if 0
    NCZ_clear_zobj(&zobj);
#endif
    return ZUNTRACE(THROW(stat));
}

static int
ncz_decode_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* subgrpnames)
{
    int stat = NC_NOERR;
    size_t i;
    struct ZOBJ zobj = emptyzobj;

    ZTRACE(3,"file=%s parent=%s |subgrpnames|=%u",file->controller->path,parent->hdr.name,nclistlength(subgrpnames));

    /* Create and load each subgrp in turn */
    for(i = 0; i < nclistlength(subgrpnames); i++) {
	const char* subgrpname = (const char*)nclistget(subgrpnames,i);
	NC_GRP_INFO_T* subgrp = NULL;
	/* Create the group object */
	if((stat=ncz4_create_grp(file,parent,subgrpname,&subgrp))) goto done;
	/* Download the group's metadata */
	if((stat = NCZF_download_grp(file,subgrp,&zobj))) goto done;
	/* Fill in the group object */
        if((stat = ncz_decode_grp(file,subgrp,&zobj))) goto done;
    }

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Read group data from storage
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 * @param zobj the grp|atts for this grp; may be NULL
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_decode_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZOBJ* zobj0)
{
    int stat = NC_NOERR;
    struct ZOBJ zobj = emptyzobj;
    NCZ_FILE_INFO_T* zinfo = file->format_file_info;
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();
    int purezarr = 0;
    const NCjson* jnczgrp = NULL;
    const NCjson* jsuper = NULL;

    ZTRACE(3,"file=%s parent=%s",file->controller->path,(parent?parent->hdr.name:"NULL"));

    TESTPUREZARR;

    /* Decode the group metadata */
    if((stat = NCZF_decode_group(file,grp,&zobj,&jnczgrp,&jsuper))) goto done;
    if(zobj.jobj == NULL) {stat = NC_ENOTZARR; goto done;}
	
    if(purezarr) {
	if((stat = get_group_content_pure(file,grp,varnames,subgrps))) goto done;
    } else { /*!purezarr*/
        /* Decode the _nczarr_group; also creates the dimension decls */
        if((stat = NCZF_decode_nczarr_group(file,grp,jnczgrp,varnames,subgrps))) goto done;
    }

    /* Process attributes */
    if((stat=ncz_decode_atts(file,(NC_OBJ*)grp,zobj.jatts,jnczgrp))) goto done;

    if(nclistlength(subgrps) > 0) {
	/* Define sub-groups */
	if((stat = ncz_decode_subgrps(file,grp,subgrps))) goto done;
    }

    if(nclistlength(varnames) > 0) {
	/* Define vars taking xarray into account */
	if((stat = ncz_decode_vars(file,grp,varnames))) goto done;
    }
    
done:
#if 0
    NCZ_reclaim_diminfo_list(dims);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    if(zobj ==  &zobj0)
        NCZ_clear_zobj(&zobj0);
#endif
    return ZUNTRACE(THROW(stat));
}

#if 0
/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param parent Pointer to parent grp info struct.
 * @param diminfo vector of struct NCZ_DimInfo*
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_decode_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* diminfo)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(3,"file=%s parent=%s |diminfo|=%u",file->controller->path,parent->hdr.name,nclistlength(diminfo));

    for(i=0;i<nclistlength(diminfo);i++) {
	struct NCZ_DimInfo* dim = (struct NCZ_DimInfo*)nclistget(diminfo,i);
	if((stat = ncz4_create_dim(file,parent,dim->name,dim->shape,dim->unlimited,NULL))) goto done;
    }

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}
#endif /*0*/

/**
 * @internal Materialize single var into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param parent Pointer to parent grp info struct.
 * @param varname name of variable in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_decode_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* varname)
{
    int stat = NC_NOERR;
    NC_VAR_INFO_T* var = NULL;
    struct ZOBJ zobj = emptyzobj;
    const NCjson* jnczvar = NULL;
    NClist* jfilters = nclistnew();
    NClist* dimrefs = nclistnew();
    NClist* dimdecls = nclistnew();
    size64_t* shapes = NULL;
    size64_t* chunks = NULL;
    size_t i;
    NCbytes* fqn = ncbytesnew();
    NCZ_VAR_INFO_T* zvar = NULL;

    ZTRACE(3,"file=%s parent=%s varname=%s",file->controller->path,parent->hdr.name,varname);

    /* Create and Download */
    if((stat = ncz4_create_var(file,parent,varname,&var))) goto done;
    zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
    if((stat = NCZF_download_var(file,var,&zobj))) goto done;
    if((stat=NCZF_decode_var(file,var,&zobj,jfilters,&shapes,&chunks,dimrefs))) goto done;

    /* Convert dimrefs to corresponding dimdecls */
    for(i=0;i<nclistlength(dimrefs);i++) {
	NC_OBJ* obj = NULL;
	const char* dimref = (const char*)nclistget(dimrefs,i);
	if(dimref == NULL || strlen(dimref) == 0) {stat = NC_ENOTZARR; goto done;}
	if(dimref[0] == '/') { /* Absolute path */
	    if((stat = NCZ_locateFQN(file->root_grp, dimref, NCDIM, &obj, NULL))) goto done;
	    if(obj == NULL || obj->sort != NCDIM) {stat = NC_ENOTZARR; goto done;}
	    nclistpush(dimdecls,obj);
	} else { /* relative to root group */
	    if((stat = NCZ_makeFQN(file->root_grp,dimref,fqn))) goto done;
	    if((stat = NCZ_locateFQN(file->root_grp, ncbytescontents(fqn), NCDIM, &obj, NULL))) goto done;
	    if(obj == NULL || obj->sort != NCDIM) {
		struct NCZ_DimInfo di;
		memcpy(di.norm_name,dimref,sizeof(di.norm_name));
		di.shape = shapes[i];
		di.unlimited = 0;
		/* create the dimension in the root group */
		if((stat = ncz4_create_dim(file,file->root_grp,&di,(NC_DIM_INFO_T**)&obj))) goto done;
	    }
	    nclistpush(dimdecls,obj);
	}
    }

    /* Process chunks and shapes */
    assert(var->chunksizes == NULL);
    if(var->ndims == 0) { /* Scalar */
	var->dimids = NULL;
	var->dim = NULL;
	var->chunksizes = NULL;
	zvar->chunkproduct = 1;
	zvar->chunksize = var->type_info->size;
    } else {
	var->dimids = (int*)malloc(sizeof(int)*var->ndims);
	var->dim = (NC_DIM_INFO_T**)malloc(sizeof(NC_DIM_INFO_T*)*var->ndims);
	zvar->chunkproduct = 1;
	for(i=0;i<nclistlength(dimdecls);i++) {
	    NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)nclistget(dimdecls,i);
	    var->dim[i] = dim;
	    var->dimids[i] = dim->hdr.id;
	    var->chunksizes[i] = (size_t)chunks[i];
	    zvar->chunkproduct *= shapes[i];
	}
        zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	/* Create the cache */
	if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
    }

    /* Process attributes */
    if((stat=ncz_decode_atts(file,(NC_OBJ*)var,zobj.jatts,jnczvar))) goto done;

    /* Process filters */
    if((stat = NCZ_filters_decode(file,var,jfilters))) goto done;

done:
#if 0
    NCZ_clear_zobj(&zobj);
#endif
    return THROW(stat);
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
ncz_decode_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* varnames)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	const char* varname = (const char*)nclistget(varnames,i);
        if((stat = ncz_decode_var1(file,parent,varname))) goto done;
    }

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}

/**************************************************/

/**************************************************/
/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
@param file - [in] the containing file
@param container - [in] the containing object (group|var)
@return ::NC_NOERR
@author Dennis Heimbigner
*/
static int
ncz_decode_atts(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson* jatts, const NCjson* jnczvar)
{
    int stat = NC_NOERR;
    NC_ATT_INFO_T* fillvalueatt = NULL;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    if(jatts != NULL) {
        if((stat = NCZF_decode_attributes(file,container,jnczvar,jatts))) goto done;
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue(file,(NC_VAR_INFO_T*)container)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
	((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
	((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Utilities */

/**
Return the list of var names and subgrp names that are
immediately below the specified group.
*/
static int
get_group_content_pure(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;

    ZTRACE(3,"zinfo=%s grp=%s |varnames|=%u |subgrps|=%u",zinfo->common.file->controller->path,grp->hdr.name,(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    nclistclear(varnames);
    if((stat = NCZF_searchobjects(file,grp,varnames,subgrps))) goto done;

done:
#if 0
#endif
    return ZUNTRACE(THROW(stat));
}

#if 0
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
static int
ncz_get_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int retval = NC_NOERR;

    assert(file && var && var->format_var_info);
    LOG((3, "%s: var %s", __func__, var->hdr.name));
    ZTRACE(3,"file=%s var=%s",file->controller->path,var->hdr.name);
    
    /* Have we already read the var metadata? */
    if (var->meta_read)
	goto done;

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
    if ((retval = nc4_adjust_var_cache(var)))
	BAIL(retval);

    /* Is there an attribute which means quantization was used? */
    if ((retval = get_quantize_info(var)))
	BAIL(retval);

    if (var->coords_read && !var->dimscale)
	if ((retval = get_attached_info(var, hdf5_var, var->ndims, hdf5_var->hdf_datasetid)))
	    goto done;;
#endif

    /* Remember that we have read the metadata for this var. */
    var->meta_read = NC_TRUE;
done:
#if 0
#endif
    return ZUNTRACE(retval);
}
#endif /*0*/

/**
Insert an attribute into a list of attribute, including typing
Takes control of javalue.
@param jatts
@param jtypes
@param aname
@param javalue
*/
int
ncz_insert_attr(NCjson* jatts, NCjson* jtypes, const char* aname, NCjson* javalue, const char* atype)
{
    int stat = NC_NOERR;
    if(jatts != NULL) {
	if(jtypes != NULL) {
            NCJinsertstring(jtypes,aname,atype);
	}
        NCJinsert(jatts,aname,javalue);
    }
    return THROW(stat);
}

/**************************************************/
#if 0
/* Convert an attribute "types list to an envv style list */
static int
jtypes2atypes(NCjson* jtypes, NClist* atypes)
{
    int stat = NC_NOERR;
    size_t i;
    for(i=0;i<NCJarraylength(jtypes);i+=2) {
	const NCjson* key = NCJith(jtypes,i);
	const NCjson* value = NCJith(jtypes,i+1);
	if(NCJsort(key) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	if(NCJsort(value) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	nclistpush(atypes,strdup(NCJstring(key)));
	nclistpush(atypes,strdup(NCJstring(value)));
    }
done:
#if 0
#endif
    return stat;
}
#endif

#if 0
/* See if there is reason to believe the specified path is a legitimate (NC)Zarr file
 * Do a breadth first walk of the tree starting at file path.
 * @param file to validate
 * @return ::NC_NOERR if it looks ok
 * @return ::NC_ENOTNC if it does not look ok
 */
static int
ncz_validate(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    int validate = 0;
    NCbytes* prefix = ncbytesnew();
    NClist* queue = nclistnew();
    NClist* nextlevel = nclistnew();
    NCZMAP* map = zinfo->map;
    char* path = NULL;
    char* segment = NULL;
    size_t seglen;
	    
    ZTRACE(3,"file=%s",file->controller->path);

    path = strdup("/");
    nclistpush(queue,path);
    path = NULL;
    do {
        nullfree(path); path = NULL;
	/* This should be full path key */
	path = nclistremove(queue,0); /* remove from front of queue */
	/* get list of next level segments (partial keys) */
	assert(nclistlength(nextlevel)==0);
        if((stat=nczmap_search(map,path,nextlevel))) {validate = 0; goto done;}
        /* For each s in next level, test, convert to full path, and push onto queue */
	while(nclistlength(nextlevel) > 0) {
            segment = nclistremove(nextlevel,0);
            seglen = nulllen(segment);
	    if((seglen >= 2 && memcmp(segment,".z",2)==0) || (seglen >= 4 && memcmp(segment,".ncz",4)==0)) {
		validate = 1;
	        goto done;
	     }
	     /* Convert to full path */
	     ncbytesclear(prefix);
	     ncbytescat(prefix,path);
	     if(strlen(path) > 1) ncbytescat(prefix,"/");
	     ncbytescat(prefix,segment);
	     /* push onto queue */
	     nclistpush(queue,ncbytesextract(prefix));
 	     nullfree(segment); segment = NULL;
	 }
    } while(nclistlength(queue) > 0);
done:
#if 0
    if(!validate) stat = NC_ENOTNC;
    nullfree(path);
    nullfree(segment);
    nclistfreeall(queue);
    nclistfreeall(nextlevel);
    ncbytesfree(prefix);
#endif
    return ZUNTRACE(THROW(stat));
}
#endif

#if 0
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
	NCJinsertstring(jtypes,NCZ_ATTR,"|J0"); /* type for _nczarr_attr */
        NCJnew(NCJ_DICT,&jdict);
        NCJinsert(jdict,"types",jtypes);
        NCJinsert(jatts,NCZ_ATTR,jdict);
        jdict = NULL;
    }
    return NC_NOERR;
}
#endif /*0*/

#if 0
/**
Upload a .zattrs object
Optionally take control of jatts and jtypes
@param file
@param container
@param jattsp
@param jtypesp
*/
static int
upload_attrs(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jatts)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    if(jatts == NULL) goto done;    

    zinfo = file->format_file_info;
    map = zinfo->map;

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }

    /* Construct container path */
    if(container->sort == NCGRP)
	stat = NCZ_grpkey(grp,&fullpath);
    else
	stat = NCZ_varkey(var,&fullpath);
    if(stat) goto done;

    /* write .zattrs*/
    if((stat = nczm_concat(fullpath,ZATTRS,&key))) goto done;
    if((stat=NCZ_uploadjson(map,key,jatts))) goto done;
    nullfree(key); key = NULL;

done:
#if 0
    nullfree(fullpath);
#endif
    return ZUNTRACE(THROW(stat));
}
#endif /*0*/

#if 0
/**
@internal Get contents of a meta object; fail it it does not exist
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json || NULL if not exists
@return NC_NOERR
@return NC_EXXX
@author Dennis Heimbigner
*/
static int
readarray(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    if((stat = NCZ_downloadjson(zmap,key,&json))) goto done;
    if(json != NULL && NCJsort(json) != NCJ_ARRAY) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
#if 0
    NCJreclaim(json);
#endif
    return stat;
}
#endif

#if 0
static int
downloadzarrobj(NC_FILE_INFO_T* file, struct ZARROBJ* zobj, const char* fullpath, const char* objname)
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
#if 0
    nullfree(key);
#endif
    return THROW(stat);
}
#endif /*0*/

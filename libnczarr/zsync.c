/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/


#include "zincludes.h"
#include "zfilter.h"
#include "znc4.h"

#include <stddef.h>

#ifndef nulldup
#define nulldup(x) ((x)?strdup(x):(x))
#endif

#undef FILLONCLOSE

/*mnemonics*/
#define DICTOPEN '{'
#define DICTCLOSE '}'

/* Forward */
static int ncz_sync_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose);
static int ncz_sync_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose);
static int zcharify(const NCjson* src, NCbytes* buf);
static int zconvert(const NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst);
static int computeattrinfo(NC_FILE_INFO_T*,const char* name, const NCjson* jtypes, nc_type typehint, int purezarr, NCjson* values,nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap);
static int computeattrdata(nc_type typehint, nc_type* typeidp, const NCjson* values, size_t* typelenp, size_t* countp, void** datap);
static int ncz_read_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* diminfo);
static int ncz_read_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname);
static int ncz_read_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* subgrpnames);
static int parse_group_content_pure(NC_FILE_INFO_T*, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps);
static int decodeints(const NCjson* jshape, size64_t* shapes);
static int createdim(NC_FILE_INFO_T* file, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp);
static int locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create);
static int json_convention_read(const NCjson* json, NCjson** jtextp);
static int insert_nczarr_attr(NCjson* jatts, NCjson* jtypes);
static int getnczarrkey(NC_FILE_INFO_T* file, NC_OBJ* container, struct ZJSON* json, const char* xxxname, const NCjson** jncxxxp);
static computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int netcdf_rank, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);


/**************************************************/
/* Synchronize functions to make map and memory
be consistent. There are two sets of functions,
1) _sync_ - push nc4internal.h structures to storage
2) _read_ - pull storage structures and create corresponding nc4internal.h structures.
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
ncz_sync_file(NC_FILE_INFO_T* file, int isclose)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    NC_UNUSED(isclose);

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s isclose=%d",file->controller->path,isclose);

    /* Write out root group recursively */
    if((stat = ncz_sync_grp(file, file->root_grp, isclose)))
        goto done;

done:
    NCJreclaim(json);
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
int
ncz_sync_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, int isclose)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int purezarr = 0;
    char* fullpath = NULL;
    char* key = NULL;
    NCindex* dimlist = NULL;
    NCindex* varlist = NULL;
    NCindex* subgrplist = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrp = NULL;
    NCjson* jnczgrp = NULL;
    NCjson* jsuper = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;

    LOG((3, "%s: dims: %s", __func__, key));
    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    zinfo = file->format_file_info;

    if(zinfo->flags & FLAG_PUREZARR) purezarr = 1;

    if(!purezarr) {
        if(grp->parent == NULL) { /* Root group */
	    if((stat=NCZF_encode_superblock(file,grp,&jsuper))) goto done;
	}

	/* Collect dimensions in this group */
	dimlist = grp->dim;
        /* Create dimensions json */
	if((stat=NCZF_encode_grp_dims(file,grp,dimlist,&jdims))) goto done;

	/* Collect vars in this group */
	varlist = grp->vars;
        /* Create variables json */
	if((stat=NCZF_encode_grp_vars(file,grp,varlist,&jvars))) goto done;

	/* Collect subgroups in this group */
	subgrplist = grp->children;
        /* Create subgroups json */
	if((stat=NCZF_encode_grp_subgroups(file,grp,subgrplist,&jsubgrp))) goto done;

        /* Assemble NCZARR_GROUP contents */
	if((stat=NCZF_encode_nczarr_group(file,grp,jdims,jvars,jsubgrp,&jnczgrp))) goto done;
    }

    /* Build the json attrs object */
    assert(grp->att);
    if((stat = NCZF_encode_attributes_json(file, (NC_OBJ*)grp, grp->att, &jatts, &jtypes))) goto done;

    /* Assemble group json */
    if((stat = NCZF_encode_group_json(file,grp,jsuper,jatts,jtypes,&jgroup))) goto done;

    /* upload group json and (depending on version) the group attributes */
    if((stat = NCZF_upload_grp_json(file,grp,jgroup,jatts))) goto done;

    /* Now synchronize all the variables */
    for(i=0; i<ncindexsize(grp->vars); i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	if((stat = ncz_sync_var(file,var,isclose))) goto done;
    }

    /* Now recurse to synchronize all the subgrps */
    for(i=0; i<ncindexsize(grp->children); i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = ncz_sync_grp(file,g,isclose))) goto done;
    }

done:
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
ncz_sync_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    char* dimpath = NULL;
    NClist* dimrefs = NULL;
    NCjson* jvar = NULL;
    NCjson* jatts = NULL;
    NCjson* jnczvar = NULL;
    NCjson* jtypes = NULL;
    char* dtypename = NULL;
    int purezarr = 0;
    NCZ_VAR_INFO_T* zvar = var->format_var_info;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    zinfo = file->format_file_info;

    if(zinfo->flags & FLAG_PUREZARR) purezarr = 1;

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

    /* Build the NCZ_ARRAY object */
    if(!purezarr) {
	if((stat=NCZF_encode_nczarr_array(file,var,&jnczvar))) goto done;
    }

    /* Build attribute dictionary */
    assert(var->att);
    NCJnew(NCJ_DICT,&jatts);
    NCJnew(NCJ_DICT,&jtypes);
    if((stat = ncz_sync_atts(file, (NC_OBJ*)var, var->att, jatts, jtypes, isclose))) goto done;

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

    if((stat=NCZF_encode_var_json(file, var, jatts, jnczvar, &jvar))) goto done;

    /* Write out the the var JSON and the corresponding attributes */
    if((stat = NCZF_upload_var_json(file,var,jvar,jatts))) goto done;

    var->created = 1;

done:
    nclistfreeall(dimrefs);
    nullfree(fullpath);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jnczvar);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
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
ncz_sync_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = var->format_var_info;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    if(isclose) {
	if((stat = ncz_sync_var_meta(file,var,isclose))) goto done;
    }

    /* flush only chunks that have been written */
    if(zvar->cache) {
        if((stat = NCZ_flush_chunk_cache(zvar->cache)))
	    goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/*
Flush all modified chunks to disk. Create any that are missing
and fill as needed.
*/
int
ncz_write_var(NC_VAR_INFO_T* var)
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
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize attribute data from memory to map.
 *
 * @param file
 * @param container Pointer to grp|var struct containing the attributes
 * @param attlist
 * @param jattsp
 * @param jtypesp
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCindex* attlist, NCjson* jatts, NCjson* jtypes, int isclose)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    char* fullpath = NULL;
    char* key = NULL;
    char* content = NULL;
    char* dimpath = NULL;
    int isxarray = 0;
    int inrootgroup = 0;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* tname = NULL;
    char* talias = NULL;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    NC_UNUSED(isclose);
    
    ZTRACE(3,"file=%s container=%s |attlist|=%u",file->controller->path,container->name,(unsigned)ncindexsize(attlist));
    
    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL)
	    inrootgroup = 1;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }
    
    zinfo = file->format_file_info;
    if(zinfo->flags & FLAG_XARRAYDIMS) isxarray = 1;

    if(ncindexsize(attlist) > 0) {
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(attlist);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	    size_t typesize = 0;
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE)
	        {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
	        typesize = (size_t)NCZ_get_maxstrlen(container);
	    else
	        {if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;

	    /* Collect the corresponding dtype */
            if((stat = NCZF_nctype2dtype(file,a->nc_typeid,endianness,typesize,&tname,&talias))) goto done;

	    /* Insert the attribute; consumes jdata */
	    if((stat = ncz_insert_attr(jatts,jtypes,a->hdr.name, jdata, tname))) goto done;

	    /* cleanup */
            nullfree(tname); tname = NULL;
	    jdata = NULL;

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
	    NCJnew(NCJ_ARRAY,&jdimrefs);
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
	        if((stat = NCJinsert(jatts,NC_XARRAY_DIMS,jdimrefs))<0) {stat = NC_EINVAL; goto done;}
	        jdimrefs = NULL;
	    }
        }
    }
    /* Add Quantize Attribute */
    if(container->sort == NCVAR && var && var->quantize_mode > 0) {    
	char mode[64];
	snprintf(mode,sizeof(mode),"%d",var->nsd);
        NCJnewstring(NCJ_INT,mode,&jint);
	/* Insert the quantize attribute */
	switch (var->quantize_mode) {
	case NC_QUANTIZE_BITGROOM:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITGROOM_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_GRANULARBR:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_GRANULARBR_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_BITROUND:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITROUND_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	default: break;
	}
    }

done:
    nullfree(fullpath);
    nullfree(key);
    nullfree(content);
    nullfree(dimpath);
    nullfree(tname);
    NCJreclaim(jdimrefs);
    NCJreclaim(jint);
    NCJreclaim(jdata);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/*
 * @internal pull storage structures and create corresponding nc4internal.h structures
 */

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
    NC_GRP_INFO_T* root = NULL;
    struct ZJSON jsonz = emptyjsonz;

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s",file->controller->path);
    
    /* Download the root group object and associated attributess */
    root = file->root_grp;
    if((stat = NCZF_download_grp_json(file, root, &jsonz))) goto done;

    /* Ok, try to read superblock */
    if((stat = ncz_read_superblock(file,&jsonz))) goto done;

    /* Load metadata starting at the root group */
    if((stat = ncz_read_grp(file,root,&jsonz))) goto done;

done:
    NCZ_clear_zjson(&jsonz);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Read group data from storage
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_read_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, struct ZJSON* jsonz)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = file->format_file_info;
    NCjson* jdims = NULL;
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();
    int purezarr = 0;

    ZTRACE(3,"file=%s parent=%s",file->controller->path,(parent?parent->hdr.name:"NULL"));
    
    if(zinfo->flags & FLAG_PUREZARR) purezarr = 1;

    /* Download json group and json attrs and optionally create grp */
    if((stat = NCZF_download_grp_json(file,grp,jsonz))) goto done;

    if(purezarr) {
	if((stat = parse_group_content_pure(file,grp,varnames,subgrps))) goto done;
    } else { /*!purezarr*/
        if(jsonz->jobj == NULL || jsonz->jatts == NULL) { /* does not exist, use search */
            if((stat = parse_group_content_pure(file,grp,varnames,subgrps))) goto done;
	    purezarr = 1; /* treat as if purezarr */
            zinfo->flags |= FLAG_PUREZARR;
	} else { /*!purezarr && jgroup && jatts */
  	    /* Decode and group object */
            if((stat = NCZF_decode_nczarr_group(file,grp,jsonz,&jdims,varnames,subgrps))) goto done;
	}
    }

    if(!purezarr) {
	/* Define dimensions */
	if((stat = ncz_read_dims(file,grp,jdims))) goto done;
    }

    /* Define sub-groups */
    if((stat = ncz_read_subgrps(file,grp,subgrps))) goto done;

    /* Define vars taking xarray into account */
    if((stat = ncz_read_vars(file,grp,varnames))) goto done;

done:
    NCJreclaim(jdims);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param jdims json holding dim info
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_read_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson* jdims)
{
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |diminfo|=%u",file->controller->path,grp->hdr.name,nclistlength(diminfo));

    if((stat = NCZF_decode_grp_dims(file,grp,jdims))) goto done;

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize single var into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param paret Pointer to parent grp info struct.
 * @param varname name of variable in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_read_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, const char* varname)
{
    int stat = NC_NOERR;
    NC_VAR_INFO_T* var = NULL;
    struct ZJSON jsonz = emptyjsonz;

    ZTRACE(3,"file=%s parent=%s varname=%s",file->controller->path,parent->hdr.name,varname);

    /* Create and Download */
    if((stat = NCZF_create_var(file,parent,varname,&var))) goto done;
    if((stat = NCZF_download_var_json(file,var,&jsonz))) goto done;
    if((stat=NCZF_decode_var_json(file,var,&jsonz))) goto done;

done:
    NCZ_clear_zjson(&jsonz);
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
int
ncz_read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* varnames)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	const char* varname = (const char*)nclistget(varnames,i);
        if((stat = ncz_read_var1(file,parent,varname))) goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

static int
ncz_read_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* parent, NClist* subgrpnames)
{
    int stat = NC_NOERR;
    size_t i;
    struct ZJSON jsonz = emptyjsonz;

    ZTRACE(3,"file=%s parent=%s |subgrpnames|=%u",file->controller->path,parent->hdr.name,nclistlength(subgrpnames));

    /* Create and load each subgrp in turn */
    for(i = 0; i < nclistlength(subgrpnames); i++) {
	const char* subgrpname = (const char*)nclistget(subgrpnames,i);
	NC_GRP_INFO_T* subgrp = NULL;
	/* Create the group object */
	if((stat = NCZF_create_grp(file,parent,subgrpname,&subgrp))) goto done;
	/* Fill in the group object */
        if((stat = ncz_read_grp(file,subgrp,&jsonz))) goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

/**************************************************/

#if 0
/**
@internal Extract attributes from a group or var and return
the corresponding NCjson dict.
@param map - [in] the map object for storage
@param container - [in] the containing object
@param jattsp    - [out] the json for .zattrs || NULL if not found
@param jtypesp   - [out] the json attribute type dict || NULL
@param jnczgrp   - [out] the json for _nczarr_group || NULL
@param jnczarray - [out] the json for _nczarr_array || NULL
@return ::NC_NOERR
@return ::NC_EXXX
@author Dennis Heimbigner
*/
static int
download_jatts(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson** jattsp, const NCjson** jtypesp)
{
    int stat = NC_NOERR;
    const NCjson* jatts = NULL;
    const NCjson* jtypes = NULL;
    const NCjson* jnczattr = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_GRP_INFO_T* zgrp = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    int purezarr = 0;
    int zarrkey = 0;

    ZTRACE(3,"map=%p container=%s ",map,container->name);

    if(zinfo->flags & FLAG_PUREZARR) purezarr = 1;
    if(zinfo->flags & FLAG_NCZARR_KEY) zarrkey = 1;

    if(container->sort == NCGRP) {
	grp = (NC_GRP_INFO_T*)container;
	zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;	
	jatts = zgrp->zgroup.atts;
    } else {
	var = (NC_VAR_INFO_T*)container;
	zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
	jatts = zvar->zarray.atts;
    }
    assert(purezarr || zarrkey || jatts != NULL);

    if(jatts != NULL) {
	/* Get _nczarr_attr from .zattrs */
        if((stat = NCJdictget(jatts,NCZ_ATTR,&jnczattr))<0) {stat = NC_EINVAL; goto done;}
	if(jnczattr != NULL) {
	    /* jnczattr attribute should be a dict */
	    if(NCJsort(jnczattr) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	    /* Extract "types"; may not exist if only hidden attributes are defined */
	    if((stat = NCJdictget(jnczattr,"types",&jtypes))<0) {stat = NC_EINVAL; goto done;}
	    if(jtypes != NULL) {
	        if(NCJsort(jtypes) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	    }
	}
    }
    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jtypes) {*jtypesp = jtypes; jtypes = NULL;}

done:
    return ZUNTRACE(THROW(stat));
}
#endif /*0*/

/* Convert a JSON singleton or array of strings to a single string */
static int
zcharify(const NCjson* src, NCbytes* buf)
{
    int stat = NC_NOERR;
    size_t i;
    struct NCJconst jstr = NCJconst_empty;

    if(NCJsort(src) != NCJ_ARRAY) { /* singleton */
        if((stat = NCJcvt(src, NCJ_STRING, &jstr))<0) {stat = NC_EINVAL; goto done;}
        ncbytescat(buf,jstr.sval);
    } else for(i=0;i<NCJarraylength(src);i++) {
	NCjson* value = NCJith(src,i);
	if((stat = NCJcvt(value, NCJ_STRING, &jstr))<0) {stat = NC_EINVAL; goto done;}
	ncbytescat(buf,jstr.sval);
        nullfree(jstr.sval);jstr.sval = NULL;
    }
done:
    nullfree(jstr.sval);
    return stat;
}

/* Convert a json value to actual data values of an attribute. */
static int
zconvert(const NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst)
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
	    if((stat = zcharify(src,dst))) goto done;
	    count = ncbyteslength(dst);
        } else {
	    count = NCJarraylength(src);
	    for(i=0;i<count;i++) {
	        NCjson* value = NCJith(src,i);
                if((stat = NCZ_convert1(value, typeid, dst))) goto done;
	    }
	}
	break;
    case NCJ_STRING:
	if(typeid == NC_CHAR) {
	    if((stat = zcharify(src,dst))) goto done;
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

/*
Extract type and data for an attribute
*/
static int
computeattrinfo(NC_FILE_INFO_T* file, const char* name, const NCjson* jtypes, nc_type typehint, int purezarr, NCjson* values,
		nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap)
{
    int stat = NC_NOERR;
    size_t i;
    size_t len, typelen;
    void* data = NULL;
    nc_type typeid;

    ZTRACE(3,"name=%s typehint=%d purezarr=%d values=|%s|",name,typehint,purezarr,NCJtotext(values));

    /* Get type info for the given att */
    typeid = NC_NAT;
    for(i=0;i<NCJdictlength(jtypes);i++) {
	NCjson* akey = NCJdictkey(jtypes,i);
	if(strcmp(NCJstring(akey),name)==0) {
	    const NCjson* avalue = NCJdictvalue(jtypes,i);
	    if((stat = NCZF_dtype2nctype(file,NCJstring(avalue),typehint,&typeid,NULL,NULL))) goto done;
	    break;
	}
    }
    if(typeid > NC_MAX_ATOMIC_TYPE)
	{stat = NC_EINTERNAL; goto done;}
    /* Use the hint if given one */
    if(typeid == NC_NAT)
        typeid = typehint;

    if((stat = computeattrdata(typehint, &typeid, values, &typelen, &len, &data))) goto done;

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
computeattrdata(nc_type typehint, nc_type* typeidp, const NCjson* values, size_t* typelenp, size_t* countp, void** datap)
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
	if((stat = json_convention_read(values,&jtext))) goto done;
	values = jtext; jtext = NULL;
	reclaimvalues = 1;
    } 

    if((stat = NC4_inq_atomic_type(typeid, NULL, &typelen)))
        goto done;

    /* Convert the JSON attribute values to the actual netcdf attribute bytes */
    if((stat = zconvert(values,typeid,typelen,&count,buf))) goto done;

    if(typelenp) *typelenp = typelen;
    if(typeidp) *typeidp = typeid; /* return possibly inferred type */
    if(countp) *countp = (size_t)count;
    if(datap) *datap = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    if(reclaimvalues) NCJreclaim((NCjson*)values); /* we created it */
    return ZUNTRACEX(THROW(stat),"typelen=%d count=%u",(typelenp?*typelenp:0),(countp?*countp:-1));
}

/**************************************************/
/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
@param zfile - [in] the containing file (annotation)
@param container - [in] the containing object
@return ::NC_NOERR
@author Dennis Heimbigner
*/
int
ncz_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jatts)
{
    int stat = NC_NOERR;
    char* fullpath = NULL;
    char* key = NULL;

    NC_ATT_INFO_T* att = NULL;
    size_t len;
    void* data = NULL;
    NC_ATT_INFO_T* fillvalueatt = NULL;
    NCjson* jtypes = NULL;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    if(jsonz->jatts != NULL) {
        if((stat = NCZF_decode_attributes_json(file,container,jatts,&jtypes))) goto done;
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
    NCJreclaim(jtypes);
    if(data != NULL)
        stat = NC_reclaim_data(file->controller,att->nc_typeid,data,len);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

int
ncz_read_superblock(NC_FILE_INFO_T* file, struct ZJSON* jsonz)
{
    int stat = NC_NOERR;

    const NCjson* jgroup = jsonz->jobj;
    const NCjson* jnczgroup = NULL;
    const NCjson* jnczattr = NULL;
    const NCjson* jsuper = NULL;
    char* nczarr_version = NULL;
    char* zarr_format = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_GRP_INFO_T* root = NULL;

    ZTRACE(3,"file=%s",file->controller->path);

    root = file->root_grp;
    assert(root != NULL);

    zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;    

    /* Look for superblock; first in jattrs and then in jgroup */
    if((stat = getnczarrkey(file,(NC_OBJ*)root,jsonz,NCZ_SUPERBLOCK,&jsuper))) goto done;

    /* Set where _nczarr_xxx are stored */
    if(jsuper != NULL && zinfo->flags & FLAG_NCZARR_KEY) {
	/* Also means file is read only */
	file->no_write = 1;
    }
    
    if(jsuper == NULL) {
#if 0
	/* See if this is looks like a NCZarr/Zarr dataset at all
           by looking for anything here of the form ".z*" */
        if((stat = ncz_validate(file))) goto done;
#endif
	/* ok, assume pure zarr */
	zinfo->flags |= FLAG_PUREZARR;	
	if(zarr_format == NULL) {
	    assert(zinfo->zarr.zarr_format > 0);
	    zarr_format = (char*)calloc(1,4); /* overkill */
	    snprintf(zarr_format,4,"%d",zinfo->zarr.zarr_format);
	}
    }

    /* Look for _nczarr_group */
    if((stat = getnczarrkey(file,(NC_OBJ*)root,jsonz,NCZ_GROUP,&jnczgroup))) goto done;

    /* Look for _nczarr_attr*/
    if((stat = getnczarrkey(file,(NC_OBJ*)root,jsonz,NCZ_ATTR,&jnczattr))) goto done;

    if(jsuper != NULL) {
	const NCjson* jtmp = NULL;
	if(jsuper->sort != NCJ_DICT) {stat = NC_ENCZARR; goto done;}
	if((stat = NCZ_dictgetalt(jsuper,"nczarr_version","version",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	nczarr_version = nulldup(NCJstring(jtmp));
    }

    if(jgroup != NULL) {
	const NCjson* jtmp = NULL;
        if(jgroup->sort != NCJ_DICT) {stat = NC_ENCZARR; goto done;}
        /* In any case, extract the zarr format */
        if((stat = NCJdictget(jgroup,"zarr_format",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	if(zarr_format == NULL)
	    zarr_format = nulldup(NCJstring(jtmp));
	else if(strcmp(zarr_format,NCJstring(jtmp))!=0)
	    {stat = NC_ENCZARR; goto done;}
    }

done:
    nullfree(zarr_format);
    nullfree(nczarr_version);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Utilities */

static int
parse_group_content_pure(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;

    ZTRACE(3,"zinfo=%s grp=%s |varnames|=%u |subgrps|=%u",zinfo->common.file->controller->path,grp->hdr.name,(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    nclistclear(varnames);
    if((stat = NCZF_searchvars(file,grp,varnames))) goto done;
    nclistclear(subgrps);
    if((stat = NCZF_searchsubgrps(file,grp,subgrps))) goto done;

done:
    return ZUNTRACE(THROW(stat));
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
	if((stat = ncz_splitkey(dimpath,segments)))
	    goto done;
	if((stat=locategroup(file,nclistlength(segments)-1,segments,&g)))
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
    return ZUNTRACE(retval);
}


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
/**
Implement the JSON convention:
Stringify it as the value and make the attribute be of type "char".
*/

static int
json_convention_read(const NCjson* json, NCjson** jtextp)
{
    int stat = NC_NOERR;
    NCjson* jtext = NULL;
    char* text = NULL;

    if(json == NULL) {stat = NC_EINVAL; goto done;}
    if(NCJunparse(json,0,&text)) {stat = NC_EINVAL; goto done;}
    NCJnewstring(NCJ_STRING,text,&jtext);
    *jtextp = jtext; jtext = NULL;
done:
    NCJreclaim(jtext);
    nullfree(text);
    return stat;
}

#if 0
/**
Implement the JSON convention:
Parse it as JSON and use that as its value in .zattrs.
*/
static int
json_convention_write(size_t len, const void* data, NCjson** jsonp, int* isjsonp)
{
    int stat = NC_NOERR;
    NCjson* jexpr = NULL;
    int isjson = 0;

    assert(jsonp != NULL);
    if(NCJparsen(len,(char*)data,0,&jexpr)) {
	/* Ok, just treat as sequence of chars */
	NCJnewstringn(NCJ_STRING, len, data, &jexpr);
    }
    isjson = 1;
    *jsonp = jexpr; jexpr = NULL;
    if(isjsonp) *isjsonp = isjson;
done:
    NCJreclaim(jexpr);
    return stat;
}
#endif

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
    if(!validate) stat = NC_ENOTNC;
    nullfree(path);
    nullfree(segment);
    nclistfreeall(queue);
    nclistfreeall(nextlevel);
    ncbytesfree(prefix);
    return ZUNTRACE(THROW(stat));
}
#endif

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
    nullfree(fullpath);
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
    NCJreclaim(json);
    return stat;
}
#endif

/* Get _nczarr_xxx from either grp|var json or atts json */
static int
getnczarrkey(NC_FILE_INFO_T* file, NC_OBJ* container, struct ZJSON* json, const char* xxxname, const NCjson** jncxxxp, int* nczkeyp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->file_format_info;
    const NCjson* jxxx = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;

    /* Decode container */
    if(container->sort == NCGRP) {
	grp = (NC_GRP_INFO_T*)container;
    } else {
	var = (NC_VAR_INFO_T*)container;
    }

    /* Try jatts first */
    if(json->jatts != NULL) {
	jxxx = NULL;
        if((stat = NCJdictget(json->atts,name,&jxxx))<0) {stat = NC_EINVAL; goto done;}
    }
    if(jxxx == NULL) {
        /* Try .zxxx second */
	if(json->obj != NULL) {
            if((stat = NCJdictget(json->obj,name,&jxxx))<0) {stat = NC_EINVAL; goto done;}
	}
	/* Mark as old style with _nczarr_xxx in obj as keys not attributes */
        zfile->flags |= FLAG_NCZARR_KEY;
    }
    if(jncxxxp) *jncxxxp = jxxx;
done:
    return THROW(stat);
}

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
    nullfree(key);
    return THROW(stat);
}
#endif /*0*/

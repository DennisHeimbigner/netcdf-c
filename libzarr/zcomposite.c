/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
 * @file
 * @internal The ZARR functions to create the composite metadata
 *
 * @author Dennis Heimbigner
 */

/* The composite metadata Json structure conforms to this grammar.
Note that attributes are left out because they can constute
such a large amount of metadata.

dataset: // Dict
	NAME
	ZARRVERSION
	NCZARRVERSION
	group

group: // Dict
	NAME
	dimensions
	variables
	(sub)groups

dimensions: // Array
	dimension*

dimension: // Dict
	NAME SIZE

types: // Array
	type*

type: // Dict
	KIND // Indicates compound | opaque | (eventually) enumerations | ATOMIC
	(compound | opaque)

compound: // Dict
	NAME
	fields

fields: // Array
	field+

field: // Dict
	NAME
	typeref
	sizes

sizes: // Array
	size*

size: POSITIVE-INTEGER // constant

opaque: // Dict
	NAME
	size

variables: // Array
	variable*

variable: // Dict
	NAME
	typeref
	dimrefs

dimrefs: // Array
	dimref*

dimref: // Constant
	Fully-qualified-name-of-dimension

typeref: // Constant
	Fully-qualified-name-of type | atomic-type

groups: // Array
	group*


*/

/*
Given the NC_FILE_INFO_T* of a dataset,
generate an NCJson instance containing
the composite metadata for that dataset
and conforming to the above grammar.
*/

int
NCZ_build_composite(NC_FILE_INFO_T* file, NCjson** metadatap)
{
    int stat = NC_NOERR;
    NCjson* metadata = NULL;
    char* key = NULL;
    NCjson* node = NULL;
    char snumber[16];
    uintptr_t value = 0;
    NCZ_FILE_INFO* dataset = NULL;
    NClist* array = NULL;

    dataset = (NCZ_FILE_INFO*)file->format_file_info;

    metadata = NCJnew(NCJ_DICT);

    node = NCJnew(NCJ_STRING);
    node->word = strdup(dataset->urltext)
    if((stat=NCJinsert(metadata,"name",node))) goto done;
    node = NULL; /* avoid reclaim */

    snprintf(snumber,sizeof(snumber),"%ld",dataset->zarr.zarrversion);
    node = NCJnew(NCJ_INT);
    node->word = strdup(snumber);
    if((stat=NCJinsert(metadata,"zarrversion",node))) goto done;
    node = NULL;

    snprintf(snumber,sizeof(snumber),"%ld",dataset->zarr.nczarrversion);
    node = NCJnew(NCJ_STRING);
    node->word = strdup(snumber);
    if((stat=NCJinsert(metadata,"nczarrversion",node))) goto done;
    node = NULL;

    /* Recursively build the tree of groups */
    if((stat=build_json_group(dataset,file->root_grp,&node))) goto done;
    if((stat=NCJinsert(metadata,"root",node))) goto done;    
    node = NULL;

    if(metadatap) {*metadatap = metadata; metadata = NULL;}

done:
    if(dict != NULL) NC_hashmapfree(dict);
    if(stat) NCJreclaim(metadata);
    return stat;
}

static int
build_json_group(NCZ_FILE_INFO* dataset, NC_GRP_INFO_T* grp, NCjson** nodep)
{
    int stat = NC_NOERR;
    int i;
    char* key = NULL;
    char snumber[16];
    uintptr_t value = 0;
    NCjson* group = NULL;
    NCjson* dnodeset = NULL;
    NCjson* tnodeset = NULL;
    NCjson* vnodeset = NULL;
    NCjson* gnodeset = NULL;
    NCjson* subnode = NULL;
    NCjson* dimarray = NULL;
    NCjson* ref = NULL;
    NCjson* vdict = NULL;
    NCjson* gdict = NULL;
    char* fqn = NULL;

    if((group = NCJnew(NCJ_DICT)) == NULL) {stat = NC_ENOMEM; goto done;}

    /* Build the dict of dimensions */
    if((dnodeset = NCJnew(NCJ_DICT)) == NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<ncindexsize(grp->dim);i++) {
        NC_DIM_INFO_T* d = ncindexith(grp->dim,i);
	char size[16];
	snprintf(size,sizeof(size),"%ld",d->len);
	if((stat=NCJinsert(dnodeset,d->hdr.name,strdup(size)))) goto done;
    }
    /* Add to group */
    if((stat=NCJinsert(group,"dimensions",dnodeset))) goto done;
    dnodeset = NULL;

    /* TBD: types */

    /* Build the dict of variables */
    if((vnodeset = NCJnew(NCJ_DICT)) == NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<ncindexsize(grp->vars);i++) {
	int j;
        NC_VAR_INFO_T* v = ncindexith(grp->vars,i);
	/* Build array of the dimrefs */
        if((dimarray = NCJnew(NCJ_ARRAY)) == NULL) {stat = NC_ENOMEM; goto done;}
        for(j=0;j<v->ndims;j++) {
            NC_DIM_INFO_T* d = v->dim[j];
	    if((fqn = fqn(d)) != NULL) {stat = NC_ENOMEM; goto done;}
            if((ref = NCJnew(NCJ_STRING)) == NULL) {stat = NC_ENOMEM; goto done;}
	    ref->word = fqn;
	    fqn = NULL;	
	    NCJappend(dimarray,ref);
	    ref = NULL;
	}
	/* Build subdict of type and the dimrefs */
        if((vdict = NCJnew(NCJ_DICT)) == NULL) {stat = NC_ENOMEM; goto done;}
	if((stat=NCJinsert(vdict,"dimrefs",dimarray))) goto done;
	dimarray = NULL;
	/* Add type name */
        if((fqn = fqn(v->type_info)) != NULL) {stat = NC_ENOMEM; goto done;}
        if((ref = NCJnew(NCJ_STRING)) == NULL) {stat = NC_ENOMEM; goto done;}
	ref->word = fqn;	
	fqn = NULL;
	if((stat=NCJinsert(vdict,"type",ref))) goto done;
	ref = NULL;
	/* Add subdict to variable dict */
	if((stat=NCJinsert(vnodeset,v->hdr.name,vdict))) goto done;
	vdict = NULL;
    }
    /* Add to group */
    if((stat=NCJinsert(group,"variables",vnodeset))) goto done;
    vnodeset = NULL;

    /* Build the dict of sub-groups*/
    if((gnodeset = NCJnew(NCJ_DICT)) == NULL) {stat = NC_ENOMEM; goto done;}
    for(i=0;i<ncindexsize(grp->children);i++) {
        NC_GRP_INFO_T* g = ncindexith(grp->children,i);
	/* Recurse to create subgroup */
	if((stat = build_json_group(dataset,g,&gdict))) goto done;
	if((stat=NCJinsert(gnodeset,g->hdr.name,gdict))) goto done;
    }
    /* Add to group */
    if((stat=NCJinsert(group,"groups",gnodeset))) goto done;
    gnodeset = NULL;
	
done:
    nullfree(fqn);
    if(stat) {
	NCJreclaim(group);
	NCJreclaim(dnodeset);
	NCJreclaim(tnodeset);
	NCJreclaim(vnodeset);
	NCJreclaim(gnodeset);
	NCJreclaim(subnode);
	NCJreclaim(dimarray);
	NCJreclaim(ref);
	NCJreclaim(vdict);
	NCJreclaim(gdict);
    }
    return stat;
}

/*
Inverse of build_composite:
namely construct tree of netcdf-4 objects
from a json tree
*/

int
NCZ_build_meta(NC_FILE_INFO_T* file, NCjson* composite)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCjson* node = NULL;
    NCZ_FILE_INFO* dataset = NULL;
    NClist* array = NULL;
    NClist* dict = NULL;

    dataset = (NCZ_FILE_INFO*)file->format_file_info;

    /* Add zarr version and the nczarr version to
      _NCProperties */
    /*TBD*/

    /* fill the root group and recursively, everything below it */
    if((stat = fill_nc4_group(dataset,file->root_grp,composite))) goto done;   

done:
    NClistfree(array);
    NClistfree(dict);
    return stat;
}

/* Recursive meta builder
*/
static int
fill_nc4_group(NCZ_FILE_INFO* dataset, NC_GRP_INFO_T* grp, NCjson* grpdict)
{
    NCjson* subgroups = NULL;
    NCjson* dimension = NULL;
    NCjson* types = NULL;
    NCjson* variables = NULL;
    NC_FILE_INFO_T* file = dataset->dispatch->dataset;

    assert(grpdict->sort == NCJ_DICT);
    /* Start by recursively building the group tree */
    if((stat=NCJdictget(jnode,"groups",&subgroups))) goto done;
    if(subgroups != NULL) {
	/* Walk the subgroups */
	assert(subgroups->sort == NCJ_ARRAY);
	if((stat=build_nc4_subgroups(dataset,grp,subgroups))) goto done;
    }
    /* Fill in this group's dimensions */
    if((stat=NCJdictget(grpdict,"dimensions",&dimensions))) goto done;
    if(dimensions != NULL) {
	/* Walk the dimensions */
	assert(dimensions->sort == NCJ_ARRAY);
	if((stat=build_nc4_dimensions(dataset,grp,dimensions))) goto done;
    }
    /* Fill in this group's variables */
    if((stat=NCJdictget(grpdict,"variables",&variables))) goto done;
    if(variables != NULL) {
	/* Walk the variables */
	assert(variables->sort == NCJ_ARRAY);
	if((stat=build_nc4_variables(dataset,grp,variables))) goto done;
    }

done:
    return stat;
}

/* Mutually recursive with fill_nc4_group to create the group tree and fill it in */
static int
build_nc4_subgroups(NCZ_FILE_INFO_T* dataset, NC_GRP_INFO_T* parent, NCjson* subgroups)
{
    int stat = NC_NOERR;
    int i;
    NC_FILE_INFO_T* file = dataset->dispatch->dataset;

    /* Walk the subgroups */
    assert(subgroups->sort == NCJ_ARRAY);
    for(i=0;i<nclistlength(subgroups);i++) {
	NC_GRP_INFO_T* g = NULL;
	NCjson* gdict = nclistget(subgroups,i);
	NCjson* value = NULL;
	assert(gdict->sort == NCJ_DICT);
	if((stat=NCJdictget(dict,"name",&value))) goto done;
        /* Create the subgroup in parent */
	assert(value->sort == NCJ_STRING);
	if((stat = nc4_grp_list_add(file,parent,value->word,&g))) goto done;
        /* recursively fill the newly created subgroup */
        if((stat=fill_nc4_group(dataset,g,gdict))) goto done;
	}
    }
done:
    return stat;
}

static int
build_nc4_dimensions(NCZ_FILE_INFO_T* dataset, NC_GRP_INFO_T* parent, NCjson* dimensions)
{
    int stat = NC_NOERR;
    int i;
    NC_FILE_INFO_T* file = dataset->dispatch->dataset;

    /* Walk the dimensions */
    assert(dimensions->sort == NCJ_ARRAY);
    for(i=0;i<nclistlength(dimensions);i++) {
	NC_DIM_INFO_T* dim;
	NCjson* ddict = nclistget(dimensions,i);
	NCjson* jname = NULL;
	NCjson* jsize = NULL;
	long long size;
        if((stat=NCJdictget(ddict,"name",&jname))) goto done;
	if((stat=NCJdictget(ddict,"size",&jsize))) goto done;
	assert(jsize->sort == NCJ_INT);
	if(sscanf(jsize->word,"%lld",&size) != 1) {stat = NC_EINVAL; goto done;}
	/* Create the dimension and insert into parent grp*/
	if((stat=nc4_dim_list_add(grp,name->word,size,0,&dim))) goto done;
    }
done:
    return stat;
}

static int
build_nc4_variables(NCZ_FILE_INFO_T* dataset, NC_GRP_INFO_T* parent, NCjson* variables)
{
    int stat = NC_NOERR;
    int i;
    NC_FILE_INFO_T* file = dataset->dispatch->dataset;

    /* Walk the variables */
    assert(variables->sort == NCJ_ARRAY);
    for(i=0;i<nclistlength(variables);i++) {
	NC_VAR_INFO_T* var;
	NCjson* vdict = nclistget(variables,i);
	NCjson* jname = NULL;
	NCjson* jtyperef = NULL;
	NCjson* jdimrefs = NULL;
	size_t rank;
	int j;
        if((stat=NCJdictget(ddict,"name",&jname))) goto done;
	if((stat=NCJdictget(ddict,"typeref",&jtyperef))) goto done;
	if((stat=NCJdictget(ddict,"dimrefs",&jdimrefs))) goto done;
	rank = nclistlength(jdimrefs->array);
	/* Create the variable and insert into parent grp*/
	if((stat=nc4_var_list_add(grp,name->word,rank,&var))) goto done;
	/* Find the type */
	fqn = jtyperef->value;a
	if((stat = find_type(file,fqn,&type))) goto done;
	var->type_info = type;	
	/* Find the dimensions */
	for(j=0;j<nclistlength(dimrefs->array);i++) {
	    fqn = ((NCjson*)nclistget(dimrefs->arrat,i))->value;
	    if((stat=find_dim(file,fqn,&dim))) goto done;
??? consider using dimid ints rather than fqn	
	}	
    }
done:
    return stat;
}

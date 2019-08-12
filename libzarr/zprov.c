/**
 * @file
 * @internal Add provenance info for netcdf-4 files.
 *
 * Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for copying
 * and redistribution conditions.
 * @author Dennis Heimbigner
 */

#include "zinternal.h"
#include "nc_provenance.h"

/* Provide a hack to suppress the writing of _NCProperties attribute.
   This is for creating a file without _NCProperties for testing purposes.
*/
#undef SUPPRESSNCPROPS

/* Various Constants */
#define NCPROPS_MAX_NAME 1024 /* max key name size */
#define NCPROPS_MAX_VALUE 1024 /* max value size */
#define NCZ_MAX_NAME 1024 /**< ZARR max name. */

#define ESCAPECHARS "\\=|,"

#define NCPNCZLIB "zarr"

/** @internal Check NetCDF return code. */
#define NCHECK(expr) {if((expr)!=NC_NOERR) {goto done;}}

/** @internal Check ZARR return code. */
#define HCHECK(expr) {if((expr)<0) {ncstat = NC_EHDFERR; goto done;}}

static int NCZ_read_ncproperties(NC_FILE_INFO_T* h5, char** propstring);
static int NCZ_write_ncproperties(NC_FILE_INFO_T* h5);

static int globalpropinitialized = 0;
static NCZ_Provenance globalprovenance;

/**
 * @internal Initialize default provenance info
 * This will only be used for newly created files
 * or for opened files that do not contain an _NCProperties
 * attribute.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_provenance_init(void)
{
    int stat = NC_NOERR;
    char* name = NULL;
    char* value = NULL;
    unsigned major,minor,release;
    NCbytes* buffer = NULL; /* for constructing the global _NCProperties */
    char printbuf[1024];
    const char* p = NULL;

    if(globalpropinitialized)
        return stat;

    /* Build _NCProperties info */

    /* Initialize globalpropinfo */
    memset((void*)&globalprovenance,0,sizeof(NCZ_Provenance));
    globalprovenance.version = NCPROPS_VERSION;

    buffer = ncbytesnew();

    /* Insert version as first entry */
    ncbytescat(buffer,NCPVERSION);
    ncbytescat(buffer,"=");

    snprintf(printbuf,sizeof(printbuf),"%d",globalprovenance.version);
    ncbytescat(buffer,printbuf);

    /* Insert the netcdf version */
    ncbytesappend(buffer,NCPROPSSEP2);
    ncbytescat(buffer,NCPNCLIB2);
    ncbytescat(buffer,"=");
    ncbytescat(buffer,PACKAGE_VERSION);

    /* Insert the ZARR as underlying storage format library */
    ncbytesappend(buffer,NCPROPSSEP2);
    ncbytescat(buffer,NCPNCZLIB);
    ncbytescat(buffer,"=");
    if((stat = NCZ_get_libversion(&major,&minor,&release))) goto done;
    snprintf(printbuf,sizeof(printbuf),"%1u.%1u.%1u",major,minor,release);
    ncbytescat(buffer,printbuf);

#ifdef NCPROPERTIES_EXTRA
    /* Add any extra fields */
    p = NCPROPERTIES_EXTRA;
    if(p[0] == NCPROPSSEP2) p++; /* If leading separator */
    ncbytesappend(buffer,NCPROPSSEP2);
    ncbytescat(buffer,p);
#endif
    ncbytesnull(buffer);
    globalprovenance.ncproperties = ncbytesextract(buffer);

done:
    ncbytesfree(buffer);
    if(name != NULL) free(name);
    if(value != NULL) free(value);
    if(stat == NC_NOERR)
        globalpropinitialized = 1; /* avoid repeating it */
    return stat;
}

/**
 * @internal finalize default provenance info
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_provenance_finalize(void)
{
    return NCZ_clear_provenance(&globalprovenance);
}

/**
 * @internal
 *
 * Construct the provenance information for a newly created file.
 * Note that creation of the _NCProperties attribute is deferred
 * to the sync_netcdf4_file function.
 *
 * @param file Pointer to file object.
 *
 * @return ::NC_NOERR No error.
 * [Note: other errors are reported via LOG()]
 * @author Dennis Heimbigner
 */
int
NCZ_new_provenance(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_Provenance* provenance = NULL;
    int superblock = -1;

    LOG((5, "%s: ncid 0x%x", __func__, file->root_grp->hdr.id));

    assert(file->provenance.ncproperties == NULL); /* not yet defined */

    provenance = &file->provenance;
    memset(provenance,0,sizeof(NCZ_Provenance)); /* make sure */

    /* Set the version */
    provenance->version = globalprovenance.version;

#ifdef LOOK
    /* Set the superblock number */
    if((stat = NCZ_nczget_superblock(file,&superblock))) goto done;
    provenance->superblockversion = superblock;
#endif

    if(globalprovenance.ncproperties != NULL) {
        if((provenance->ncproperties = strdup(globalprovenance.ncproperties)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
    }

done:
    if(stat) {
        LOG((0,"Could not create _NCProperties attribute"));
    }
    return NC_NOERR;
}

/**
 * @internal
 *
 * Construct the provenance information for an existing file.
 *
 * @param file Pointer to file object.
 *
 * @return ::NC_NOERR No error.
 * [Note: other errors are reported via LOG()]
 * @author Dennis Heimbigner
 */
int
NCZ_read_provenance(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_Provenance* provenance = NULL;
    char* propstring = NULL;

    LOG((5, "%s: ncid 0x%x", __func__, file->root_grp->hdr.id));

    assert(file->provenance.version == 0); /* not yet defined */

    provenance = &file->provenance;
    memset(provenance,0,sizeof(NCZ_Provenance)); /* make sure */

#ifdef LOOK
    /* Set the superblock number */
    int superblock = -1;
    if((stat = NCZ_nczget_superblock(file,&superblock))) goto done;
    provenance->superblockversion = superblock;
#endif

    /* Read the _NCProperties value from the file */
    if((stat = NCZ_read_ncproperties(file,&propstring))) goto done;
    provenance->ncproperties = propstring;
    propstring = NULL;

done:
    nullfree(propstring);
    if(stat) {
        LOG((0,"Could not create _NCProperties attribute"));
    }
    return NC_NOERR;
}

/**
 * @internal
 *
 * Add the provenance information to a newly created file.
 *
 * @param file Pointer to file object.
 *
 * @return ::NC_NOERR No error.
 * [Note: other errors are reported via LOG()]
 * @author Dennis Heimbigner
 */
int
NCZ_write_provenance(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    if((stat = NCZ_write_ncproperties(file)))
	goto done;
done:
    return stat;
}

#ifdef LOOK
/* ZARR Specific attribute read/write of _NCProperties */
static int
NCZ_read_ncproperties(NC_FILE_INFO_T* h5, char** propstring)
{
    int stat = NC_NOERR;
    hid_t nczgrpid = -1;
    hid_t attid = -1;
    hid_t aspace = -1;
    hid_t atype = -1;
    hid_t ntype = -1;
    char* text = NULL;
    H5T_class_t t_class;
    hsize_t size;

    LOG((5, "%s", __func__));

    nczgrpid = ((NCZ_GRP_INFO_T *)(h5->root_grp->format_grp_info))->hdf_grpid;

    if(H5Aexists(nczgrpid,NCPROPS) <= 0) { /* Does not exist */
        /* File did not contain a _NCProperties attribute; leave empty */
        goto done;
    }

    /* NCPROPS Attribute exists, make sure it is legitimate */
    attid = H5Aopen_name(nczgrpid, NCPROPS);
    assert(attid > 0);
    aspace = H5Aget_space(attid);
    atype = H5Aget_type(attid);
    /* Verify atype and size */
    t_class = H5Tget_class(atype);
    if(t_class != H5T_STRING)
    {stat = NC_EINVAL; goto done;}
    size = H5Tget_size(atype);
    if(size == 0)
    {stat = NC_EINVAL; goto done;}
    text = (char*)malloc(1+(size_t)size);
    if(text == NULL)
    {stat = NC_ENOMEM; goto done;}
    if((ntype = H5Tget_native_type(atype, H5T_DIR_DEFAULT)) < 0)
    {stat = NC_EHDFERR; goto done;}
    if((H5Aread(attid, ntype, text)) < 0)
    {stat = NC_EHDFERR; goto done;}
    /* Make sure its null terminated */
    text[(size_t)size] = '\0';
    if(propstring) {*propstring = text; text = NULL;}

done:
    if(text != NULL) free(text);
    /* Close out the ZARR objects */
    if(attid > 0 && H5Aclose(attid) < 0) stat = NC_EHDFERR;
    if(aspace > 0 && H5Sclose(aspace) < 0) stat = NC_EHDFERR;
    if(atype > 0 && H5Tclose(atype) < 0) stat = NC_EHDFERR;
    if(ntype > 0 && H5Tclose(ntype) < 0) stat = NC_EHDFERR;

    /* For certain errors, actually fail, else log that attribute was invalid and ignore */
    if(stat != NC_NOERR) {
        if(stat != NC_ENOMEM && stat != NC_EHDFERR) {
            LOG((0,"Invalid _NCProperties attribute: ignored"));
            stat = NC_NOERR;
        }
    }
    return stat;
}

static int
NCZ_write_ncproperties(NC_FILE_INFO_T* h5)
{
#ifdef SUPPRESSNCPROPERTY
    return NC_NOERR;
#else /*!SUPPRESSNCPROPERTY*/
    int stat = NC_NOERR;
    hid_t nczgrpid = -1;
    hid_t attid = -1;
    hid_t aspace = -1;
    hid_t atype = -1;
    size_t len = 0;
    NCZ_Provenance* prov = &h5->provenance;

    LOG((5, "%s", __func__));

    /* If the file is read-only, return an error. */
    if (h5->no_write)
    {stat = NC_EPERM; goto done;}

    nczgrpid = ((NCZ_GRP_INFO_T *)(h5->root_grp->format_grp_info))->hdf_grpid;

    if(H5Aexists(nczgrpid,NCPROPS) > 0) /* Already exists, no overwrite */
        goto done;

    /* Build the property if we have legit value */
    if(prov->ncproperties != NULL) {
	/* Build the ZARR string type */
	if ((atype = H5Tcopy(H5T_C_S1)) < 0)
	    {stat = NC_EHDFERR; goto done;}
	if (H5Tset_strpad(atype, H5T_STR_NULLTERM) < 0)
	    {stat = NC_EHDFERR; goto done;}
	if(H5Tset_cset(atype, H5T_CSET_ASCII) < 0)
	    {stat = NC_EHDFERR; goto done;}
	len = strlen(prov->ncproperties);
	if(H5Tset_size(atype, len) < 0)
	    {stat = NC_EFILEMETA; goto done;}
	/* Create NCPROPS attribute */
	if((aspace = H5Screate(H5S_SCALAR)) < 0)
	    {stat = NC_EFILEMETA; goto done;}
	if ((attid = H5Acreate(nczgrpid, NCPROPS, atype, aspace, H5P_DEFAULT)) < 0)
	    {stat = NC_EFILEMETA; goto done;}
	if (H5Awrite(attid, atype, prov->ncproperties) < 0)
	    {stat = NC_EFILEMETA; goto done;}
/* Verify */
#if 0
    {
        hid_t spacev, typev;
        hsize_t dsize, tsize;
        typev = H5Aget_type(attid);
        spacev = H5Aget_space(attid);
        dsize = H5Aget_storage_size(attid);
        tsize = H5Tget_size(typev);
        fprintf(stderr,"dsize=%lu tsize=%lu\n",(unsigned long)dsize,(unsigned long)tsize);
    }
#endif
    }

done:
    /* Close out the ZARR objects */
    if(attid > 0 && H5Aclose(attid) < 0) stat = NC_EHDFERR;
    if(aspace > 0 && H5Sclose(aspace) < 0) stat = NC_EHDFERR;
    if(atype > 0 && H5Tclose(atype) < 0) stat = NC_EHDFERR;

    /* For certain errors, actually fail, else log that attribute was invalid and ignore */
    switch (stat) {
    case NC_ENOMEM:
    case NC_EHDFERR:
    case NC_EPERM:
    case NC_EFILEMETA:
    case NC_NOERR:
        break;
    default:
        LOG((0,"Invalid _NCProperties attribute"));
        stat = NC_NOERR;
        break;
    }
    return stat;
#endif /*!SUPPRESSNCPROPERTY*/
}
#endif

/**************************************************/
/* Utilities */

/* Debugging */

void
ncprintprovenance(NCZ_Provenance* info)
{
    fprintf(stderr,"[%p] version=%d superblockversion=%d ncproperties=|%s|\n",
	info,
	info->version,
	info->superblockversion,
	(info->ncproperties==NULL?"":info->ncproperties));
}

/**
 * @internal
 *
 * Clear the NCPROVENANCE object; do not free it
 * @param prov Pointer to provenance object
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_clear_provenance(NCZ_Provenance* prov)
{
    LOG((5, "%s", __func__));

    if(prov == NULL) return NC_NOERR;
    nullfree(prov->ncproperties);
    memset(prov,0,sizeof(NCZ_Provenance));
    return NC_NOERR;
}

#if 0
/* Unused functions */

/**
 * @internal Parse file properties.
 *
 * @param text0 Text properties.
 * @param pairs list of parsed (key,value) pairs
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
properties_parse(const char* text0, NClist* pairs)
{
    int ret = NC_NOERR;
    char* p;
    char* q;
    char* text = NULL;

    if(text0 == NULL || strlen(text0) == 0)
        goto done;

    text = strdup(text0);
    if(text == NULL) return NC_ENOMEM;

    /* For back compatibility with version 1, translate '|' -> ',' */
    for(p=text;*p;p++) {
        if(*p == NCPROPSSEP1)
            *p = NCPROPSSEP2;
    }

    /* Walk and fill in ncinfo */
    p = text;
    while(*p) {
        char* name = p;
        char* value = NULL;
        char* next = NULL;

        /* Delimit whole (key,value) pair */
        q = locate(p,NCPROPSSEP2);
        if(*q != '\0') /* Never go beyond the final nul term */
            *q++ = '\0';
        next = q;
        /* split key and value */
        q = locate(p,'=');
        name = p;
        *q++ = '\0';
        value = q;
        /* Set up p for next iteration */
        p = next;
        nclistpush(pairs,strdup(name));
        nclistpush(pairs,strdup(value));
    }
done:
    if(text) free(text);
    return ret;
}

/* Locate a specific character and return its pointer
   or EOS if not found
   take \ escapes into account */
static char*
locate(char* p, char tag)
{
    char* next;
    int c;
    assert(p != NULL);
    for(next = p;(c = *next);next++) {
        if(c == tag)
            return next;
        else if(c == '\\' && next[1] != '\0')
            next++; /* skip escaped char */
    }
    return next; /* not found */
}

/* Utility to transfer a string to a buffer with escaping */
static void
escapify(NCbytes* buffer, const char* s)
{
    const char* p;
    for(p=s;*p;p++) {
        if(strchr(ESCAPECHARS,*p) != NULL)
            ncbytesappend(buffer,'\\');
        ncbytesappend(buffer,*p);
    }
}

/**
 * @internal Build _NCProperties attribute value.
 *
 * Convert a NCPROPINFO instance to a single string.
 * Will always convert to current format
 *
 * @param version
 * @param list Properties list
 * @param spropp Pointer that gets properties string.
 * @return ::NC_NOERR No error.
 * @return ::NC_EINVAL failed.
 * @author Dennis Heimbigner
 */
static int
build_propstring(int version, NClist* list, char** spropp)
{
    int stat = NC_NOERR;
    int i;
    NCbytes* buffer = NULL;
    char sversion[64];

    LOG((5, "%s version=%d", __func__, version));

    if(spropp != NULL) *spropp = NULL;

    if(version == 0 || version > NCPROPS_VERSION) /* unknown case */
	goto done;
     if(list == NULL)
        {stat = NC_EINVAL; goto done;}

    if((buffer = ncbytesnew()) ==  NULL)
        {stat = NC_ENOMEM; goto done;}

    /* start with version */
    ncbytescat(buffer,NCPVERSION);
    ncbytesappend(buffer,'=');
    /* Use current version */
    snprintf(sversion,sizeof(sversion),"%d",NCPROPS_VERSION);
    ncbytescat(buffer,sversion);

    for(i=0;i<nclistlength(list);i+=2) {
        char* value, *name;
        name = nclistget(list,i);
        if(name == NULL) continue;
        value = nclistget(list,i+1);
        ncbytesappend(buffer,NCPROPSSEP2); /* terminate last entry */
        escapify(buffer,name);
        ncbytesappend(buffer,'=');
        escapify(buffer,value);
    }
    /* Force null termination */
    ncbytesnull(buffer);
    if(spropp) *spropp = ncbytesextract(buffer);

done:
    if(buffer != NULL) ncbytesfree(buffer);
    return stat;
}

static int
properties_getversion(const char* propstring, int* versionp)
{
    int stat = NC_NOERR;
    int version = 0;
    /* propstring should begin with "version=dddd" */
    if(propstring == NULL || strlen(propstring) < strlen("version=") + strlen("1"))
        {stat = NC_EINVAL; goto done;} /* illegal version */
    if(memcmp(propstring,"version=",strlen("version=")) != 0)
        {stat = NC_EINVAL; goto done;} /* illegal version */
    propstring += strlen("version=");
    /* get version */
    version = atoi(propstring);
    if(version < 0)
        {stat = NC_EINVAL; goto done;} /* illegal version */
    if(versionp) *versionp = version;
done:
    return stat;
}

/**
 * @internal
 *
 * Construct the parsed provenance information
 *
 * @param prov Pointer to provenance object
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENOMEM
 * @return ::NC_EINVAL
 * @author Dennis Heimbigner
 */
static int
parse_provenance(NCZ_Provenance* prov)
{
    int stat = NC_NOERR;
    char *name = NULL;
    char *value = NULL;
    int version = 0;
    NClist* list = NULL;

    LOG((5, "%s: prov 0x%x", __func__, prov));

    if(prov->ncproperty == NULL || strlen(prov->ncproperty) < strlen("version="))
        {stat = NC_EINVAL; goto done;}
    if((list = nclistnew()) == NULL)
        {stat = NC_ENOMEM; goto done;}

    /* Do we understand the version? */
    if(prov->version > 0 && prov->version <= NCPROPS_VERSION) {/* recognized version */
        if((stat=properties_parse(prov->ncproperty,list)))
	    goto done;
        /* Remove version pair from properties list*/
        if(nclistlength(list) < 2)
            {stat = NC_EINVAL; goto done;} /* bad _NCProperties attribute */
        /* Throw away the purported version=... */
        nclistremove(list,0); /* version key */
        nclistremove(list,0); /* version value */

        /* Now, rebuild to the latest version */
	switch (version) {
	default: break; /* do nothing */
	case 1: {
            int i;
            for(i=0;i<nclistlength(list);i+=2) {
                char* newname = NULL;
                name = nclistget(list,i);
                if(name == NULL) continue; /* ignore */
                if(strcmp(name,NCPNCLIB1) == 0)
                    newname = NCPNCLIB2; /* change name */
                else if(strcmp(name,NCPNCZLIB1) == 0)
                    newname = NCPNCZLIB2;
                else continue; /* ignore */
                /* Do any rename */
                nclistset(list,i,strdup(newname));
                if(name) {free(name); name = NULL;}
            }
        } break;
	} /*switch*/
    }
    prov->properties = list;
    list = NULL;

done:
    nclistfreeall(list);
    if(name != NULL) free(name);
    if(value != NULL) free(value);
    return stat;
}

/**
 * @internal
 *
 * Clear and Free the NCZ_Provenance object
 * @param prov Pointer to provenance object
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
NCZ_free_provenance(NCZ_Provenance* prov)
{
    LOG((5, "%s", __func__));

    if(prov == NULL) return NC_NOERR;
    NCZ_clear_provenance(prov);
    free(prov);
    return NC_NOERR;
}

/* Utility to copy contents of the dfalt into an NCPROPINFO object */
static int
propinfo_default(NCZ_Properties* dst, const NCZ_Properties* dfalt)
{
    int i;
    if(dst->properties == NULL) {
        dst->properties = nclistnew();
        if(dst->properties == NULL) return NC_ENOMEM;
    }
    dst->version = dfalt->version;
    for(i=0;i<nclistlength(dfalt->properties);i++) {
        char* s = nclistget(dfalt->properties,i);
        s = strdup(s);
        if(s == NULL) return NC_ENOMEM;
        nclistpush(dst->properties,s);
    }
    return NC_NOERR;
}

#endif /*0*/

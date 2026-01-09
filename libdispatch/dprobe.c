/**
 * @file
 *
 * Copyright 2018 University Corporation for Atmospheric
 * Research/Unidata. See COPYRIGHT file for more info.
*/
#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifndef _WIN32
#ifdef USE_HDF5
#include <hdf5.h>
#endif /* USE_HDF5 */
#endif /* _WIN32 */
#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "ncdispatch.h"
#include "ncpathmgr.h"
#include "netcdf_mem.h"
#include "fbits.h"
#include "ncbytes.h"
#include "nclist.h"
#include "nclog.h"
#include "nchttp.h"
#include "ncutil.h"
#ifdef NETCDF_ENABLE_S3
#include "ncs3sdk.h"
#endif

#undef DEBUG

/* If Defined, then use only stdio for all magic number io;
   otherwise use stdio or mpio as required.
 */
#undef USE_STDIO

/**
The goal is to read a file or URI to probe its contents
in  order to figure out what kind of file we are attempting
to access.

There are basically two entry points. One to probe local file system files
and a second to probe remote sources of data.
*/


NC_FORMATX_NC3       
NC_FORMATX_NC_HDF5
NC_FORMATX_NC_HDF4
NC_FORMATX_PNETCDF   
NC_FORMATX_DAP2      
NC_FORMATX_DAP4      
NC_FORMATX_NCZARR

NC_FORMATX_UDF0      
NC_FORMATX_UDF1      

/**
Sort info for open/read/close of
file when searching for magic numbers
*/
struct MagicFile {
    const char* path;
    struct NCURI* uri;
    int omode;
    NCmodel* model;
    long long filelen;
    int use_parallel;
    int iss3;
    void* parameters; /* !NULL if inmemory && !diskless */
    FILE* fp;
#ifdef USE_PARALLEL
    MPI_File fh;
#endif
    char* curlurl; /* url to use with CURLOPT_SET_URL */
    NC_HTTP_STATE* state;
#ifdef NETCDF_ENABLE_S3
    NCS3INFO s3;
    void* s3client;
    char* errmsg;
#endif
};

/**
Most local files are detected using magic numbers.
*/
const static struct MAGIC_NUMBERS {
    const char* magic;
    unsigned formatx;
    unsigned format;
} magic_numbers = {
{"CDF\001", NC_FORMATX_NC3, NC_CLASSIC_MODEL},
{"CDF\002", NC_FORMATX_NC3, NC_64BIT_OFFSET},
{"CDF\005", NC_FORMATX_NC3, NC_64BIT_DATA},
{"CDF\005", NC_FORMATX_ PNETCDF | Use MPIO},
{"\211HDF\r\n\032\n", NC_FORMATX_NC_HDF5, NC_FORMAT_NETCDF4},
{"\x0e\x03\x13\x01", NC_FORMATX_NC_HDF4, NC_FORMAT_NETCDF4},
{NULL,0,0}
};

#define modelcomplete(model) ((model)->impl != 0)

#ifdef DEBUG
static void dbgflush(void)
{
    fflush(stdout);
    fflush(stderr);
}

static void
fail(int err)
{
    return;
}

static int
check(int err)
{
    if(err != NC_NOERR)
	fail(err);
    return err;
}
#else
#define check(err) (err)
#endif

/*
Define a table of "mode=" string values
from which the implementation can be inferred.
Note that only cases that can currently
take URLs are included.
*/
static struct FORMATMODES {
    const char* tag;
    const int impl; /* NC_FORMATX_XXX value */
    const int format; /* NC_FORMAT_XXX value */
} formatmodes[] = {
{"dap2",NC_FORMATX_DAP2,NC_FORMAT_CLASSIC},
{"dap4",NC_FORMATX_DAP4,NC_FORMAT_NETCDF4},
{"netcdf-3",NC_FORMATX_NC3,0}, /* Might be e.g. cdf5 */
{"classic",NC_FORMATX_NC3,0}, /* ditto */
{"netcdf-4",NC_FORMATX_NC4,NC_FORMAT_NETCDF4},
{"enhanced",NC_FORMATX_NC4,NC_FORMAT_NETCDF4},
{"udf0",NC_FORMATX_UDF0,0},
{"udf1",NC_FORMATX_UDF1,0},
{"nczarr",NC_FORMATX_NCZARR,NC_FORMAT_NETCDF4},
{"zarr",NC_FORMATX_NCZARR,NC_FORMAT_NETCDF4},
{"bytes",NC_FORMATX_NC4,NC_FORMAT_NETCDF4}, /* temporary until 3 vs 4 is determined */
{NULL,0},
};

/* Replace top-level name with defkey=defvalue */
static const struct MACRODEF {
    char* name;
    char* defkey;
    char* defvalues[4];
} macrodefs[] = {
{"zarr","mode",{"nczarr","zarr",NULL}},
{"dap2","mode",{"dap2",NULL}},
{"dap4","mode",{"dap4",NULL}},
{"s3","mode",{"s3","nczarr",NULL}},
{"bytes","mode",{"bytes",NULL}},
{"xarray","mode",{"zarr", NULL}},
{"noxarray","mode",{"nczarr", "noxarray", NULL}},
{"zarr","mode",{"nczarr","zarr", NULL}},
{"gs3","mode",{"gs3","nczarr",NULL}}, /* Google S3 API */
{NULL,NULL,{NULL}}
};

/*
Mode inferences: if mode contains key value, then add the inferred value;
Warning: be careful how this list is constructed to avoid infinite inferences.
In order to (mostly) avoid that consequence, any attempt to
infer a value that is already present will be ignored.
This effectively means that the inference graph
must be a DAG and may not have cycles.
You have been warned.
*/
static const struct MODEINFER {
    char* key;
    char* inference;
} modeinferences[] = {
{NULL,NULL}
};

/* Mode negations: if mode contains key, then remove all occurrences of the inference and repeat */
static const struct MODEINFER modenegations[] = {
{"bytes","nczarr"}, /* bytes negates (nc)zarr */
{"bytes","zarr"}, /* ditto */
{NULL,NULL}
};

/* Map FORMATX to readability to get magic number */
static struct Readable {
    int impl;
    int readable;
} readable[] = {
{NC_FORMATX_NC3,1},
{NC_FORMATX_NC_HDF5,1},
{NC_FORMATX_NC_HDF4,1},
{NC_FORMATX_PNETCDF,1},
{NC_FORMATX_DAP2,0},
{NC_FORMATX_DAP4,0},
{NC_FORMATX_UDF0,1},
{NC_FORMATX_UDF1,1},
{NC_FORMATX_NCZARR,0}, /* eventually make readable */
{0,0},
};

/* Define the known URL protocols and their interpretation */
static struct NCPROTOCOLLIST {
    const char* protocol;
    const char* substitute;
    const char* fragments; /* arbitrary fragment arguments */
} ncprotolist[] = {
    {"http",NULL,NULL},
    {"https",NULL,NULL},
    {"file",NULL,NULL},
    {"dods","http","mode=dap2"},
    {"dap4","http","mode=dap4"},
    {"s3","s3","mode=s3"},
    {"gs3","gs3","mode=gs3"},
    {NULL,NULL,NULL} /* Terminate search */
};

/**************************************************/
/** @internal Magic number for HDF5 files. To be consistent with
 * H5Fis_hdf5, use the complete HDF5 magic number */
static char HDF5_SIGNATURE[MAGIC_NUMBER_LEN] f= "\211HDF\r\n\032\n";

#define modelcomplete(model) ((model)->impl != 0)

/**************************************************/
/**
 * @internal Given an existing file, figure out its format and return
 * that format value (NC_FORMATX_XXX) in model arg. Assume any path
 * conversion was already performed at a higher level.
 *
 * @param path File name.
 * @param flags
 * @param use_parallel
 * @param parameters
 * @param model Pointer that gets the model to use for the dispatch table.
 * @param uri parsed path
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
*/
int
NC_probe_file(const char *path, int omode, int use_parallel,
		   void *parameters, NCmodel* model, NCURI* uri)
{
    char magic[NC_MAX_MAGIC_NUMBER_LEN];
    int status = NC_NOERR;
    struct MagicFile magicinfo;
#ifdef _WIN32
    NC* nc = NULL;
#endif

    memset((void*)&magicinfo,0,sizeof(magicinfo));

#ifdef _WIN32 /* including MINGW */
    /* Windows does not handle multiple handles to the same file very well.
       So if file is already open/created, then find it and just get the
       model from that. */
    if((nc = find_in_NCList_by_name(path)) != NULL) {
	int format = 0;
	/* Get the model from this NC */
	if((status = nc_inq_format_extended(nc->ext_ncid,&format,NULL))) goto done;
	model->impl = format;
	if((status = nc_inq_format(nc->ext_ncid,&format))) goto done;
	model->format = format;
	goto done;
    }
#endif

    magicinfo.path = path; /* do not free */
    magicinfo.uri = uri; /* do not free */
    magicinfo.omode = omode;
    magicinfo.model = model; /* do not free */
    magicinfo.parameters = parameters; /* do not free */
#ifdef USE_STDIO
    magicinfo.use_parallel = 0;
#else
    magicinfo.use_parallel = use_parallel;
#endif

    if((status = openmagic(&magicinfo))) goto done;

    /* Verify we have a large enough file */
    if(MAGIC_NUMBER_LEN >= (unsigned long long)magicinfo.filelen)
	{status = NC_ENOTNC; goto done;}
    if((status = readmagic(&magicinfo,0L,magic)) != NC_NOERR) {
	status = NC_ENOTNC;
	goto done;
    }

    /* Look at the magic number */
    if(NC_interpret_magic_number(magic,model) == NC_NOERR
	&& model->format != 0) {
        if (use_parallel && (model->format == NC_FORMAT_NC3 || model->impl == NC_FORMATX_NC3))
            /* this is called from nc_open_par() and file is classic */
            model->impl = NC_FORMATX_PNETCDF;
        goto done; /* found something */
    }

    /* Remaining case when implementation is an HDF5 file;
       search forward at starting at 512
       and doubling to see if we have HDF5 magic number */
    {
	size_t pos = 512L;
        for(;;) {
	    if((pos+MAGIC_NUMBER_LEN) > (unsigned long long)magicinfo.filelen)
		{status = NC_ENOTNC; goto done;}
            if((status = readmagic(&magicinfo,pos,magic)) != NC_NOERR)
	        {status = NC_ENOTNC; goto done; }
            NC_interpret_magic_number(magic,model);
            if(model->impl == NC_FORMATX_NC4) break;
	    /* double and try again */
	    pos = 2*pos;
        }
    }
done:
    closemagic(&magicinfo);
    return check(status);
}

/**
\internal
\ingroup datasets
Provide open, read and close for use when searching for magic numbers
*/
static int
openmagic(struct MagicFile* file)
{
    int status = NC_NOERR;
    if(fIsSet(file->omode,NC_INMEMORY)) {
	/* Get its length */
	NC_memio* meminfo = (NC_memio*)file->parameters;
        assert(meminfo != NULL);
	file->filelen = (long long)meminfo->size;
	goto done;
    }
    if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
	/* Construct a URL minus any fragment */
        file->curlurl = ncuribuild(file->uri,NULL,NULL,NCURISVC);
	/* Open the curl handle */
        if((status=nc_http_open(file->path, &file->state))) goto done;
	if((status=nc_http_size(file->state,&file->filelen))) goto done;
#else /*!BYTERANGE*/
	{status = NC_ENOTBUILT;}
#endif /*BYTERANGE*/
	goto done;
    }	
#ifdef USE_PARALLEL
    if (file->use_parallel) {
	int retval;
	MPI_Offset size;
        assert(file->parameters != NULL);
	if((retval = MPI_File_open(((NC_MPI_INFO*)file->parameters)->comm,
                                   (char*)file->path,MPI_MODE_RDONLY,
                                   ((NC_MPI_INFO*)file->parameters)->info,
                                   &file->fh)) != MPI_SUCCESS) {
#ifdef MPI_ERR_NO_SUCH_FILE
	    int errorclass;
	    MPI_Error_class(retval, &errorclass);
	    if (errorclass == MPI_ERR_NO_SUCH_FILE)
#ifdef NC_ENOENT
	        status = NC_ENOENT;
#else /*!NC_ENOENT*/
		status = errno;
#endif /*NC_ENOENT*/
	    else
#endif /*MPI_ERR_NO_SUCH_FILE*/
	        status = NC_EPARINIT;
	    file->fh = MPI_FILE_NULL;
	    goto done;
	}
	/* Get its length */
	if((retval=MPI_File_get_size(file->fh, &size)) != MPI_SUCCESS)
	    {status = NC_EPARINIT; goto done;}
	file->filelen = (long long)size;
	goto done;
    }
#endif /* USE_PARALLEL */
    {
        if (file->path == NULL || strlen(file->path) == 0)
            {status = NC_EINVAL; goto done;}
        file->fp = NCfopen(file->path, "r");
        if(file->fp == NULL)
	    {status = errno; goto done;}
	/* Get its length */
	{
	    int fd = fileno(file->fp);
#ifdef _WIN32
	    __int64 len64 = _filelengthi64(fd);
	    if(len64 < 0)
		{status = errno; goto done;}
	    file->filelen = (long long)len64;
#else
	    off_t size;
	    size = lseek(fd, 0, SEEK_END);
	    if(size == -1)
		{status = errno; goto done;}
		file->filelen = (long long)size;
#endif
	}
        int retval2 = fseek(file->fp, 0L, SEEK_SET);        
	    if(retval2 != 0)
		{status = errno; goto done;}
    }
done:
    return check(status);
}

static int
readmagic(struct MagicFile* file, size_t pos, char* magic)
{
    int status = NC_NOERR;
    NCbytes* buf = ncbytesnew();

    memset(magic,0,MAGIC_NUMBER_LEN);
    if(fIsSet(file->omode,NC_INMEMORY)) {
	char* mempos;
	NC_memio* meminfo = (NC_memio*)file->parameters;
	if((pos + MAGIC_NUMBER_LEN) > meminfo->size)
	    {status = NC_EINMEMORY; goto done;}
	mempos = ((char*)meminfo->memory) + pos;
	memcpy((void*)magic,mempos,MAGIC_NUMBER_LEN);
#ifdef DEBUG
	printmagic("XXX: readmagic",magic,file);
#endif
    } else if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
        size64_t start = (size64_t)pos;
        size64_t count = MAGIC_NUMBER_LEN;
        status = nc_http_read(file->state, start, count, buf);
        if (status == NC_NOERR) {
            if (ncbyteslength(buf) != count)
                status = NC_EINVAL;
            else
                memcpy(magic, ncbytescontents(buf), count);
        }
#endif
    } else {
#ifdef USE_PARALLEL
        if (file->use_parallel) {
	    MPI_Status mstatus;
	    int retval;
	    if((retval = MPI_File_read_at_all(file->fh, pos, magic,
			    MAGIC_NUMBER_LEN, MPI_CHAR, &mstatus)) != MPI_SUCCESS)
	        {status = NC_EPARINIT; goto done;}
        }
        else
#endif /* USE_PARALLEL */
        { /* Ordinary read */
            long i;
            i = fseek(file->fp, (long)pos, SEEK_SET);
            if (i < 0) { status = errno; goto done; }
            ncbytessetlength(buf, 0);
            if ((status = NC_readfileF(file->fp, buf, MAGIC_NUMBER_LEN))) goto done;
            memcpy(magic, ncbytescontents(buf), MAGIC_NUMBER_LEN);
        }
    }

done:
    ncbytesfree(buf);
    if(file && file->fp) clearerr(file->fp);
    return check(status);
}

/**
 * Close the file opened to check for magic number.
 *
 * @param file pointer to the MagicFile struct for this open file.
 * @returns NC_NOERR for success
 * @returns NC_EPARINIT if there was a problem closing file with MPI
 * (parallel builds only).
 * @author Dennis Heimbigner
 */
static int
closemagic(struct MagicFile* file)
{
    int status = NC_NOERR;

    if(fIsSet(file->omode,NC_INMEMORY)) {
	/* noop */
    } else if(file->uri != NULL) {
#ifdef NETCDF_ENABLE_BYTERANGE
	    status = nc_http_close(file->state);
#endif
	    nullfree(file->curlurl);
    } else {
#ifdef USE_PARALLEL
        if (file->use_parallel) {
	    int retval;
	    if(file->fh != MPI_FILE_NULL
	       && (retval = MPI_File_close(&file->fh)) != MPI_SUCCESS)
		    {status = NC_EPARINIT; return status;}
        } else
#endif
        {
	    if(file->fp) fclose(file->fp);
        }
    }
    return status;
}

/*!
  Interpret the magic number found in the header of a netCDF file.
  This function interprets the magic number/string contained in the header of a netCDF file and sets the appropriate NC_FORMATX flags.

  @param[in] magic Pointer to a character array with the magic number block.
  @param[out] model Pointer to an integer to hold the corresponding netCDF type.
  @param[out] version Pointer to an integer to hold the corresponding netCDF version.
  @returns NC_NOERR if a legitimate file type found
  @returns NC_ENOTNC otherwise

\internal
\ingroup datasets

*/
static int
NC_interpret_magic_number(char* magic, NCmodel* model)
{
    int status = NC_NOERR;
    int tmpimpl = 0;
    /* Look at the magic number */
    if(model->impl == NC_FORMATX_UDF0 || model->impl == NC_FORMATX_UDF1)
        tmpimpl = model->impl;

    /* Use the complete magic number string for HDF5 */
    if(memcmp(magic,HDF5_SIGNATURE,sizeof(HDF5_SIGNATURE))==0) {
	model->impl = NC_FORMATX_NC4;
	model->format = NC_FORMAT_NETCDF4;
	goto done;
    }
    if(magic[0] == '\016' && magic[1] == '\003'
              && magic[2] == '\023' && magic[3] == '\001') {
	model->impl = NC_FORMATX_NC_HDF4;
	model->format = NC_FORMAT_NETCDF4;
	goto done;
    }
    if(magic[0] == 'C' && magic[1] == 'D' && magic[2] == 'F') {
        if(magic[3] == '\001') {
	    model->impl = NC_FORMATX_NC3;
	    model->format = NC_FORMAT_CLASSIC;
	    goto done;
	}
        if(magic[3] == '\002') {
	    model->impl = NC_FORMATX_NC3;
	    model->format = NC_FORMAT_64BIT_OFFSET;
	    goto done;
        }
        if(magic[3] == '\005') {
	  model->impl = NC_FORMATX_NC3;
	  model->format = NC_FORMAT_64BIT_DATA;
	  goto done;
	}
     }
     /* No match  */
     if (!tmpimpl) 
         status = NC_ENOTNC;         

     goto done;

done:
     /* if model->impl was UDF0 or UDF1 on entry, make it so on exit */
     if(tmpimpl)
         model->impl = tmpimpl;
     /* if this is a UDF magic_number update the model->impl */
     if (strlen(UDF0_magic_number) && !strncmp(UDF0_magic_number, magic,
                                               strlen(UDF0_magic_number)))
     {
         model->impl = NC_FORMATX_UDF0;
         status = NC_NOERR;
     }
     if (strlen(UDF1_magic_number) && !strncmp(UDF1_magic_number, magic,
                                               strlen(UDF1_magic_number)))
     {
         model->impl = NC_FORMATX_UDF1;
         status = NC_NOERR;
     }    

     return check(status);
}

/* Return NC_NOERR if path is a DAOS container; return NC_EXXX otherwise */
static int
isdaoscontainer(const char* path)
{
    int stat = NC_ENOTNC; /* default is that this is not a DAOS container */
#ifndef _WIN32
#ifdef USE_HDF5
#if H5_VERSION_GE(1,12,0)
    htri_t accessible;
    hid_t fapl_id;
    int rc;
    /* Check for a DAOS container */
    if((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) < 0) {stat = NC_EHDFERR; goto done;}
    H5Pset_fapl_sec2(fapl_id);
    accessible = H5Fis_accessible(path, fapl_id);
    H5Pclose(fapl_id); /* Ignore any error */
    rc = 0;
    if(accessible > 0) {
#ifdef HAVE_SYS_XATTR_H
	ssize_t xlen;
#ifdef __APPLE__
	xlen = listxattr(path, NULL, 0, 0);
#else
	xlen = listxattr(path, NULL, 0);
#endif
        if(xlen > 0) {
  	    char* xlist = NULL;
	    char* xvalue = NULL;
	    char* p;
	    char* endp;
	    if((xlist = (char*)calloc(1,(size_t)xlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
#ifdef __APPLE__
	    (void)listxattr(path, xlist, (size_t)xlen, 0); /* Get xattr names */
#else
	    (void)listxattr(path, xlist, (size_t)xlen); /* Get xattr names */
#endif
	    p = xlist; endp = p + xlen; /* delimit names */
	    /* walk the list of xattr names */
	    for(;p < endp;p += (strlen(p)+1)) {
		/* The popen version looks for the string ".daos";
                   It would be nice if we know whether that occurred
		   int the xattr's name or it value.
		   Oh well, we will do the general search */
		/* Look for '.daos' in the key */
		if(strstr(p,".daos") != NULL) {rc = 1; break;} /* success */
		/* Else get the p'th xattr's value size */
#ifdef __APPLE__
		xlen = getxattr(path, p, NULL, 0, 0, 0);
#else
		xlen = getxattr(path, p, NULL, 0);
#endif
		if((xvalue = (char*)calloc(1,(size_t)xlen))==NULL)
		    {stat = NC_ENOMEM; goto done;}
		/* Read the value */
#ifdef __APPLE__
		(void)getxattr(path, p, xvalue, (size_t)xlen, 0, 0);
#else
		(void)getxattr(path, p, xvalue, (size_t)xlen);
#endif
		/* Look for '.daos' in the value */
		if(strstr(xvalue,".daos") != NULL) {rc = 1; break;} /* success */
	    }
        }
#else /*!HAVE_SYS_XATTR_H*/

#ifdef HAVE_GETFATTR
	{
	    FILE *fp;
	    char cmd[4096];
	    memset(cmd,0,sizeof(cmd));
        snprintf(cmd,sizeof(cmd),"getfattr %s | grep -c '.daos'",path);
        if((fp = popen(cmd, "r")) != NULL) {
               fscanf(fp, "%d", &rc);
               pclose(fp);
	    }
    }
#else /*!HAVE_GETFATTR*/
    /* We just can't test for DAOS container.*/
    rc = 0;
#endif /*HAVE_GETFATTR*/
#endif /*HAVE_SYS_XATTR_H*/
    }
    /* Test for DAOS container */
    stat = (rc == 1 ? NC_NOERR : NC_ENOTNC);
done:
#endif
#endif
#endif
    errno = 0; /* reset */
    return stat;
}

#ifdef DEBUG
static void
printmagic(const char* tag, char* magic, struct MagicFile* f)
{
    int i;
    fprintf(stderr,"%s: ispar=%d magic=",tag,f->use_parallel);
    for(i=0;i<MAGIC_NUMBER_LEN;i++) {
        unsigned int c = (unsigned int)magic[i];
	c = c & 0x000000FF;
	if(c == '\n')
	    fprintf(stderr," 0x%0x/'\\n'",c);
	else if(c == '\r')
	    fprintf(stderr," 0x%0x/'\\r'",c);
	else if(c < ' ')
	    fprintf(stderr," 0x%0x/'?'",c);
	else
	    fprintf(stderr," 0x%0x/'%c'",c,c);
    }
    fprintf(stderr,"\n");
    fflush(stderr);
}

static void
printlist(NClist* list, const char* tag)
{
    int i;
    fprintf(stderr,"%s:",tag);
    for(i=0;i<nclistlength(list);i++) {
        fprintf(stderr," %s",(char*)nclistget(list,i));
	fprintf(stderr,"[%p]",(char*)nclistget(list,i));
    }
    fprintf(stderr,"\n");
    dbgflush();
}


#endif

/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/*
Provide a simple, file based zmap implementation
*/

#include "zincludes.h"
#include "ncwinpath.h"
#include <errno.h>
#include <stdarg.h>
#ifdef HAVE_FTW_H
#include <ftw.h>
#endif
#ifdef HAVE_DIRENT_H
#endif

/* Constants */
#define SEP '/'
#define ESC '\\'

#define CONTENTFILENAME ".content"

#ifdef _WIN32
#define RMODE "rb"
#define RWMODE "rwb"
#else
#define RMODE "r"
#define RWMODE "rw"
#endif

/*Mnemonics*/
#define RW 1

#ifdef _WIN32
#define CREATEFLAGS (O_RDWR|O_TRUNC|O_CREAT|O_BINARY)
#define CREATEMODE (_S_IREAD|_S_IWRITE)
#define OPENRFLAGS (O_RDONLY|O_BINARY)
#define OPENRWFLAGS (O_RDWR|O_BINARY)
#define OPENRWMODE (_S_IREAD|_S_IWRITE)
#define OPENRMODE (_S_IREAD)
#else
#define CREATEFLAGS (O_RDWR|O_TRUNC|O_CREAT|O_EXCL)
#define CREATEMODE (S_IRWXU)
#define OPENRFLAGS (O_RDONLY)
#define OPENRWFLAGS (O_RDWR)
#define OPENMODE (S_IRWXU)
#define OPENRMODE (S_IRWXU)
#define OPENRWMODE (S_IRWXU)
#endif /*!_WIN32*/

/* Implementation Specific data */

typedef struct NCZFILE {
    NCZMAP map;
    /* File implementation specific data follows */
} NCZFILE;

/*Forward*/
static int zfile_synch(NCZFILE* map0);
static char* pathappend(const char* path, const char* suffix);
static char* pathappendn(const char* path, ...);
static int deltree(const char* path);
static int createroot(const char* path);
static int opencontent(NCZFILE*, const char* rpath, int rw, FILE** fp);
static int pathlast(const char* path, const char** lastp);
#if 0
static int createobject(const char* parent, const char* name);
static int readcontent(NCZFILE*, const char* rpath, NCbytes* content);
static int writecontent(NCZFILE*, const char* rpath, NCbytes* content);
#endif

/*
nFor this simple file map, the root dataset object
is the initial path given to open/create.
We mimic the implicit zmap tree by making
every node in that tree be a file system directory.
Then, containment is as ususal for a directory.
In addition, since every node is an object that has
content, we assume that there is a data file in every
tree node directory called .content and containing the
associated data for that object.
*/

static int
zfile_create(NCZMAP_API* api, const char *suri, int omode, unsigned int flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZFILE* map = NULL;
    NCURI* uri = NULL;
    const char* path = NULL;

    if(mapp) *mapp = NULL;

    if((map=calloc(1,sizeof(NCZFILE))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    map->map.api = api;
    map->map.flags = flags;
    map->map.mode = omode;
    /* Process parameters here, if there are any */
    if(parameters != NULL) {
    }

    /* Parse the uri; either path or file: uri is ok */
    if(ncuriparse(suri,&uri) == NCU_OK) {
	if(strcmp(uri->protocol,"file") == 0) {
	    path = uri->path;
	} else
	    {stat = NC_EURL; goto done;} /* not file:// url */
    } else {
	path = suri; /* regular old file path */
    }

    /* save the dataset path */
    if((map->map.path = strdup(path)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    
    /* Create the root directory object */
    if((stat=createroot(path))) goto done;

    if(mapp) *mapp = (NCZMAP*)map;
done:
    if(stat) {
	if(map)	api->close((NCZMAP*)map);
    }
    return stat;
}

static int
zfile_open(NCZMAP_API* api, const char *path, int mode, unsigned int flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZFILE* map = NULL;
    int readwrite = (mode & NC_WRITE ? 1 : 0);

    if(mapp) *mapp = NULL;

    if((map=calloc(1,sizeof(NCZFILE))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    map->map.api = api;
    map->map.flags = flags;
    map->map.mode = mode;
    if((map->map.path = strdup(path)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    /* Process parameters here, if there are any */
    if(parameters != NULL) {
    }

    /* Verify that the root exists and is mode accessible */
    if((stat = NCaccess(path,
		(readwrite ? (ACCESS_MODE_EXISTS|ACCESS_MODE_RW)
		           : (ACCESS_MODE_EXISTS|ACCESS_MODE_R))))) goto done;

#if 0
    /* Open the root directory object, possibly read-only */
    if((map->fd = NCopen3(path,
			  (readwrite?OPENRWFLAGS:OPENRFLAGS),
			  (readwrite?OPENRWMODE:OPENRWMODE))) < 0) goto done;
#endif

    if(mapp) *mapp = (NCZMAP*)map;
done:
    return stat;
}

/**************************************************/
/* API Wrapper */

static int
zfile_close(NCZMAP* map0)
{
    int stat = NC_NOERR;
    NCZFILE* map = (NCZFILE*)map0;

    if((stat=zfile_synch(map))) goto done;
    nullfree(map->map.path);
    free(map);    
done:
    return stat;
}

/* Remove a whole directory tree */
static int
zfile_clear(NCZMAP* map0)
{
    int stat = NC_NOERR;
    NCZFILE* map = (NCZFILE*)map0;

    if((stat=deltree(map->map.path))) goto done;
done:
    return stat;
}

/* Get the content size of an object */
static int
zfile_len(NCZMAP* map0, const char* rpath, fileoffset_t* lenp)
{
    int stat = NC_NOERR;
    FILE* cf = NULL;
    long offset = 0;
    NCZFILE* map = (NCZFILE*)map0;

    /* Open the content file */
    if((stat=opencontent(map,rpath,!RW,&cf)) < 0) goto done;
    if(fseek(cf,0,SEEK_END) < 0) {stat = errno; goto done;}
    offset = ftell(cf);
    if(lenp) *lenp = (fileoffset_t)offset;
done:
    if(cf) fclose(cf);
    return stat;
}

#if 0
static int
zfile_exists(NCZMAP* map0, const char* rpath)
{
    int stat = NC_NOERR;
    char* path = NULL;
    NCZFILE* map = (NCZFILE*)map0;

    if((path=pathappend(map->map.path,rpath))) goto done;
    if(NCaccess(path,ACCESS_MODE_EXISTS) == 0)
	{stat = NC_EEXIST; goto done;}
done:
    nullfree(path);
    return stat;
}
#endif

static int
zfile_read(NCZMAP* map0, const char* rpath, fileoffset_t start, fileoffset_t count, char** contentp)
{
    int stat = NC_NOERR;
    NCZFILE* map = (NCZFILE*)map0;
    char* path = NULL;
    FILE* stream = NULL;
    fileoffset_t remain;
    NCbytes* buf = NULL;
    char partial[1<<16];

    if((path=pathappend(map->map.path,rpath))) goto done;

    stream = NCfopen(path,RMODE);
    if(stream == NULL) {stat=errno; goto done;}
    remain = count;
    if((stat=fseek(stream,start,SEEK_SET))) {stat = errno; goto done;}
    buf = ncbytesnew();
    ncbytessetalloc(buf,count);
    while(remain > 0) {
	size_t red;
	fileoffset_t toread = sizeof(partial);
	if(remain < toread) toread = remain;
	red = fread(partial, 1, toread, stream);
	if(ferror(stream)) {stat = errno; goto done;}
	if(feof(stream)) {stat = NC_EIO; goto done;}
	remain -= red;
	ncbytesappendn(buf,partial,red);
    }
    if(contentp) *contentp = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    if(stream) fclose(stream);
    nullfree(path);
    return stat;
}

static int
zfile_write(NCZMAP* map0, const char* rpath, fileoffset_t start, fileoffset_t count, const char* content)
{
    int stat = NC_NOERR;
    NCZFILE* map = (NCZFILE*)map0;
    char* path = NULL;
    FILE* stream = NULL;
    const void* p;
    fileoffset_t remain;

    if((path=pathappend(map->map.path,rpath))) goto done;

#ifdef _WIN32
    stream = NCfopen(path,"wb");
#else
    stream = NCfopen(path,"w");
#endif
    if(stream == NULL) {stat=errno; goto done;}
    p = content;
    remain = count;
    if((stat=fseek(stream,start,SEEK_SET))) {stat = errno; goto done;}
    while(remain > 0) {
	size_t written = fwrite(p, 1, remain, stream);
	if(ferror(stream)) {stat = errno; goto done;}
	if(feof(stream)) {stat = NC_EIO; goto done;}
	remain -= written;
	p += written;
    }

done:
    if(stream) fclose(stream);
    nullfree(path);
    return stat;
}

static int
zfile_rename(NCZMAP* map0, const char* oldrpath, const char* newname)
{
    int stat = NC_NOERR;
    NCZFILE* map = (NCZFILE*)map0;
    char* oldpath = NULL;
    char* newpath = NULL;
    char* newrpath = NULL;
    const char* lastseg = NULL;

    /* Create the new relative path by replacing last element in path
       with newname*/
    if((stat = pathlast(oldrpath,&lastseg))) goto done;
    if(*lastseg == '\0') /* no slash in oldrpath, just replace */
	newrpath = strdup(newname);
    else { /* Rebuild with last segment replaced by newname */
        ptrdiff_t delta = (lastseg - oldrpath);
	size_t newlen = delta + strlen(newname);
	newrpath = malloc(newlen+1);
	memcpy(newrpath,oldrpath,delta);
	newrpath[delta] = '\0';
	strlcat(newrpath,newname,newlen);	
    }
    if((oldpath=pathappend(map->map.path,oldrpath))) goto done;
    if((newpath=pathappend(map->map.path,newrpath))) goto done;
  
    /* rename */
    if((stat=NCrename(oldpath,newpath))) {stat = errno; goto done;}

done:
    nullfree(newpath);
    nullfree(oldpath);
    nullfree(newrpath);
    return stat;
}

/**************************************************/
/* Utilities */

static int
zfile_synch(NCZFILE* map)
{
    return NC_NOERR;
}


/*
Given a root directory fd, walk it and unlink
everything below it.
*/
/* Helper function*/
static int
unlink_cb(const char *fpath, const struct stat* sb, int typefl, struct FTW* buf)
{
    int rv = remove(fpath);
    (void)sb; /* Keep compiler quiet*/
    (void)typefl;
    (void)buf;
    return rv;
}

static int
deltree(const char* path)
{
#ifdef HAVE_FTW_H
    return nftw(path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
#else
#error "nftw() not available"
#endif
}

/* Add suffix to a path; caller frees */
static char*
pathappend(const char* path, const char* suffix)
{
    return pathappendn(path,suffix,NULL);
}

/* Add multiple suffixes to a path; caller frees */
static char*
pathappendn(const char* path, ...)
{
    va_list suffixes;
    char* newpath = NULL;
    NCbytes* fullpath = ncbytesnew();
    ncbytescat(fullpath,path);
    va_start(suffixes, path);
    for(;;) {
	char* suffix = va_arg(suffixes,char*);
	if(suffix == NULL) break;
	ncbytesappend(fullpath,SEP);
    }
    va_end(suffixes);
    ncbytesnull(fullpath);
    newpath = ncbytesextract(fullpath);
    ncbytesfree(fullpath);
    return newpath;
}

/* Create an object as a the root object */
static int
createroot(const char* path)
{
    int stat = NC_NOERR;
    FILE* f = NULL; /* Use stdio since it is more windows compatible */
    char* contentpath = NULL;

    if(path == NULL || strlen(path) == 0)
	{stat = NC_EINTERNAL; goto done;}

    /* Verify that path does not exist */
    if(NCaccess(path,ACCESS_MODE_EXISTS) == 0)
	{stat = NC_EEXIST; goto done;}
    /* Create */
    if(NCmkdir(path,CREATEMODE) < 0)
	{stat = errno; goto done;}
    /* Create the contents file */
    /* Create the contents file */
    if((contentpath = pathappend(path,CONTENTFILENAME)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((f = NCfopen(contentpath,RWMODE)) == NULL)
	{stat = errno; goto done;}
done:
    if(f != NULL) fclose(f);
    nullfree(contentpath);
    return stat;
}

#if 0
/* Create an object as a directory under the parent group
   as specified by "parent".
*/
static int
createobject(const char* parent, const char* name)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    char* opath = NULL;
    char* contentpath = NULL;

    if(parent == NULL || strlen(parent) == 0)
	{stat = NC_EINTERNAL; goto done;}
    if(name == NULL || strlen(name) == 0)
	{stat = NC_EINTERNAL; goto done;}
	
    /* Verify that the name is not the content file name */
    if(strcmp(name,CONTENTFILENAME)==0) 
	{stat = NC_EBADID; goto done;}

    /* Check the access to the parent */
    if(NCaccess(parent,ACCESS_MODE_EXISTS|ACCESS_MODE_RW) != 0)
	{stat = NC_EACCESS; goto done;}
    /* Create the full object path */
    if((opath = pathappend(parent,name)) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* Verify that opath does not exist */
    if(NCaccess(opath,ACCESS_MODE_EXISTS) == 0)
	{stat = NC_EEXIST; goto done;}
    /* Create */
    if(NCmkdir(opath,CREATEMODE) < 0)
	{stat = errno; goto done;}
    /* Create the contents file */
    if((contentpath = pathappend(opath,CONTENTFILENAME)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((f = NCfopen(contentpath,RWMODE)) < 0)
	{stat = errno; goto done;}
done:
    if(f != NULL) fclose(f);
    nullfree(opath);
    nullfree(contentpath);
    return stat;
}
#endif

static int
opencontent(NCZFILE* map, const char* rpath, int rw, FILE** fp)
{
    int stat = NC_NOERR;
    FILE* f = NULL;
    char* path = NULL;

    if((path = pathappendn(map->map.path,rpath,CONTENTFILENAME,NULL)))
	return NC_ENOMEM;
    if((f = NCfopen(path,(rw?RWMODE:RMODE))) == NULL)
	{stat = errno; goto done;}
    if(fp) *fp = f;
done:
    if(stat && f != NULL) fclose(f);
    nullfree(path);
    return stat;
}

/* Locate the position of the last segment
   of a slash separated path, allowing escaping.
   Path may be relative.
   Returns pointer to the character past the last slash.
*/
static int
pathlast(const char* path, const char** lastp)
{
    int stat = NC_NOERR;
    const char* p;
    const char* last = NULL;

    if(path == NULL || path[0] == '\0')
	{stat = NC_EINVAL; goto done;}
	
    /* We cannot use strchr because of the possibility of escaped chars */
    last = NULL;
    for(p=path;*p;p++) {
	switch (*p) {
	case '\0':
	    goto exitloop;
	case SEP: 
	    last = p;
	    break;	
	case ESC:
	    p++; /* skip escape char */
	    break;
	default:
	    break;
	}
    }
exitloop:
    if(last == NULL)         
	last = p; /* point to trailing nul */
    else
	last = p+1; /* point past the last slash */
    if(lastp) *lastp = last;    
done:
    return stat;
}

/**************************************************/


static NCZMAP_API zmap_file = {
NCZM_FILE,
zfile_create,
zfile_open,
zfile_close,
zfile_clear,
zfile_len,
zfile_read,
zfile_write,
zfile_rename,
};

NCZMAP_API* zmap_file_api = &zmap_file;

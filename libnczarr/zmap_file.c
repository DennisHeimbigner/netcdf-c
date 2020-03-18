/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/* Not sure this has any effect */
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1

#include "zincludes.h"

#include <errno.h>
#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <iostream>
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <dirent.h>

#include "fbits.h"
#include "ncwinpath.h"

#undef DEBUG

#define NCZM_FILE_V1 1

#ifdef S_IRUSR
#define NC_DEFAULT_CREAT_MODE \
        (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH) /* 0666 */

#else
#define NC_DEFAULT_CREAT_MODE 0666
#endif

/*
Do a simple mapping of our simplified map model
to a file system.

For the object API, the mapping is as follows:
1. Every object (e.g. group or array) is mapped to a directory.
2. Meta data objects (e.g. .zgroup, .zarray, etc) are kept as a
   char typed file in the corresponding directory.
3. Actual variable data (for e.g. chunks) is stored as
   a ubyte typed object with the chunk name.
*/


/* define the attr/var name containing an objects content */
#define ZCONTENT "data"

/* Mnemonic */
#define CREATEGROUP 1

/* Define the "subclass" of NCZMAP */
typedef struct ZFMAP {
    NCZMAP map;
    char* root;
    int rootfd;
    int ioflags;
} ZFMAP;

/* Forward */
static NCZMAP_API zapi;
static int zfileclose(NCZMAP* map, int delete);
static int zflookupgroup(ZFMAP*, const char* key, int nskip, int create, int* fd);
static int zflookupobj(ZFMAP*, const char* key, int* objidp);
static int zfcreateobj(ZFMAP*, const char* key, size64_t, int* objidp);
static int zfgetpath(const char* path0, char** pathp);
static char* zffullpath(ZFMAP* zfmap, const char* key);
static void zfrelease(ZFMAP* zfmap, int* fdp);
static int isabsolutepath(const char* path);
static int zfabsolutepath(ZFMAP* zmap, const char* relpath, char** pathp);
static int platformcreate(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformcreatedir(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformcreatex(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformopen(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformdircontent(int dfd, NClist* contents);
static int platformdelete(const char* path);

/* Define the Dataset level API */

static int
zfileverify(const char *path, int mode, size64_t flags, void* parameters)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    int ioflags, fd;
    
    if((stat=zfgetpath(path,&filepath)))
	goto done;

    /* Attempt to open the file to see if it is file */
    mode = (NC_NETCDF4 | mode) | NC_NOCLOBBER; /* make sure */
    if((stat = platformopen(filepath,mode,&ioflags,&fd)))
	{stat = NC_ENOTFOUND; goto done;}
    close(fd);

done:
    nullfree(filepath);
    return (stat);
}

static int
zfilecreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    ZFMAP* zfmap = NULL;
    int ioflags, rootfd;
	
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if((stat=zfgetpath(path,&filepath)))
	goto done;

    /* If NC_CLOBBER, then delete file tree */
    if(!fIsSet(mode,NC_NOCLOBBER)) {
	platformdelete(filepath);
    }

    /* Use the path to create the root directory */
    if((stat = platformcreatedir(path,mode,&ioflags,&rootfd)))
	goto done;
    
    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = strdup(path);
    zfmap->map.mode = mode;
    zfmap->map.flags = flags;
    zfmap->map.api = &zapi;
    zfmap->root = filepath;
	filepath = NULL;
    zfmap->rootfd = rootfd;
    zfmap->ioflags = ioflags;

    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    nullfree(filepath);
    if(stat) zfileclose((NCZMAP*)zfmap,1);
    return (stat);
}

static int
zfileopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    ZFMAP* zfmap = NULL;
    int fd, ioflags;
    
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if((stat=zfgetpath(path,&filepath)))
	goto done;

    /* Use the path to open the root directory */
    if((stat = platformopen(path,mode,&ioflags,&fd)))
	goto done;

    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = strdup(path);
    zfmap->map.mode = mode;
    zfmap->map.flags = flags;
    zfmap->map.api = (NCZMAP_API*)&zapi;
    zfmap->rootfd = fd;
    zfmap->ioflags = ioflags;
    zfmap->root = strdup(filepath);
	filepath = NULL;

    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    nullfree(filepath);
    if(stat) zfileclose((NCZMAP*)zfmap,0);
    return (stat);
}

/**************************************************/
/* Object API */

static int
zfileexists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    int fd = -1;

    if((stat=zflookupobj(zfmap,key,&fd)))
	goto done;
    zfrelease(zfmap,&fd);

done:
    return (stat);
}

static int
zfilelen(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    int fd;
    off_t len;

    if((stat=zflookupobj(zfmap,key,&fd)))
	goto done;
    /* Get file size */
    len = lseek(fd, 0, SEEK_END);
    zfrelease(zfmap,&fd);
    if(lenp) *lenp = len;

done:
    return stat;
}

static int
zfiledefine(NCZMAP* map, const char* key, size64_t len)
{
    int stat = NC_NOERR;
    int fd;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */

    /* Create the intermediate groups */
    if((stat = zflookupgroup(zfmap,key,1,CREATEGROUP,NULL)))
	goto done;
    stat = zflookupobj(zfmap,key,&fd);
    zfrelease(zfmap,&fd);
    if(stat == NC_NOERR) /* Already exists */
	goto done;
    else if(stat != NC_EACCESS) /* Some other kind of failure */
	goto done;

    if((stat = zfcreateobj(zfmap,key,len,&fd)))
        goto done;
    zfrelease(zfmap,&fd);

done:
    return (stat);
}

static int
zfileread(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    int fd;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    ssize_t red;
    size_t need;

    if((stat = zflookupobj(zfmap,key,&fd)))
	goto done;

    lseek(fd,start,SEEK_SET);
    need = (size_t)count;
    while(need > 0) {
        if((red = read(fd,content,need)) <= 0) goto done;
        need -= red;
    }

done:
    return (stat);
}

static int
zfilewrite(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    int fd;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */
    ssize_t writ;
    size_t need;

    if((stat = zflookupobj(zfmap,key,&fd)))
	goto done;

    lseek(fd,start,SEEK_SET);
    need = (size_t)count;
    while(need > 0) {
        if((writ = write(fd,content,need)) <= 0) goto done;
        need -= writ;
    }

done:
    return (stat);
}

static int
zfilereadmeta(NCZMAP* map, const char* key, size64_t count, char* content)
{
    return zfileread(map,key,0,count,content);
}

static int
zfilewritemeta(NCZMAP* map, const char* key, size64_t count, const char* content)
{
    return zfilewrite(map,key,0,count,content);
}

static int
zfileclose(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;

    zfrelease(zfmap,&zfmap->rootfd);
    if(delete)
	unlink(zfmap->root);

    nczm_clear(map);
    nullfree(zfmap->root);
    free(zfmap);
    return (stat);
}

/*
Return a list of keys immediately "below" a specified prefix.
In theory, the returned list should be sorted in lexical order,
but it is not.
*/
int
zfilesearch(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    int xfd;

    /* Get fd of the the prefix */
    if((stat = zflookupgroup(zfmap,prefix,0,!CREATEGROUP,&xfd)))
	goto done;
    
    /* get names of the files in the group */
    if((stat = platformdircontent(xfd, matches))) goto done;

done:
    zfrelease(zfmap,&xfd);
    return stat;
}

/**************************************************/
/* Utilities */

/* Lookup a group by parsed path (segments)*/
/* Return NC_EACCESS if not found and create if 0 */
static int
zflookupgroup(ZFMAP* zfmap, const char* key, int nskip, int create, int* gfdp)
{
    int stat = NC_NOERR;
    int i, len, gfd;
    NCbytes* path = ncbytesnew();
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    len = nclistlength(segments);
    len += nskip; /* leave off last nskip segments */
    ncbytescat(path,zfmap->root); /* Assumed to exist */
    for(i=0;i<len;i++) {
	const char* seg = nclistget(segments,i);
	ncbytescat(path,"/");
	ncbytescat(path,seg);
	/* open and optionally create */	
	zfrelease(zfmap,&gfd);
	stat = platformopen(ncbytescontents(path),0,NULL,&gfd);
        if(create && (stat == NC_EACCESS || stat == ENOENT))
	    stat = platformcreate(ncbytescontents(path),0,NULL,&gfd);
	if(stat) goto done;
    }
    if(gfdp) {*gfdp = gfd; gfd = -1;}

done:
    zfrelease(zfmap,&gfd);
    nclistfreeall(segments);
    return (stat);
}

/* Lookup an object */
/* Return NC_EACCESS if not found */
static int
zflookupobj(ZFMAP* zfmap, const char* key, int* fdp)
{
    int stat = NC_NOERR;
    int fd;
    char* path = NULL;

    if((path = zffullpath(zfmap,key))==NULL)
	{stat = NC_ENOMEM; goto done;}    

    if((fd = open(path,zfmap->ioflags)) < 0)
	{stat = NC_EACCESS; goto done;}
    if(fdp) *fdp = fd;

done:
    nullfree(path);
    return (stat);    
}

/* When we are finished accessing object */
static void
zfrelease(ZFMAP* zfmap, int* fdp)
{
    if(fdp) {
	if(*fdp >=0) close(*fdp);
    }
    if(*fdp) *fdp = -1;
}


#if 0
/* Create a group; assume all intermediate groups exist
   (do nothing if it already exists) */
static int
zfcreategroup(ZFMAP* zfmap, const char* key, int nskip, int* fdp)
{
    int stat = NC_NOERR;
    char* prefix = NULL;
    char* suffix = NULL;
    int mode = 0;
    int ioflags = 0;
    int fd = -1;

    if((stat = nczm_divide(key,nskip,&prefix,&suffix)))
	goto done;
    if((stat = platformcreatedir(prefix, mode, &ioflags, &fd)))
	goto done;
    if(fdp) {*fdp = fd; fd = -1;}

done:
    zfrelease(zfmap,&fd);
    nullfree(prefix);
    nullfree(suffix);
    return (stat);
}
#endif

/* Create an object file corresponding to a key; create any
   necessary intermediate groups */
static int
zfcreateobj(ZFMAP* zfmap, const char* key, size64_t len, int* fdp)
{
    int stat = NC_NOERR;
    int mode = 0;
    int ioflags = 0;
    int fd = -1;
    char* prefix = NULL;
    char* suffix = NULL;
    char* fullpath = NULL;

    /* Create all the prefix groups as directories */
    if((stat = nczm_divide(key,1,&prefix,&suffix)))
	goto done;
    if((stat=zfabsolutepath(zfmap,prefix,&fullpath))) goto done;
    if((stat = platformcreatedir(fullpath, mode, &ioflags, NULL)))
	goto done;
    /* Create the final object */
    nullfree(fullpath); fullpath = NULL;
    if((stat=zfabsolutepath(zfmap,key,&fullpath))) goto done;
    if((stat = platformcreate(fullpath,mode,&ioflags,&fd)))
	goto done;
    /* Set its length */
    (void)lseek(fd,len,SEEK_END);
    (void)lseek(fd,0,SEEK_SET); /* reset file cursor */
    if(fdp) {*fdp = fd; fd = -1;}

done:
    zfrelease(zfmap,&fd);
    nullfree(prefix);
    nullfree(suffix);
    nullfree(fullpath);
    return (stat);
}

static int
zfgetpath(const char* path0, char** pathp)
{
    int stat = NC_NOERR;
    const char* path = NULL;
    NCURI* uri = NULL;
    if(!ncuriparse(path0,&uri)) {
	/* Check the protocol and extract the file part */	
	if(strcasecmp(uri->protocol,"file") != 0)
	    {stat = NC_EURL; goto done;}
	path = uri->path;
    } else
	/* Assume path0 is the path */
	path = path0;
    /* Convert file path */
    if(pathp)
 	*pathp = NCpathcvt(path);
done:
    ncurifree(uri);
    return stat;
}

static int
zfabsolutepath(ZFMAP* zmap, const char* relpath, char** pathp)
{
    char* abspath = NULL;

    /* make sure that relpath is relative */
    if(isabsolutepath(relpath)) {
    	abspath = strdup(relpath); /* really is absolute */
    } else {
	size_t len = strlen(relpath)+strlen(zmap->root)+strlen("/") + 1;
	abspath = malloc(len);
	abspath[0] = '\0';
	strlcat(abspath,zmap->root,len);
	strlcat(abspath,"/",len);
	strlcat(abspath,relpath,len);
    }
    if(pathp) {*pathp = abspath; abspath = NULL;}
    return NC_NOERR;
}

static const char* driveletter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

static int
isabsolutepath(const char* path)
{
    if(path == NULL) return 0;
    switch (path[0]) {
    case '\\': return 1;
    case '/': return 1;
    case '\0': break;
    default:
	/* Check for windows drive letter */
	if(strchr(driveletter,path[0]) != NULL && path[1] == ':')
	    return 1; /* windows path with drive letter */
        break;
    }
    return 0;
}


/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_file = {
    NCZM_FILE_V1,
    zfileverify,
    zfilecreate,
    zfileopen,
};

static NCZMAP_API zapi = {
    NCZM_FILE_V1,
    zfileexists,
    zfilelen,
    zfiledefine,
    zfileread,
    zfilewrite,
    zfilereadmeta,
    zfilewritemeta,
    zfileclose,
    zfilesearch,
};

#ifdef _WIN32
static int
zfdirfiles(
{
    WIN32_FIND_DATA data;
    HANDLE hFind = FindFirstFile("C:\\semester2", &data);      // DIRECTORY

    if ( hFind != INVALID_HANDLE_VALUE ) {
        do {
            std::cout << data.cFileName << std::endl;
        } while (FindNextFile(hFind, &data));
        FindClose(hFind);
    }
}
#endif

static char*
zffullpath(ZFMAP* zfmap, const char* key)
{
    int stat = NC_NOERR;
    size_t klen, pxlen, flen;
    char* path = NULL;

    if(key == NULL) return NULL;
    klen = strlen(key);
    pxlen = strlen(zfmap->root);
    flen = klen+pxlen+1+1;
    if((path = malloc(flen)) == NULL) return NULL;
    path[0] = '\0';
    strlcat(path,zfmap->root,flen);
    strlcat(path,"/",flen);
    strlcat(path,key,flen);

    if(stat) {nullfree(path); path = NULL;}
    return path;
}

static int
platformcreate(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    stat = platformcreatex(fullpath,mode,&ioflags,fdp);
    if(ioflagsp) *ioflagsp = ioflags;
    return stat;
}

static int
platformcreatedir(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = O_DIRECTORY;
    stat = platformcreatex(fullpath,mode,&ioflags,fdp);
    if(ioflagsp) *ioflagsp = ioflags;
    return stat;
}

static int
platformcreatex(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int createflags = 0;
    int ioflags = 0;
    int fd = -1;

    if(ioflagsp) ioflags = *ioflagsp;
    ioflags |= (O_RDWR);
    createflags = (ioflags|O_CREAT);
    if(fIsSet(mode, NC_NOCLOBBER))
	fSet(createflags, O_EXCL);
    else
	fSet(createflags, O_TRUNC);
    if(fIsSet(ioflags,O_DIRECTORY)) {
	/* Create the directory usinbg mkdir */
	if(NCmkdir(fullpath,NC_DEFAULT_CREAT_MODE) < 0)
	    {stat = errno; errno = 0; goto done;}
    }
    /* Open the file/dir */
    fd = NCopen3(fullpath, createflags, NC_DEFAULT_CREAT_MODE);
    if(fd < 0)
        {stat = errno; goto done;} /* could not open */
    if(fdp) *fdp = fd;
    if(ioflagsp) *ioflagsp = ioflags;
done:
    return stat;
}

static int
platformopen(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags,fd;

    ioflags = fIsSet(mode, NC_WRITE) ? O_RDWR : O_RDONLY;
#ifdef O_BINARY
    fSet(ioflags, O_BINARY);
#endif
    fd = NCopen3(fullpath,ioflags,0);
    if(fd < 0)
        {stat = errno; goto done;} /* could not open */
    if(fdp) *fdp = fd;
    if(ioflagsp) *ioflagsp = ioflags;
done:
    return stat;
}

static int
platformdircontent(int dfd, NClist* contents)
{
    int stat = NC_NOERR;
    struct dirent* entry = NULL;
    DIR* dir = NULL;

    if((dir = fdopendir(dfd)) == NULL)
	{stat = errno; goto done;}
    for(;;) {
	errno = 0;
        if((entry = readdir(dir)) == NULL) {stat = errno; goto done;}
	nclistpush(contents,strdup(entry->d_name));
    }
done:
    closedir(dir);
    nullfree(entry);
    return stat;
}

static int
platformdeleter(NClist* segments)
{
    int status = NC_NOERR;
    struct stat statbuf;
    struct dirent* entry = NULL;
    DIR* dir = NULL;
    char* path = NULL;

    if((status = nczm_join(segments,&path))) goto done;
    errno= 0;
    status = stat(path, &statbuf);
    if(status < 0) {
	status = errno; errno = 0;
        switch (status) {
	case ENOENT: status = NC_NOERR; goto done;
	case EACCES: status = NC_EACCESS; goto done;
	default:
	    goto done;
	}
    }
    /* recurse on directory */
    if(S_ISDIR(statbuf.st_mode)) {
        if((dir = opendir(path)) == NULL)
	     {status = errno; goto done;}
        for(;;) {
	    char* seg = NULL;
	    errno = 0;
            entry = readdir(dir);
	    if(entry == NULL) {status = errno; break;}
	    /* append name to segments */
	    if((seg = strdup(entry->d_name)) == NULL)
		{status = NC_ENOMEM; goto done;}
	    nclistpush(segments,seg);
	    /* recurse */
	    if((status = platformdeleter(segments))) goto done;
	    /* remove+reclaim last segment */
	    nclistpop(segments);
	    nullfree(seg);	    	    
        }
done:	    
        if(dir) closedir(dir);
    }
    /* delete this file|dir */
    remove(path);
    return status;
}

/* Deep file/dir deletion */
static int
platformdelete(const char* path)
{
    int stat = NC_NOERR;
    NClist* segments = NULL;
    if(path == NULL || strlen(path) == 0) goto done;
    segments = nclistnew();
    nclistpush(segments,strdup(path));
    stat = platformdeleter(segments);
done:
    nclistfreeall(segments);
    return stat;
}


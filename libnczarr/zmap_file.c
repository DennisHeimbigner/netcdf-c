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
static int NC_DEFAULT_CREATE_PERMS =
        (S_IRUSR|S_IWUSR        |S_IRGRP);
static int NC_DEFAULT_ROPEN_PERMS =
        (S_IRUSR                |S_IRGRP);
static int NC_DEFAULT_RWOPEN_PERMS =
        (S_IRUSR|S_IWUSR        |S_IRGRP);
static int NC_DEFAULT_DIR_PERMS =
        (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP);
#else
static int NC_DEFAULT_CREATE_PERMS = 0640;
static int NC_DEFAULT_DIR_PERMS = 0750;
static int NC_DEFAULT_ROPEN_PERMS = 0440;
static int NC_DEFAULT_RWOPEN_PERMS = 0640;
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
    char* cwd;
} ZFMAP;

/* Forward */
static NCZMAP_API zapi;
static int zfileclose(NCZMAP* map, int delete);
static int zflookupgroup(ZFMAP*, const char* key, int nskip, int create, int* fdp);
static int zflookupobj(ZFMAP*, const char* key, int* objidp);
static int zfcreateobj(ZFMAP*, const char* key, size64_t, int* objidp);
static int zfgetrootpath(const char* path0, char** pathp);
static int zffullpath(ZFMAP* zfmap, const char* key, char**);
static void zfrelease(ZFMAP* zfmap, int* fdp);
static int isabsolutepath(const char* path);

static int platformerr(int err);
static int platformcreate(const char* fullpath, int mode, int* fdp);
static int platformcreatedir(const char* fullpath, int mode, int* fdp);
static int platformcreatex(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformopenx(const char* fullpath, int mode, int* ioflagsp, int* fdp);
static int platformopen(const char* fullpath, int mode, int* fdp);
static int platformopendir(const char* fullpath, int mode, int* fdp);
static int platformdircontent(int dfd, NClist* contents);
static int platformdelete(const char* path);
static int platformseek(int fd, int pos, size64_t* offset);
static int platformread(int fd, size64_t count, void* content);
static int platformwrite(int fd, size64_t count, const void* content);
static int platformcwd(char** cwdp);

static int zfinitialized = 0;
static void zfinitialize(void)
{
    if(!zfinitialized) {
	const char* env = NULL;
	int perms = 0;
	env = getenv("NC_DEFAULT_CREATE_PERMS");
	if(env != NULL && strlen(env) > 0) {
	    if(sscanf(env,"%d",&perms) == 1) NC_DEFAULT_CREATE_PERMS = perms;
	}
	env = getenv("NC_DEFAULT_DIR_PERMS");
	if(env != NULL && strlen(env) > 0) {
	    if(sscanf(env,"%d",&perms) == 1) NC_DEFAULT_DIR_PERMS = perms;
	}
        zfinitialized = 1;
    }
}

/* Define the Dataset level API */

static int
zfilecreate(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    char* zfcwd = NULL;
    ZFMAP* zfmap = NULL;
    int rootfd;
	
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if(!zfinitialized) zfinitialize();

    /* Get cwd so we can use absolute paths */
    if((stat = platformcwd(&zfcwd))) goto done;

    /* Get root file from the path url */
    if((stat=zfgetrootpath(path,&filepath)))
	goto done;

    /* Make the root path be absolute */
    if(!isabsolutepath(filepath)) {
	char* abspath = NULL;
	if((stat = nczm_suffix(zfcwd,filepath,&abspath))) goto done;
	nullfree(filepath);
	filepath = abspath;
    }

    /* If NC_CLOBBER, then delete file tree */
    if(!fIsSet(mode,NC_NOCLOBBER)) {
	platformdelete(filepath);
    }
    
    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = strdup(path);
    zfmap->map.flags = flags;
    /* create => NC_WRITE */
    zfmap->map.mode = mode|NC_WRITE;
    zfmap->map.api = &zapi;
    zfmap->root = filepath;
	filepath = NULL;

    /* Use the path to create the root directory */
    rootfd = 0;
    if((stat = platformcreatedir(path,zfmap->map.mode,&rootfd)))
	{rootfd = -1; goto done;}
    zfmap->rootfd = rootfd;
    
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    nullfree(filepath);
    if(stat)
    	zfileclose((NCZMAP*)zfmap,1);
    return (stat);
}

static int
zfileopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    char* zfcwd = NULL;
    ZFMAP* zfmap = NULL;
    int fd;
    
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if(!zfinitialized) zfinitialize();

    /* Get cwd so we can use absolute paths */
    if((stat = platformcwd(&zfcwd))) goto done;

    /* Get root file from the path url */
    if((stat=zfgetrootpath(path,&filepath)))
	goto done;

    /* Make the root path be absolute */
    if(!isabsolutepath(filepath)) {
	char* abspath = NULL;
	if((stat = nczm_suffix(zfcwd,filepath,&abspath))) goto done;
	nullfree(filepath);
	filepath = abspath;
    }

    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = strdup(path);
    zfmap->map.flags = flags;
    zfmap->map.mode = mode;
    zfmap->map.api = (NCZMAP_API*)&zapi;
    zfmap->root = strdup(filepath);
	filepath = NULL;

    /* Use the path to open the root directory */
    if((stat = platformopendir(path,mode,&fd)))
	goto done;
    zfmap->rootfd = fd;
    
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    nullfree(zfcwd);
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

    if((stat=zflookupobj(zfmap,key,NULL)))
	goto done;

done:
    return (stat);
}

static int
zfilelen(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZFMAP* zfmap = (ZFMAP*)map;
    size64_t len;
    int fd;

    if((stat=zflookupobj(zfmap,key,&fd)))
	goto done;
    /* Get file size */
    len = 0;
    if((stat=platformseek(fd, SEEK_END, &len))) goto done;
    zfrelease(zfmap,&fd);
    if(lenp) *lenp = len;

done:
    return THROW(stat);
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
    else if(stat != NC_EACCESS) /* NC_EACCESS => file does not exist */
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

    if((stat = zflookupobj(zfmap,key,&fd)))
	goto done;

    if((stat = platformseek(fd,SEEK_SET,&start))) goto done;
    if((stat = platformread(fd,count,content))) goto done;

done:
    return (stat);
}

static int
zfilewrite(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    int fd;
    ZFMAP* zfmap = (ZFMAP*)map; /* cast to true type */

    if((stat = zflookupobj(zfmap,key,&fd)))
	goto done;

    if((stat = platformseek(fd,SEEK_SET,&start))) goto done;
    if((stat = platformwrite(fd,count,content))) goto done;

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

    if(zfmap == NULL) return NC_NOERR;
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
but it possible that it is not.
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
    return THROW(stat);
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
    char* fullpath = NULL;
    NCbytes* path = ncbytesnew();
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    len = nclistlength(segments);
    len -= nskip; /* leave off last nskip segments */
    gfd = -1;
    ncbytescat(path,zfmap->root); /* We need path to be absolute */
    if((stat = platformopendir(ncbytescontents(path),zfmap->map.mode,&gfd))) goto done;
    for(i=0;i<len;i++) {
	const char* seg = nclistget(segments,i);
	ncbytescat(path,"/");
	ncbytescat(path,seg);
	/* open and optionally create */	
	zfrelease(zfmap,&gfd);
	stat = platformopendir(ncbytescontents(path),zfmap->map.mode,&gfd);
        if(create && (stat == NC_EACCESS)) {
	    zfrelease(zfmap,&gfd);
	    stat = platformcreatedir(ncbytescontents(path),zfmap->map.mode,&gfd);
	}
	if(stat) goto done;
    }
    if(gfdp) {*gfdp = gfd; gfd = -1;}

done:
    nullfree(fullpath);
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

    if((stat = zffullpath(zfmap,key,&path)))
	{goto done;}    

    if((stat = platformopen(path,zfmap->map.mode,&fd)))
	goto done;
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
    int fd = -1;

    if((stat = nczm_divide(key,nskip,&prefix,&suffix)))
	goto done;
    if((stat = platformcreatedir(prefix, zfmap->map.mode, &fd)))
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
    int fd = -1;
    char* prefix = NULL;
    char* suffix = NULL;
    char* fullpath = NULL;

    /* Create all the prefix groups as directories */
    if((stat = nczm_divide(key,1,&prefix,&suffix)))
	goto done;
    if((stat=zffullpath(zfmap,prefix,&fullpath))) goto done;
    if((stat = platformcreatedir(fullpath, zfmap->map.mode, NULL)))
	goto done;
    /* Create the final object */
    nullfree(fullpath); fullpath = NULL;
    if((stat=zffullpath(zfmap,key,&fullpath))) goto done;
    if((stat = platformcreate(fullpath,zfmap->map.mode,&fd)))
	goto done;
    /* Set its length */
    if((stat = platformseek(fd,SEEK_END,&len))) goto done;
    if((stat = platformseek(fd,SEEK_SET,NULL))) goto done;
    if(fdp) {*fdp = fd; fd = -1;}

done:
    zfrelease(zfmap,&fd);
    nullfree(prefix);
    nullfree(suffix);
    nullfree(fullpath);
    return (stat);
}

static int
zfgetrootpath(const char* path0, char** pathp)
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
    return THROW(stat);
}

#if 0
static int
zfabsolutepath(ZFMAP* zmap, const char* relpath, char** pathp)
{
    char* abspath = NULL;

    /* make sure that relpath is relative */
    if(isabsolutepath(relpath)) {
    	abspath = strdup(relpath); /* really is absolute */
    } else {
	size_t len = strlen(relpath)+strlen("/")+strlen(zmap->root)+1;
	abspath = malloc(len);
	abspath[0] = '\0';
	strlcat(abspath,zmap->cwd,len);
	strlcat(abspath,"/",len);
	strlcat(abspath,zmap->root,len);
	strlcat(abspath,"/",len);
	strlcat(abspath,relpath,len);
    }
    if(pathp) {*pathp = abspath; abspath = NULL;}
    return NC_NOERR;
}
#endif

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
    FD hFind = FindFirstFile("C:\\semester2", &data);      // DIRECTORY

    if ( hFind != INVALID_FD_VALUE ) {
        do {
            std::cout << data.cFileName << std::endl;
        } while (FindNextFile(hFind, &data));
        FindClose(hFind);
    }
}
#endif

static int
zffullpath(ZFMAP* zfmap, const char* key, char** pathp)
{
    int stat = NC_NOERR;
    size_t klen, pxlen, flen;
    char* path = NULL;

    klen = nulllen(key);
    pxlen = strlen(zfmap->root);
    flen = klen+pxlen+1+1;
    if((path = malloc(flen)) == NULL) {stat = NC_ENOMEM; goto done;}
    path[0] = '\0';
    strlcat(path,zfmap->root,flen);
    /* look for special cases */
    if(key != NULL) {
        if(key[0] != '/') strlcat(path,"/",flen);
	if(strcmp(key,"/") != 0)
            strlcat(path,key,flen);
    }
    if(pathp) {*pathp = path; path = NULL;}
done:
    nullfree(path)
    return stat;
}

/**************************************************/
static int
platformerr(int err)
{
     switch (err) {
     case ENOENT: err = NC_EACCESS; break; /* File does not exist */
     case EACCES: err = NC_EAUTH; break; /* file permissions */
     case EPERM:  err = NC_EAUTH; break; /* ditto */
     default: break;
     }
     return err;
}

static int
platformcreate(const char* fullpath, int mode, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    stat = platformcreatex(fullpath,mode,&ioflags,fdp);
    return THROW(stat);
}

static int
platformcreatedir(const char* fullpath, int mode, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = O_DIRECTORY;
    stat = platformcreatex(fullpath,mode,&ioflags,fdp);
    return THROW(stat);
}

static int
platformcreatex(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int createflags = 0;
    int ioflags = 0;
    int fd = -1;

    if(*ioflagsp) ioflags = *ioflagsp;
    
    errno = 0;
    if(fIsSet(ioflags,O_DIRECTORY)) {
        ioflags |= (O_RDONLY);
	/* Open the file/dir */
        fd = NCopen2(fullpath, ioflags);
	if(fd < 0) {
	    /* Create the directory using mkdir */
   	    stat=NCmkdir(fullpath,NC_DEFAULT_DIR_PERMS);
            if(stat < 0)
	        {stat = platformerr(errno); errno = 0; goto done;}
            /* open it again */
	    fd = NCopen2(fullpath, ioflags);
            if(stat < 0)
	        {stat = platformerr(errno); errno = 0; goto done;}
	}
    } else {/* Open the file */
	int permissions = NC_DEFAULT_ROPEN_PERMS;
        if(!fIsSet(mode, NC_WRITE))
            ioflags |= (O_RDONLY);
        else {
            ioflags |= (O_RDWR);
	    permissions = NC_DEFAULT_RWOPEN_PERMS;
	}
#ifdef O_BINARY
        fSet(ioflags, O_BINARY);
#endif
        if(fIsSet(mode, NC_NOCLOBBER))
	    fSet(createflags, O_EXCL);
        else
	    fSet(createflags, O_TRUNC);

	/* Try to open file as if it exists */
        fd = NCopen3(fullpath, createflags, permissions);
	if(fd < 0 && errno == ENOENT && fIsSet(mode,NC_WRITE)) {
	    /* Try to create it */
            createflags = (ioflags|O_CREAT);
            fd = NCopen3(fullpath, createflags, permissions);
	    if(fd < 0) goto done; /* could not create */
	}
    }
    if(fd < 0)
        {stat = platformerr(errno); goto done;} /* could not open */
    if(fdp) *fdp = fd;
    if(ioflagsp) *ioflagsp = ioflags;
done:
    errno = 0;
    return THROW(stat);
}

static int
platformopen(const char* fullpath, int mode, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    stat = platformopenx(fullpath,mode,&ioflags,fdp);
    return THROW(stat);
}

static int
platformopendir(const char* fullpath, int mode, int* fdp)
{
    int stat = NC_NOERR;
    int ioflags = O_DIRECTORY;
    stat = platformopenx(fullpath,mode,&ioflags,fdp);
    return THROW(stat);
}

static int
platformopenx(const char* fullpath, int mode, int* ioflagsp, int* fdp)
{
    int stat = NC_NOERR;
    int fd = -1;
    int ioflags = 0;

    if(*ioflagsp) ioflags = *ioflagsp;

    if(fIsSet(ioflags,O_DIRECTORY) || !fIsSet(mode,NC_WRITE))
        ioflags |= O_RDONLY;
    else
        ioflags |= O_RDWR;
#ifdef O_BINARY
    ioflags |= O_BINARY;
#endif
    errno = 0;
    fd = NCopen2(fullpath,ioflags);
    if(fd < 0)
	{stat = platformerr(errno); goto done;} /* could not open */
    if(fdp) *fdp = fd;

done:
    errno = 0;
    return THROW(stat);
}

static int
platformdircontent(int dfd, NClist* contents)
{
    int stat = NC_NOERR;
    struct dirent* entry = NULL;
    DIR* dir = NULL;

    errno = 0;
    if((dir = fdopendir(dfd)) == NULL)
	{stat = platformerr(errno); goto done;}
    for(;;) {
	errno = 0;
        if((entry = readdir(dir)) == NULL) {stat = platformerr(errno); goto done;}
	if(strcmp(entry->d_name,".")==0 || strcmp(entry->d_name,"..")==0)
	    continue;
	nclistpush(contents,strdup(entry->d_name));
    }
done:
    closedir(dir);
    nullfree(entry);
    errno = 0;
    return THROW(stat);
}

static int
platformdeleter(NClist* segments)
{
    int ret = NC_NOERR;
    struct stat statbuf;
    struct dirent* entry = NULL;
    DIR* dir = NULL;
    char* path = NULL;

    if((ret = nczm_join(segments,&path))) goto done;
    errno= 0;
    ret = stat(path, &statbuf);
    if(ret < 0) {
	ret = platformerr(errno); errno = 0;
    }
    /* recurse on directory */
    if(S_ISDIR(statbuf.st_mode)) {
        if((dir = opendir(path)) == NULL)
	     {ret = platformerr(errno); goto done;}
        for(;;) {
	    char* seg = NULL;
	    errno = 0;
            entry = readdir(dir);
	    if(entry == NULL) {ret = platformerr(errno); break;}
	    /* Ignore "." and ".." */
	    if(strcmp(entry->d_name,".")==0) continue;
    	    if(strcmp(entry->d_name,"..")==0) continue;
	    /* append name to segments */
	    if((seg = strdup(entry->d_name)) == NULL)
		{ret = NC_ENOMEM; goto done;}
	    nclistpush(segments,seg);
	    /* recurse */
	    if((ret = platformdeleter(segments))) goto done;
	    /* remove+reclaim last segment */
	    nclistpop(segments);
	    nullfree(seg);	    	    
        }
done:	    
        if(dir) closedir(dir);
    }
    /* delete this file|dir */
    remove(path);
    errno = 0;
    return THROW(ret);
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
    errno = 0;
    return THROW(stat);
}

static int
platformseek(int fd, int pos, size64_t* sizep)
{
    int ret = NC_NOERR;
    off_t size, newsize;
    struct stat statbuf;    
    
    errno = 0;
    ret = fstat(fd, &statbuf);
    if(ret < 0) {
	ret = platformerr(errno); errno = 0;
    }
    if(S_ISDIR(statbuf.st_mode))
        {ret = NC_EINVAL; goto done;}
    if(sizep) size = *sizep; else size = 0;
    newsize = lseek(fd,size,pos);
    if(sizep) *sizep = newsize;
done:
    errno = 0;
    return THROW(ret);
}

static int
platformread(int fd, size64_t count, void* content)
{
    int stat = NC_NOERR;
    size_t need = count;
    unsigned char* readpoint = content;
    while(need > 0) {
        ssize_t red;
        if((red = read(fd,readpoint,need)) <= 0)
	    {stat = NC_EINVAL; goto done;}
        need -= red;
	readpoint += red;
    }
done:
    return THROW(stat);
}

static int
platformwrite(int fd, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    size_t need = count;
    unsigned char* writepoint = (unsigned char*)content;
    while(need > 0) {
        ssize_t red;
        if((red = write(fd,writepoint,need)) <= 0)
	    {stat = NC_EINVAL; goto done;}
        need -= red;
	writepoint += red;
    }
done:
    return THROW(stat);
}

static int
platformcwd(char** cwdp)
{
    char buf[4096];
    char* cwd = NULL;
    cwd = NCcwd(buf,sizeof(buf));
    if(cwd == NULL) return errno;
    if(cwdp) *cwdp = strdup(buf);
    return NC_NOERR;
}

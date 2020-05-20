/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/* Not sure this has any effect */
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1

#include "zincludes.h"

#include <errno.h>
#if 0
#ifdef _WIN32
#ifndef __cplusplus
#include <windows.h>
#include <io.h>
#include <iostream>
#endif
#endif
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
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif

#include "fbits.h"
#include "ncwinpath.h"

#undef debug

#ifndef O_DIRECTORY
# define O_DIRECTORY  0200000
#endif

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

/* Define a struct to wrap the platform dependent file
   or directory handle
*/

typedef enum FDTYPE {FDUNDEF=-1,FDFILE=0,FDDIR=1} FDTYPE;

typedef struct FD {
    FDTYPE sort; /* Discriminator for union */
    union {
#ifdef _WIN32
	struct {
#ifdef _WIN32
	    char* dir; /* absolute path of the directory */
	} dir;
	int fd;
#else
	struct {
	    DIR* dir;
	} dir;
	int fd;
#endif
    } u;
} FD;

/* Define the "subclass" of NCZMAP */
typedef struct ZFMAP {
    NCZMAP map;
    char* root;
    FD rootfd;
    char* cwd;
} ZFMAP;

/* Forward */
static NCZMAP_API zapi;
static int zfileclose(NCZMAP* map, int delete);
static int zflookupgroup(ZFMAP*, const char* key, int nskip, int create, int* fdp);
static int zflookupobj(ZFMAP*, const char* key, int* objidp);
static int zfcreateobj(ZFMAP*, const char* key, size64_t, int* objidp);
static int zfparseurl(const char* path0, NCURI** urip);
static int zffullpath(ZFMAP* zfmap, const char* key, char**);
static void zfrelease(ZFMAP* zfmap, int* fdp);

static int platformerr(int err);
static int platformcreate(ZFMAP* map, const char* truepath, FD* fdp);
static int platformcreatedir(ZFMAP* map, const char* truepath, FD* fdp);
static int platformcreatex(ZFMAP* map, const char* truepath, int* ioflagsp, FD* fdp);
static int platformopenx(ZFMAP* map, const char* truepath, int* ioflagsp, FD* fdp);
static int platformopen(ZFMAP* map, const char* truepath, FD* fdp);
static int platformopendir(ZFMAP* map, const char* truepath, void* direntp);
static int platformdircontent(ZFMAP* map, int dfd, NClist* contents);
static int platformdelete(ZFMAP* map, const char* path);
static int platformseek(ZFMAP* map, int fd, int pos, size64_t* offset);
static int platformread(ZFMAP* map, int fd, size64_t count, void* content);
static int platformwrite(ZFMAP* map, int fd, size64_t count, const void* content);
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
    char* truepath = NULL; /* Might be a URL */
    char* zfcwd = NULL;
    ZFMAP* zfmap = NULL;
    FD rootfd;
    NCURI* url = NULL;
	
    NC_UNUSED(parameters);

    if(!zfinitialized) zfinitialize();

    /* Fixup mode flags */
    mode = (NC_NETCDF4 | NC_WRITE | mode);
    if(flags & FLAG_BYTERANGE)
        mode &=  ~(NC_CLOBBER | NC_WRITE);

    if(!(mode & NC_WRITE))
        {stat = NC_EPERM; goto done;}

    /* Get cwd so we can use absolute paths */
    if((stat = platformcwd(&zfcwd))) goto done;

    /* must be a url */
    if((stat=zfparseurl(path,&url)))
	goto done;

    /* We get file path based on flags & FLAG_BYTERANGE */
    if(flags & FLAG_BYTERANGE)
	truepath = ncuribuild(url,NULL,NULL,NCURIALL);	
    else {
        /* Make the root path be absolute */
        if(!nczm_isabsolutepath(url->path)) {
   	    if((stat = nczm_suffix(zfcwd,url->path,&truepath))) goto done;
	} else
	    truepath = strdup(url->path);
    }

    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    zfmap->map.flags = flags;
    /* create => NC_WRITE */
    zfmap->map.mode = mode;
    zfmap->map.api = &zapi;
    zfmap->root = truepath;
        truepath = NULL;

    /* If NC_CLOBBER, then delete file tree */
    if(!fIsSet(mode,NC_NOCLOBBER)) {
	platformdelete(zfmap,truepath);
    }
    
    /* Use the path to create the root directory */
   
    if((stat = platformcreatedir(zfmap, zfmap->root ,&rootfd)))
	{rootfd.sort = FDUNDEF; goto done;}
    zfmap->rootfd = rootfd;
    
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    ncurifree(url);
    nullfree(truepath);
    nullfree(zfcwd);
    if(stat)
    	zfileclose((NCZMAP*)zfmap,1);
    return (stat);
}

static int
zfileopen(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* truepath = NULL;
    char* zfcwd = NULL;
    ZFMAP* zfmap = NULL;
    int fd;
    NCURI*url = NULL;
    
    NC_UNUSED(parameters);

    if(!zfinitialized) zfinitialize();

    /* Fixup mode flags */
    mode = (NC_NETCDF4 | mode);
    if(flags & FLAG_BYTERANGE)
        mode &=  ~(NC_CLOBBER | NC_WRITE);

    /* Get cwd so we can use absolute paths */
    if((stat = platformcwd(&zfcwd))) goto done;

    if((stat=zfparseurl(path,&url)))
	goto done;

    /* We get file path based on flags & FLAG_BYTERANGE */
    if(flags & FLAG_BYTERANGE)
	truepath = ncuribuild(url,NULL,NULL,NCURIALL);	
    else {
        /* Make the root path be absolute */
        if(!nczm_isabsolutepath(url->path)) {
	    if((stat = nczm_suffix(zfcwd,url->path,&truepath))) goto done;
	} else
	    truepath = strdup(url->path);
    }

    /* Build the z4 state */
    if((zfmap = calloc(1,sizeof(ZFMAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zfmap->map.format = NCZM_FILE;
    zfmap->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    zfmap->map.flags = flags;
    zfmap->map.mode = mode;
    zfmap->map.api = (NCZMAP_API*)&zapi;
    zfmap->root = truepath;
	truepath = NULL;

    /* Use the path to open the root directory */
    if((stat = platformopendir(zfmap,zfmap->root,&fd)))
	goto done;
    zfmap->rootfd = fd;
    
    if(mapp) *mapp = (NCZMAP*)zfmap;    

done:
    ncurifree(url);
    nullfree(zfcwd);
    nullfree(truepath);
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
    if((stat=platformseek(zfmap, fd, SEEK_END, &len))) goto done;
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

    if((stat = platformseek(zfmap, fd,SEEK_SET,&start))) goto done;
    if((stat = platformread(zfmap, fd,count,content))) goto done;

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

    if((stat = platformseek(zfmap,fd,SEEK_SET,&start))) goto done;
    if((stat = platformwrite(zfmap,fd,count,content))) goto done;

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
    stat = zflookupgroup(zfmap,prefix,0,!CREATEGROUP,&xfd);
    switch (stat) {
    case NC_EINVAL: /* not a directory, so stop */
	stat = NC_NOERR;
	break;
    case NC_NOERR:
        /* get names of the files in the group */
        if((stat = platformdircontent(zfmap, xfd, matches))) goto done;
	break;
    case NC_EACCESS: /* No such file */
        /* Fall thru */
    default:
	goto done;
    }

done:
    zfrelease(zfmap,&xfd);
    return THROW(stat);
}

/**************************************************/
/* Utilities */

/* Lookup a group by parsed path (segments)*/
/* Return NC_EACCESS if not found, NC_EINVAL if not a directory; create if create flag is set */
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
    if((stat = platformopendir(zfmap, ncbytescontents(path),&gfd))) goto done;
    for(i=0;i<len;i++) {
	const char* seg = nclistget(segments,i);
	ncbytescat(path,"/");
	ncbytescat(path,seg);
	/* open and optionally create */	
	zfrelease(zfmap,&gfd);
	stat = platformopendir(zfmap,ncbytescontents(path),&gfd);
        if(create && (stat == NC_EACCESS)) {
	    zfrelease(zfmap,&gfd);
	    stat = platformcreatedir(zfmap,ncbytescontents(path),&gfd);
	}
	if(stat) goto done;
    }
    if(gfdp) {*gfdp = gfd; gfd = -1;}

done:
    nullfree(fullpath);
    zfrelease(zfmap,&gfd);
    ncbytesfree(path);
    nclistfreeall(segments);
    return (stat);
}

/* Lookup an object */
/* Return NC_EACCESS if not found */
static int
zflookupobj(ZFMAP* zfmap, const char* key, FD* fdp)
{
    int stat = NC_NOERR;
    int fd = -1;
    char* path = NULL;

    if(fdp) *fdp = -1;

    if((stat = zffullpath(zfmap,key,&path)))
	{goto done;}    

    if((stat = platformopen(zfmap,path,&fd)))
	goto done;
    if(fdp) *fdp = fd;

done:
    nullfree(path);
    return (stat);    
}

/* When we are finished accessing object */
static void
zfrelease(ZFMAP* zfmap, FD* fdp)
{
    if(fdp) {
	if(*fdp >=0) close(*fdp);
    }
    if(fdp) *fdp = -1;
}


#if 0
/* Create a group; assume all intermediate groups exist
   (do nothing if it already exists) */
static int
zfcreategroup(ZFMAP* zfmap, const char* key, int nskip, FD* fdp)
{
    int stat = NC_NOERR;
    char* prefix = NULL;
    char* suffix = NULL;
    int fd = -1;

    if(fdp) *fdp = -1;
    if((stat = nczm_divide(key,nskip,&prefix,&suffix)))
	goto done;
    if((stat = platformcreatedir(zfmap,prefix, zfmap->map.mode, &fd)))
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
zfcreateobj(ZFMAP* zfmap, const char* key, size64_t len, FD* fdp)
{
    int stat = NC_NOERR;
    int fd = -1;
    char* fullpath = NULL;

    if(fdp) *fdp = -1;
    /* Create all the prefix groups as directories */
    if((stat = zflookupgroup(zfmap, key, 1, CREATEGROUP, NULL))) goto done;
    /* Create the final object */
    if((stat=zffullpath(zfmap,key,&fullpath))) goto done;
    if((stat = platformcreate(zfmap,fullpath,&fd)))
	goto done;
    /* Set its length */
    if((stat = platformseek(zfmap,fd,SEEK_END,&len))) goto done;
    if((stat = platformseek(zfmap,fd,SEEK_SET,NULL))) goto done;
    if(fdp) {*fdp = fd; fd = -1;}

done:
    zfrelease(zfmap,&fd);
    nullfree(fullpath);
    return (stat);
}

static int
zfparseurl(const char* path0, NCURI** urip)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;
    ncuriparse(path0,&uri);
    if(uri == NULL)
	{stat = NC_EURL; goto done;}
    if(urip) {*urip = uri; uri = NULL;}

done:
    ncurifree(uri);
    return stat;
}


/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_nzf = {
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
     case ENOTDIR: err = NC_EINVAL; break; /* not a directory */
     case EACCES: err = NC_EAUTH; break; /* file permissions */
     case EPERM:  err = NC_EAUTH; break; /* ditto */
     default: break;
     }
     return err;
}

/* Create a file */
static int
platformcreatefile(ZFMAP* zfmap, const char* truepath, FD* fdp)
{
    int stat = NC_NOERR;
    int ioflags = 0;
    int stat = NC_NOERR;
    int createflags = 0;
    int ioflags = 0;
    int mode = zfmap->map.mode;
    int fd = -1;
    int permissions = NC_DEFAULT_ROPEN_PERMS;

    fdp->sort = FDUNDEF;
    
    if(*ioflagsp) ioflags = *ioflagsp;
    
    errno = 0;
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
    fdp->u.fd = NCopen3(truepath, createflags, permissions);
    if(fdp->u.fd < 0) {
	if(errno == ENOENT) {
	    if(fIsSet(mode,NC_WRITE)) {
	        /* Try to create it */
                createflags = (ioflags|O_CREAT);
                fdp->u.fd = NCopen3(truepath, createflags, permissions);
	        if(fdp->u.fd < 0) goto done; /* could not create */
	    }
	}
    }
    if(fdp->u.fd < 0)
        {stat = platformerr(errno); goto done;} /* could not open */
    if(ioflagsp) *ioflagsp = ioflags;
    fdp->sort = FDFILE;
done:
    errno = 0;
    return THROW(stat);
}

static int
platformcreatedir(ZFMAP* zfmap, const char* truepath, FD* fdp)
{
    int stat = NC_NOERR;
    int ioflags = O_DIRECTORY;
    int stat = NC_NOERR;
    int createflags = 0;
    int ioflags = 0;
    int mode = zfmap->map.mode;
    int fd = -1;

    memset(&fdp->u.dir,0,sizeof(fdp->u.dir));
    
    fdp->sort = FDUNDEF;

    if(*ioflagsp) ioflags = *ioflagsp;
    
    errno = 0;
    ioflags |= (O_RDONLY);

    /* Open the file/dir */
#ifndef USEDIRENT /*=> _WIN32*/
    {
	HANDLE hFind;
	WIN32_FIND_DATA data;
        hFind = FindFirstFile(truepath,&data);
        if(hFind == INVALID_HANDLE_VALUE) {
            /* Create the directory using mkdir */
   	    stat=NCmkdir(truepath,NC_DEFAULT_DIR_PERMS);
            if(stat < 0)
	        {stat = platformerr(errno); goto done;}
	    /* Open again */
            hFind = FindFirstFile(truepath,&data);
            if(hFind == INVALID_HANDLE_VALUE)
	        {stat = platformerr(EACCES); goto done;}
	}
        /* close it up for now */
	FindClose(hFind);	
	fdp->u.dir.dir = strdup(truepath);
    }
#endif
#ifdef USEDIRENT
    fdp->u.dir.dir = NCopendir(truepath);
    if(fdp->u.dir.dir == NULL) {
	if(errno == ENOENT) {
	    errno = 0;
	    /* Create the directory using mkdir */
   	    stat=NCmkdir(truepath,NC_DEFAULT_DIR_PERMS);
            if(stat < 0)
	        {stat = platformerr(errno); goto done;}
            /* open it again */
	    fdp->u.dir.dir = NCopendir(truepath);
            if(fdp->u.dir.dir == NULL)
	        {stat = platformerr(errno); goto done;}
	} else
	    {stat = platformerr(errno); goto done;}
    }
#endif
    if(ioflagsp) *ioflagsp = ioflags;
    fdp->sort = FDDIR;
done:
    errno = 0;
    return THROW(stat);
}

static int
platformdircontent(ZFMAP* zfmap, FD* dfd, NClist* contents)
{
    int stat = NC_NOERR;
#ifndef USEDIRENT /*=> _WIN32*/
    HANDLE hFind;
    WIN32_FIND_DATA data;
    hFind = FindFirstFile(dfd->u.dir.dir,&data);
    if(hFind == INVALID_HANDLE_VALUE)
	{stat = platformerr(EACCES); goto done;
    do {
	const char* file = dfd->u.dir.FindFileData.cFileName;
	if(strcmp(file,".")==0 || strcmp(file,"..")==0)
	    continue;
	nclistpush(contents,strdup(file));
    } while (FindNextFile(dfd->u.dir.hFind, &dfd->u.dir.FindFileData);
    FindClose(hFind);
#endif /*!USEDIRENT*/
#ifdef USEDIRENT
    errno = 0;
    for(;;) {
	errno = 0;
        if((entry = readdir(dfd->u.dir.dir)) == NULL)
	    {stat = platformerr(errno); goto done;}
	if(strcmp(entry->d_name,".")==0 || strcmp(entry->d_name,"..")==0)
	    continue;
	nclistpush(contents,strdup(entry->d_name));
    }
    nullfree(entry);
    closedir(df->u.dir.dir);
#endif
done:
    errno = 0;
    return THROW(stat);
}

static int
platformdeleter(ZFMAP* zfmap, NClist* segments)
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
	    if((ret = platformdeleter(zfmap, segments))) goto done;
	    /* remove+reclaim last segment */
	    nclistpop(segments);
	    nullfree(seg);	    	    
        }
done:	    
        if(dir) closedir(dir);
    }
    /* delete this file|dir */
    remove(path);
    nullfree(path);
    errno = 0;
    return THROW(ret);
}

/* Deep file/dir deletion */
static int
platformdelete(ZFMAP* zfmap, const char* path)
{
    int stat = NC_NOERR;
    NClist* segments = NULL;
    if(path == NULL || strlen(path) == 0) goto done;
    segments = nclistnew();
    nclistpush(segments,strdup(path));
    stat = platformdeleter(zfmap,segments);
done:
    nclistfreeall(segments);
    errno = 0;
    return THROW(stat);
}

static int
platformseek(ZFMAP* zfmap, int fd, int pos, size64_t* sizep)
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
platformread(ZFMAP* zfmap, int fd, size64_t count, void* content)
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
platformwrite(ZFMAP* zfmap, int fd, size64_t count, const void* content)
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

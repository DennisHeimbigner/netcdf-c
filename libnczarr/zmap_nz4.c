/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"

#include "fbits.h"
#include "ncwinpath.h"

/*
Do a simple mapping of our simplified map model
to a netcdf-4 file.

For the object API, the mapping is as follows:
1. Every object (e.g. group or array) is mapped to a netcdf-4 group.
2. Meta data objects (e.g. .zgroup, .zarray, etc) are kept as an
   char typed attribute of the group
3. Actual variable data (for e.g. chunks) is stored as
   a ubyte typed variable with the chunk name.
*/

#undef DEBUG

#define NCZM_NC4_V1 1

/* What to replace ZDOT with */
#define ZDOTNC4 '_'

/* define the attr/var name containing an objects content */
#define ZCONTENT "data"

/* Define the dimension for the ZCONTENT variable */
/* Avoid creating a coordinate variable */
#define ZCONTENTDIM "data_dim"

/* Mnemonic */
#define Z4META 0

/* Define the "subclass" of NCZMAP */
typedef struct Z4MAP {
    NCZMAP map;
    char* path;
    int ncid;
} Z4MAP;


/* Forward */
static NCZMAP_API zapi;
static int znc4close(NCZMAP* map, int delete);
static int zlookupgroup(Z4MAP*, NClist* segments, int nskip, int* grpidp);
static int zlookupobj(Z4MAP*, NClist* segments, int* objidp);
static int zcreategroup(Z4MAP* z4map, NClist* segments, int nskip, int* grpidp);
static int zcreateobj(Z4MAP*, NClist* segments, size64_t, int* objidp);
static int zcreatedim(Z4MAP*, int, int* dimidp);
static int parseurl(const char* path0, NCURI** urip);
static void nc4ify(const char* zname, char* nc4name);
static void zify(const char* nc4name, char* zname);
static int z4getcwd(char** cwdp);


/* Define the Dataset level API */

static int
znc4create(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* truepath = NULL;
    Z4MAP* z4map = NULL;
    int ncid;
    NCURI* url = NULL;
    char* z4cwd = NULL;
    
    if((stat=parseurl(path,&url)))
	goto done;

    /* Fix up mode */
    mode = (NC_NETCDF4 | NC_WRITE | mode);
    if(flags & FLAG_BYTERANGE)
        mode &=  ~(NC_CLOBBER | NC_WRITE);

    if(!(mode & NC_WRITE))
        {stat = NC_EPERM; goto done;}

    /* Get cwd so we can use absolute paths */
    if((stat = z4getcwd(&z4cwd))) goto done;

    if(flags & FLAG_BYTERANGE)    
	truepath = ncuribuild(url,NULL,NULL,NCURIALL);	
    else {
        /* Make the root path be absolute */
        if(!nczm_isabsolutepath(url->path)) {
	    if((stat = nczm_suffix(z4cwd,url->path,&truepath))) goto done;
	} else
	    truepath = strdup(url->path);
    }
    
    /* Build the z4 state */
    if((z4map = calloc(1,sizeof(Z4MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z4map->map.format = NCZM_NC4;
    z4map->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    z4map->map.mode = mode;
    z4map->map.flags = flags;
    z4map->map.api = &zapi;
    z4map->path = truepath;
        truepath = NULL;

    if((stat=nc_create(z4map->path,mode,&ncid)))
        {stat = NC_ENOTFOUND; goto done;} /* could not open */
    z4map->ncid = ncid;
    
    if(mapp) *mapp = (NCZMAP*)z4map;    

done:
    ncurifree(url);
    nullfree(truepath);
    nullfree(z4cwd);
    if(stat) znc4close((NCZMAP*)z4map,1);
    return (stat);
}

static int
znc4open(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* truepath = NULL;
    Z4MAP* z4map = NULL;
    int ncid;
    NCURI* url = NULL;
    char* z4cwd = NULL;
	
    /* Fixup mode */
    mode = NC_NETCDF4 | mode;
    if(flags & FLAG_BYTERANGE)
        mode &= ~(NC_CLOBBER | NC_WRITE);

    if((stat=parseurl(path,&url)))
	goto done;

    /* Get cwd so we can use absolute paths */
    if((stat = z4getcwd(&z4cwd))) goto done;

    if(flags & FLAG_BYTERANGE)    
	truepath = ncuribuild(url,NULL,NULL,NCURIALL);	
    else {
        /* Make the root path be absolute */
        if(!nczm_isabsolutepath(url->path)) {
	    if((stat = nczm_suffix(z4cwd,url->path,&truepath))) goto done;
	} else
	    truepath = strdup(url->path);
    }

    /* Build the z4 state */
    if((z4map = calloc(1,sizeof(Z4MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    z4map->map.format = NCZM_NC4;
    z4map->map.url = ncuribuild(url,NULL,NULL,NCURIALL);
    z4map->map.mode = mode;
    z4map->map.flags = flags;
    z4map->map.api = (NCZMAP_API*)&zapi;
    z4map->path = truepath;
        truepath = NULL;

    if((stat=nc_open(z4map->path,mode,&ncid)))
        {stat = NC_ENOTFOUND; goto done;} /* could not open */
    z4map->ncid = ncid;
    
    if(mapp) *mapp = (NCZMAP*)z4map;    

done:
    nullfree(truepath);
    nullfree(z4cwd);
    ncurifree(url);
    if(stat) znc4close((NCZMAP*)z4map,0);
    return (stat);
}

/**************************************************/
/* Object API */

static int
znc4exists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    Z4MAP* z4map = (Z4MAP*)map;
    NClist* segments = nclistnew();
    int grpid;
    
    if((stat=nczm_split(key,segments)))
	goto done;    
    if((stat=zlookupobj(z4map,segments,&grpid)))
	goto done;
done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4len(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    Z4MAP* z4map = (Z4MAP*)map;
    NClist* segments = nclistnew();
    int grpid, vid;
    size_t dimlen, attlen;
    int dimids[1];

    if((stat=nczm_split(key,segments)))
	goto done;    

    if((stat=zlookupobj(z4map,segments,&grpid)))
	goto done;

    /* Look for a variable or attribute */
    if((stat = nc_inq_varid(grpid,ZCONTENT,&vid)) == NC_NOERR) {
        /* Get size for this variable */
        if((stat = nc_inq_vardimid(grpid,vid,dimids)))
	    goto done;
        /* Get size of the one and only dim */
        if((stat = nc_inq_dimlen(z4map->ncid,dimids[0],&dimlen)))
	    goto done;
        if(lenp) *lenp = (size64_t)dimlen;
    } else if((stat = nc_inq_att(grpid,NC_GLOBAL,ZCONTENT,NULL,&attlen)) == NC_NOERR) {
        if(lenp) *lenp = (size64_t)attlen;	
    } else /* Use NC_EACCESS to indicate not found */
	{stat = NC_EACCESS; goto done;}

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4define(NCZMAP* map, const char* key, size64_t len)
{
    int stat = NC_NOERR;
    int grpid;
    Z4MAP* z4map = (Z4MAP*)map; /* cast to true type */
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    stat = zlookupobj(z4map,segments,&grpid);
    if(stat == NC_NOERR) /* Already exists */
	goto done;
    else if(stat != NC_EACCESS) /* Some other kind of failure */
	goto done;

    if((stat = zcreateobj(z4map,segments,len,&grpid)))
	goto done;

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    int grpid,vid;
    Z4MAP* z4map = (Z4MAP*)map; /* cast to true type */
    size_t vstart[1];
    size_t vcount[1];
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    if((stat = zlookupobj(z4map,segments,&grpid)))
	goto done;

    /* Look for the data variable */
    if((stat = nc_inq_varid(grpid,ZCONTENT,&vid)))
	goto done;

    vstart[0] = (size_t)start;
    vcount[0] = (size_t)count;
    if((stat = nc_get_vara(grpid,vid,vstart,vcount,content)))
	goto done;

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    int grpid,vid;
    Z4MAP* z4map = (Z4MAP*)map; /* cast to true type */
    int dimids[1];
    size_t vstart[1];
    size_t vcount[1];
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    if((stat = zlookupobj(z4map,segments,&grpid)))
	goto done;

    /* Ensure the data variable exists */
    stat = nc_inq_varid(grpid,ZCONTENT,&vid);
    switch (stat) {
    case NC_NOERR: break;
    case NC_ENOTVAR:
        /* Create the dimension */
	if((stat = zcreatedim(z4map,grpid,&dimids[0])))
	    goto done;
	/* Create the variable */
        if((stat=nc_def_var(grpid, ZCONTENT, NC_UBYTE, 1, dimids, &vid)))
	    goto done;
	/* Set its properties */
	/* Var is automatically chunked because its dim is unlimited */
	break;
    default: goto done;
    }
    
    vstart[0] = (size_t)start;
    vcount[0] = (size_t)count;
    if((stat = nc_put_vara(grpid,vid,vstart,vcount,content)))
	goto done;

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4readmeta(NCZMAP* map, const char* key, size64_t avail, char* content)
{
    int stat = NC_NOERR;
    int grpid;
    Z4MAP* z4map = (Z4MAP*)map; /* cast to true type */
    NClist* segments = nclistnew();
    size_t alen;

    if((stat=nczm_split(key,segments)))
	goto done;    

    if((stat = zlookupobj(z4map,segments,&grpid)))
	goto done;

    /* Look for data attribute */
    if((stat = nc_inq_att(grpid,NC_GLOBAL,ZCONTENT,NULL,&alen)))
	goto done;

    /* Do some validation checks */
    if(avail < (size64_t)alen) {
	stat = NC_EVARSIZE; /* the content arg is too short */
	goto done;
    }
    if((stat = nc_get_att_text(grpid,NC_GLOBAL,ZCONTENT,content)))
	goto done;

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4writemeta(NCZMAP* map, const char* key, size64_t count, const char* content)
{
    int stat = NC_NOERR;
    int grpid;
    Z4MAP* z4map = (Z4MAP*)map; /* cast to true type */
    NClist* segments = nclistnew();

    if(map == NULL || key == NULL || count < 0 || content == NULL)
	{stat = NC_EINVAL; goto done;}

    if((stat=nczm_split(key,segments)))
	goto done;    

    /* Test the objects existence */
    if((stat = zlookupobj(z4map,segments,&grpid)))
	    goto done;	    

    if((stat = nc_put_att_text(grpid,NC_GLOBAL,ZCONTENT,(size_t)count,content)))
	goto done;

done:
    nclistfreeall(segments);
    return (stat);
}

static int
znc4close(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    Z4MAP* z4map = (Z4MAP*)map;
    char* path = NULL;

    if(map == NULL) return NC_NOERR;

    path = z4map->path;
        
    if((stat = nc_close(z4map->ncid)))
	goto done;
    if(delete) {
        if((stat = nc_delete(path)))
	    goto done;
    }

done:
    nullfree(z4map->path);
    nczm_clear(map);
    free(z4map);
    return (stat);
}

/*
Return a list of keys immediately "below" a specified prefix.
In theory, the returned list should be sorted in lexical order,
but it is not.
*/
int
znc4search(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    Z4MAP* z4map = (Z4MAP*)map;
    NClist* segments = nclistnew();
    int grpid, ngrps;
    int* subgrps = NULL;
    int* vars = NULL;
    int i;
	
    if((stat=nczm_split(prefix,segments)))
	goto done;    
    if(nclistlength(segments) > 0) {
        /* Fix the last name */
        size_t pos = nclistlength(segments)-1;
        char* name = nclistget(segments,pos);
        char zname[NC_MAX_NAME];
        zify(name,zname);
        nclistset(segments,pos,strdup(zname));
        nullfree(name);
    }
#ifdef DEBUG
  {
  int i;
  fprintf(stderr,"segments: %d: ",nclistlength(segments));
  for(i=0;i<nclistlength(segments);i++)
	fprintf(stderr," |%s|",(char*)nclistget(segments,i));
  }
  fprintf(stderr,"\n");
#endif

    /* Get grpid of the group for the prefix */
    if((stat = zlookupgroup(z4map,segments,0,&grpid)))
	goto done;
    /* get subgroup ids */
    if((stat = nc_inq_grps(grpid,&ngrps,NULL)))
	goto done;
    if((subgrps = calloc(1,sizeof(int)*ngrps)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((stat = nc_inq_grps(grpid,&ngrps,subgrps)))
	goto done;
    /* 1. Add the group names to the list of matches (zified) */
    for(i=0;i<ngrps;i++) {
	char gname[NC_MAX_NAME];
	char zname[NC_MAX_NAME];
	if((stat = nc_inq_grpname(subgrps[i],gname))) goto done;
	zify(gname,zname);
	nclistpush(matches,strdup(zname));	
    }
#if 0
    /* 2. Add any variables in the group */
    if((stat = nc_inq_varids(grpid,&nvars,NULL)))
	goto done;
    if((vars = calloc(1,sizeof(int)*nvars)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    if((stat = nc_inq_varids(grpid,&nvars,vars)))
	goto done;
    /* 1. Add the var names to the list of matches */
    for(i=0;i<nvars;i++) {
	char vname[NC_MAX_NAME];
	char zname[NC_MAX_NAME];
	if((stat = nc_inq_varname(grpid,vars[i],vname))) goto done;
	zify(vname,zname);
	nclistpush(matches,strdup(zname));	
    }
#endif

done:
    nullfree(vars);
    nullfree(subgrps);
    nclistfreeall(segments);
    return stat;
}

#if 0
/* Return a list of keys for all child nodes of the parent;
   It is up to the caller to figure out the type of the node.
   Assume that parentkey refers to a group; fail otherwise.
   The list includes subgroups.
*/
int
znc4children(NCZMAP* map, const char* parentkey, NClist* children)
{
    int stat = NC_NOERR;
    Z4MAP* z4map = (Z4MAP*)map;
    NClist* segments = nclistnew();
    int grpid;
    int ngrps;
    int* subgrps = NULL;
    int i;

    if((stat=nczm_split(parentkey,segments)))
	goto done;    
    if((stat = zlookupgroup(z4map,segments,0,&grpid)))
	goto done;
    /* Start by getting any subgroups */
    if((stat = nc_inq_grps(grpid,&ngrps,NULL)))
	goto done;
    if(ngrps > 0) {
        if((subgrps = calloc(1,sizeof(int)*ngrps)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        if((stat = nc_inq_grps(grpid,&ngrps,subgrps)))
	    goto done;
	/* Get the names of the subgroups */
	for(i=0;i<ngrps;i++) {
	    char name[NC_MAX_NAME];
	    char zname[NC_MAX_NAME];
	    char* path = NULL;
	    if((stat = nc_inq_grpname(subgrps[i],name)))
		goto done;
	    /* translate name */
	    zify(name,zname);
	    /* Create a full path */
	    if((stat = nczm_suffix(parentkey,zname,&path)))
		goto done;
	    /* Add to list of children */
	    nclistpush(children,path);
	    path = NULL; /* avoid mem errors */
	}		
    }

done:
    return stat;
}
#endif

/**************************************************/
/* Utilities */

/* Lookup a group by parsed path (segments)*/
/* Return NC_EACCESS if not found */
static int
zlookupgroup(Z4MAP* z4map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid;

    len = nclistlength(segments);
    len += nskip; /* leave off last nskip segments */
    grpid = z4map->ncid;
    for(i=0;i<len;i++) {
	int grpid2;
	const char* seg = nclistget(segments,i);
	char nc4name[NC_MAX_NAME];
	nc4ify(seg,nc4name);	
	if((stat=nc_inq_grp_ncid(grpid,nc4name,&grpid2)))
	    {stat = NC_EACCESS; goto done;}
	grpid = grpid2;
    }
    /* ok, so grpid should be it */
    if(grpidp) *grpidp = grpid;

done:
    return (stat);
}

/* Lookup an object */
/* Return NC_EACCESS if not found */
static int
zlookupobj(Z4MAP* z4map, NClist* segments, int* grpidp)
{
    int stat = NC_NOERR;
    int grpid;

    /* Lookup thru the final object group */
    if((stat = zlookupgroup(z4map,segments,0,&grpid)))
	goto done;
    if(grpidp) *grpidp = grpid;

done:
    return (stat);    
}

/* Create a group; assume all intermediate groups exist
   (do nothing if it already exists) */
static int
zcreategroup(Z4MAP* z4map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid, grpid2;
    const char* gname = NULL;
    char nc4name[NC_MAX_NAME];

    len = nclistlength(segments);
    len -= nskip; /* leave off last nskip segments (assume nskip > 0) */
    gname = nclistget(segments,len-1);
    grpid = z4map->ncid;
    /* Do all but last group */
    for(i=0;i<(len-1);i++) {
	const char* seg = nclistget(segments,i);
	nc4ify(seg,nc4name);	
	/* Does this group exist? */
	if((stat=nc_inq_grp_ncid(grpid,nc4name,&grpid2)) == NC_ENOGRP) {
	    {stat = NC_EACCESS; goto done;} /* missing intermediate */
	}
	grpid = grpid2;
    }
    /* Check status of last group */
    nc4ify(gname,nc4name);
    if((stat = nc_inq_grp_ncid(grpid,nc4name,&grpid2))) {
	if(stat != NC_ENOGRP) goto done;
        if((stat = nc_def_grp(grpid,nc4name,&grpid2)))
	    goto done;
	grpid = grpid2;
    }

    if(grpidp) *grpidp = grpid;

done:
    return (stat);
}

static int
zcreatedim(Z4MAP* z4map, int grpid, int* dimidp)
{
    int stat = NC_NOERR;
    int dimid;

    NC_UNUSED(z4map);

    if((stat=nc_inq_dimid(grpid,ZCONTENTDIM,&dimid))) {
	/* create it */
        if((stat=nc_def_dim(grpid,ZCONTENTDIM,NC_UNLIMITED,&dimid)))
	    goto done;
    }
    if(dimidp) *dimidp = dimid;

done:
    return (stat);
}

/* Create an object group corresponding to a key; create any
   necessary intermediates.
 */
static int
zcreateobj(Z4MAP* z4map, NClist* segments, size64_t len, int* grpidp)
{
    int skip,stat = NC_NOERR;
    int grpid;

    /* Create the whole path */
    skip = nclistlength(segments);
    for(skip--;skip >= 0; skip--) {
        if((stat = zcreategroup(z4map,segments,skip,&grpid)))
	goto done;
    }
    /* Last grpid should be one we want */
    if(grpidp) *grpidp = grpid;

done:
    return (stat);    
}

static int
parseurl(const char* path0, NCURI** urip)
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

/* Convert _z... name to .z... name */
static void
zify(const char* nc4name, char* zname)
{
    zname[0] = '\0';
    strlcat(zname,nc4name,NC_MAX_NAME);
    if(zname[0] == ZDOTNC4) zname[0] = NCZM_DOT;
}

/* Convert .z... name to _z... name */
static void
nc4ify(const char* zname, char* nc4name)
{
    nc4name[0] = '\0';
    strlcat(nc4name,zname,NC_MAX_NAME);
    if(nc4name[0] == NCZM_DOT) nc4name[0] = ZDOTNC4;
}

static int
z4getcwd(char** cwdp)
{
    char buf[4096];
    char* cwd = NULL;
    cwd = NCcwd(buf,sizeof(buf));
    if(cwd == NULL) return errno;
    if(cwdp) *cwdp = strdup(buf);
    return NC_NOERR;
}

/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_nz4 = {
    NCZM_NC4_V1,
    znc4create,
    znc4open,
};

static NCZMAP_API zapi = {
    NCZM_NC4_V1,
    znc4exists,
    znc4len,
    znc4define,
    znc4read,
    znc4write,
    znc4readmeta,
    znc4writemeta,
    znc4close,
    znc4search,
};
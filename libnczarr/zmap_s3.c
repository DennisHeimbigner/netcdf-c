/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "zincludes.h"
#include "nchttp.h"

/*
Map our simplified map model to an S3 bucket + objects.
The whole data

For the API, the mapping is as follows:
1. The whole dataset is mapped to a bucket.
2. Containment is simulated using the S3 key conventions.
3. Every object (e.g. group or array) is mapped to an S3 object
4. Meta data objects (e.g. .zgroup, .zarray, etc) are kept as an S3 object.
4. Actual variable data (for e.g. chunks) is stored as
   using an S3 object per chunk.
*/

#undef DEBUG

#define NCZM_S3_V1 1

/* the zarr meta prefix tag */
#define ZDOT '.'

/* define the attr/var name containing an objects content */
#define ZCONTENT "data"

/* Mnemonic */
#define Z4META 0

/* Define the "subclass" of NCZMAP */
typedef struct ZS3MAP {
    NCZMAP map;
    char* bucket; /* url prefix: https://<bucket> */
    CURL* curl;
    NCbytes* url; /* for holding complete object url */
    NCbytes* buf; /* for holding read/write data*/
} ZS3MAP;

/* Forward */
static NCZMAP_API zapi;
static int zs3close(NCZMAP* map, int delete);
static int zlookupgroup(ZS3MAP*, NClist* segments, int nskip, int* grpidp);
static int zlookupobj(ZS3MAP*, NClist* segments, int* objidp);
static int zcreategroup(ZS3MAP* zs3map, NClist* segments, int nskip, int* grpidp);
static int zcreateobj(ZS3MAP*, NClist* segments, size64_t, int* objidp);
static int zcreatedim(ZS3MAP*, size64_t dimsize, int* dimidp);
static int getbucketurl(const char* surl, char** bucketp);

/* Define the Dataset level API */

static int
zs3verify(const char *path, int mode, size64_t flags, void* parameters)
{
    int stat = NC_NOERR;
    char* filepath = NULL;
    int ncid;
    
    if((stat=getpath(path,&filepath)))
	goto done;

#if 0
    /* check for the group ".zarr" */
    s3ify(Z4METAROOT,s3name);
    if((stat=nc_inq_grp_ncid(ncid,s3name,&zid)))
	{stat = NC_ENOTNC; goto done;}
#endif

done:
    nullfree(filepath);
    return (stat);
}

static int
zs3create(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* bucket = NULL;
    ZS3MAP* zs3map = NULL;
    int ncid;
	
    NC_UNUSED(flags);
    NC_UNUSED(parameters);

    if((stat=getbucketpath(path,&bucket)))
	goto done;

    /* Build the z4 state */
    if((zs3map = calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zs3map->map.format = NCZM_S3;
    zs3map->map.url = strdup(path);
    zs3map->map.mode = mode;
    zs3map->map.flags = flags;
    zs3map->map.api = &zapi;
    zs3map->bucket = bucket; bucket = NULL;
    zs3map->url = ncbytesnew();
    zs3map->buf = ncbytesnew();
    if((stat = nc_http_open(zs3map->bucket,&sz3map->curl,NULL)))
	goto done;
    if(mapp) *mapp = (NCZMAP*)zs3map;    

done:
    nullfree(bucket);
    if(stat) zs3close((NCZMAP*)ZS3MAP,1);
    return (stat);
}

static int
zs3open(const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    char* bucket = NULL;
    ZS3MAP* zs3map = NULL;
    int ncid;
    
    if((stat=getbucketpath(path,&bucket)))
	goto done;

    /* Build the z4 state */
    if((zs3map = calloc(1,sizeof(ZS3MAP))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    zs3map->map.format = NCZM_S3;
    zs3map->map.url = strdup(path);
    zs3map->map.mode = mode;
    zs3map->map.flags = flags;
    zs3map->map.api = (NCZMAP_API*)&zapi;
    zs3map->bucket = bucket; bucket = NULL;
    zs3map->url = ncbytesnew();
    zs3map->buf = ncbytesnew();
    if((stat = nc_http_open(zs3map->bucket,&sz3map->curl,NULL)))
	goto done;
    if(mapp) *mapp = (NCZMAP*)ZS3MAP;    

done:
    nullfree(filepath);
    if(stat) zs3close((NCZMAP*)zs3map,0);
    return (stat);
}

/**************************************************/
/* Object API */

static int
zs3exists(NCZMAP* map, const char* key)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;

    if((stat = zs3len(zs3map, key, NULL)))
	{stat = NC_EACCESS; goto done;}

done:
    return (stat);
}

static int
zs3len(NCZMAP* map, const char* key, size64_t* lenp)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;

    if((stat=zs3buildurl(zs3map, key)))
	goto done;
    if((stat = nc_http_size(zs3map->curl,ncbytescontents(zs3map->url),lenp))
	{stat = NC_EACCESS; goto done;}

done:
    nullfree(objurl);
    return (stat);
}

static int
zs3define(NCZMAP* map, const char* key, size64_t len)
{
    int stat = NC_NOERR;
    int grpid;
    ZS3MAP* zs3map = (ZS3MAP*)map; /* cast to true type */

    if((stat = zs3exists(zs3map,key)) == NC_NOERR) 
	goto done; /* Already exists */
    else if(stat != NC_EACCESS) /* Some other kind of failure */
	goto done;

    if((stat = zcreateobj(???,segments,len,&grpid)))
	goto done;

done:
    return (stat);
}

static int
zs3read(NCZMAP* map, const char* key, size64_t start, size64_t count, void* content)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;
    char* objurl = NULL; /* basically bucket+key */

    if((objurl=zs3buildurl(zs3map->bucket, key))==NULL)
	{stat = NC_ENOMEM; goto done;}
    ncbytesclear(buf);
    if((stat = nc_http_read(zs3map->curl,objurl,start,count,zs3map->buf)))
	{stat = NC_EACCESS; goto done;}
    if(content) {memcpy(content,ncbytescontents(zs3map->buf),count);}

done:
    nullfree(objurl);
    return (stat);
}

static int
zs3write(NCZMAP* map, const char* key, size64_t start, size64_t count, const void* content)
{
    int stat = NC_NOERR;
    int grpid,vid;
    ZS3MAP* zs3map = (ZS3MAP*)map; /* cast to true type */
    size_t vstart[1];
    size_t vcount[1];
    NClist* segments = nclistnew();

    if((stat=nczm_split(key,segments)))
	goto done;    
    if((stat = zlookupobj(???,segments,&grpid)))
	goto done;

    /* Look for a data variable */
    if((stat = nc_inq_varid(grpid,ZCONTENT,&vid)))
	goto done;

    vstart[0] = (size_t)start;
    vcount[0] = (size_t)count;
    if((stat = nc_put_vara(grpid,vid,vstart,vcount,content)))
	goto done;

done:
    nclistfree(segments);
    return (stat);
}

static int
zs3readmeta(NCZMAP* map, const char* key, size64_t avail, char* content)
{
    int stat = NC_NOERR;
    int grpid;
    ZS3MAP* zs3map = (ZS3MAP*)map; /* cast to true type */
    NClist* segments = nclistnew();
    size_t alen;

    if((stat=nczm_split(key,segments)))
	goto done;    

    if((stat = zlookupobj(???,segments,&grpid)))
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
    nclistfree(segments);
    return (stat);
}

static int
zs3writemeta(NCZMAP* map, const char* key, size64_t count, const char* content)
{
    int stat = NC_NOERR;
    int grpid;
    ZS3MAP* zs3map = (ZS3MAP*)map; /* cast to true type */
    NClist* segments = nclistnew();

    if(map == NULL || key == NULL || count < 0 || content == NULL)
	{stat = NC_EINVAL; goto done;}

    if((stat=nczm_split(key,segments)))
	goto done;    

    /* Test the objects existence */
    if((stat = zlookupobj(???,segments,&grpid)))
	    goto done;	    

    if((stat = nc_put_att_text(grpid,NC_GLOBAL,ZCONTENT,(size_t)count,content)))
	goto done;

done:
    nclistfree(segments);
    return (stat);
}

static int
zs3close(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;
    char* path = NULL;

    path = ZS3MAP->path;
    ZS3MAP->path = NULL;
        
    if((stat = nc_close(???->ncid)))
	goto done;
    if(delete) {
        if((stat = nc_delete(path)))
	    goto done;
    }

done:
    nullfree(path);
    nczm_clear(map);
    nullfree(ZS3MAP->path);
    free(???);
    return (stat);
}

/*
Return a list of keys whose prefix matches the specified prefix string.
In theory, the returned list should be sorted in lexical order,
but breadth first will approximate this.
First element of the list is the prefix itself.
*/
int
zs3search(NCZMAP* map, const char* prefix, NClist* matches)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;
    NClist* segments = nclistnew();
    int grpid;
    int* subgrps = NULL;
    int i;
    NClist* queue = nclistnew(); /* To do the breadth first walk */

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
    if((stat = zlookupgroup(???,segments,0,&grpid)))
	goto done;
    /* Fill the queue in breadth first order */
    /* Start by pushing the prefix group */
    nclistinsert(queue,0,(void*)(uintptr_t)grpid);
    while(nclistlength(queue) > 0) {
	int g;
	char* fullpath = NULL;
	int ngrps;
#ifdef DEBUG
  {
  int i;
  fprintf(stderr,"queue: %d: ",nclistlength(queue));
  for(i=0;i<nclistlength(queue);i++) {
	int subg = (uintptr_t)nclistget(queue,i);
	char sgname[NC_MAX_NAME];
	nc_inq_grpname(subg,sgname);
	fprintf(stderr," (%d)|%s|",subg,sgname);
  }
  fprintf(stderr,"\n");
  }
#endif
	g = (int)(uintptr_t)nclistremove(queue,0);
	/* Construct and save the path of g */
	if((stat = NCZ_grpname_full(g,&fullpath))) goto done;
	nclistpush(matches,fullpath); /* save it */
	fullpath = NULL;
        /* get subgroup ids */
        if((stat = nc_inq_grps(grpid,&ngrps,NULL)))
	    goto done;
        if((subgrps = calloc(1,sizeof(int)*ngrps)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
        if((stat = nc_inq_grps(g,&ngrps,subgrps)))
	    goto done;
	/* Push onto end of the queue => breadth first */
	for(i=0;i<ngrps;i++) nclistpush(queue,(void*)(uintptr_t)subgrps[i]);
	/* repeat */
    }

done:
    nclistfree(queue);
    return stat;
}

#if 0
/* Return a list of keys for all child nodes of the parent;
   It is up to the caller to figure out the type of the node.
   Assume that parentkey refers to a group; fail otherwise.
   The list includes subgroups.
*/
int
zs3children(NCZMAP* map, const char* parentkey, NClist* children)
{
    int stat = NC_NOERR;
    ZS3MAP* zs3map = (ZS3MAP*)map;
    NClist* segments = nclistnew();
    int grpid;
    int ngrps;
    int* subgrps = NULL;
    int i;

    if((stat=nczm_split(parentkey,segments)))
	goto done;    
    if((stat = zlookupgroup(???,segments,0,&grpid)))
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
zlookupgroup(ZS3MAP* zs3map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid;

    len = nclistlength(segments);
    len += nskip; /* leave off last nskip segments */
    grpid = ZS3MAP->ncid;
    for(i=0;i<len;i++) {
	int grpid2;
	const char* seg = nclistget(segments,i);
	char s3name[NC_MAX_NAME];
	s3ify(seg,s3name);	
	if((stat=nc_inq_grp_ncid(grpid,s3name,&grpid2)))
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
zlookupobj(ZS3MAP* zs3map, NClist* segments, int* grpidp)
{
    int stat = NC_NOERR;
    int grpid;

    /* Lookup thru the final object group */
    if((stat = zlookupgroup(???,segments,0,&grpid)))
	goto done;
    if(grpidp) *grpidp = grpid;

done:
    return (stat);    
}

/* Create a group; assume all intermediate groups exist
   (do nothing if it already exists) */
static int
zcreategroup(ZS3MAP* zs3map, NClist* segments, int nskip, int* grpidp)
{
    int stat = NC_NOERR;
    int i, len, grpid, grpid2;
    const char* gname = NULL;
    char s3name[NC_MAX_NAME];

    len = nclistlength(segments);
    len -= nskip; /* leave off last nskip segments (assume nskip > 0) */
    gname = nclistget(segments,len-1);
    grpid = ZS3MAP->ncid;
    /* Do all but last group */
    for(i=0;i<(len-1);i++) {
	const char* seg = nclistget(segments,i);
	/* Does this group exist? */
	if((stat=nc_inq_grp_ncid(grpid,s3seg,&grpid2)) == NC_ENOGRP) {
	    {stat = NC_EACCESS; goto done;} /* missing intermediate */
	}
	grpid = grpid2;
    }
    /* Check status of last group */
    s3ify(gname,s3name);
    if((stat = nc_inq_grp_ncid(grpid,s3name,&grpid2))) {
	if(stat != NC_ENOGRP) goto done;
        if((stat = nc_def_grp(grpid,s3name,&grpid2)))
	    goto done;
	grpid = grpid2;
    }

    if(grpidp) *grpidp = grpid;

done:
    return (stat);
}

static int
zcreatedim(ZS3MAP* zs3map, size64_t dimsize, int* dimidp)
{
    int stat = NC_NOERR;
    char name[NC_MAX_NAME];
    int dimid;

    snprintf(name,sizeof(name),"dim%llu",dimsize);
    if((stat=nc_inq_dimid(???->ncid,name,&dimid))) {
	/* create it */
        if((stat=nc_def_dim(???->ncid,name,(size_t)dimsize,&dimid)))
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
zcreateobj(ZS3MAP* zs3map, const char* key)
{
    int i,nsegs,stat = NC_NOERR;
    NClist* segments = nclistnew();
    NCbytes* path = ncbytesnew();

    /* Split key so we can create intermediates */ 
    if((stat=nczm_split(key,segments)))
	goto done;

    /* Create the path prefix*/
    if((stat=zs3buildurl(zs3map, NULL)))
	goto done;
    nsegs = nclistlength(segments);
    for(i=0;i <nsegs; i++) {
	const char* seg = nclistget(segments,i);
	/* create key for intermediate */
        switch (stat=zs3extendurl(zs3map, seg)) {
	case NC_NOERR: break; /* exists */
	case NC_EACCESS: /* create it */
            if((stat = zcreateobject(zs3map,ncbytescontents(zs3map->url))))
		goto done;
	    break;
        default: goto done;
    }

done:
    return (stat);    
}

/* Extract the bucket path from a url string */
static int
getbucketurl(const char* surl, char** bucketp)
{
    int stat = NC_NOERR;
    NCURI* uri = NULL;
    char* bucket = NULL;
    if(!ncuriparse(surl,&uri)) {
	int blen;
	/* Check the protocol and extract the file part */	
	if(strcasecmp(uri->protocol,"s3") != 0)
	   && strcasecmp(uri->protocol,"http") != 0)
	   && strcasecmp(uri->protocol,"https") != 0)
	    {stat = NC_EURL; goto done;}
        /* Extract protocol + host plus port */	
	blen = strlen("https://");
        blen += strlen(uri->host)
	if(uri->port) blen += strlen(port) + 1;
	if((bucket = malloc(blen+1)) == NULL)
	   {stat = NC_ENOMEM; goto done;}
	bucket[0] = '\0';
	strlcat(bucket,"https://");
	strlcat(bucket,uri->host);
	if(uri->port != NULL) {
	    strlcat(bucket,":");
	    strlcat(bucket,uri->port);	    
	}
        if(bucketp) {*bucketp = bucket; bucket = NULL;}
    } else {
	stat = NC_EACCESS;
	goto done;
    }

done:
    ncurifree(uri);
    nullfree(bucket);
    return stat;
}

/**************************************************/
/* External API objects */

NCZMAP_DS_API zmap_s3 = {
    NCZM_S3_V1,
    zs3verify,
    zs3create,
    zs3open,
};

static NCZMAP_API zapi = {
    NCZM_S3_V1,
    zs3exists,
    zs3len,
    zs3define,
    zs3read,
    zs3write,
    zs3readmeta,
    zs3writemeta,
    zs3close,
    zs3search,
};

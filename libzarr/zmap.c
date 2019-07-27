/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/*
This API isolates the key-value pair mapping code
from the Zarr-based implementation of NetCDF-4.

It wraps an internal C dispatch table manager
for implementing an abstract data structure
loosely based on the Amazon S3 storage model.

Technically, S3 is a Key-Value Pair model
mapping a text key to an S3 *object*.
The object has an associated small set of what
I will call tags, which are themselves of the
form of key-value pairs, but where the key and value
are always text. As far as I can tell, Zarr never
uses these tags, so we do not include them in the zmap
data structure.

In practice, S3 is actually a tree
where the "contains" relationship is determined
by matching prefixes of the object keys.
So in this sense the object whose name is "/x/y/z"
is contained in the object whose name is "/x/y".

For this API, we use the prefix approach so that
for example, creating an object contained in another
object is defined by the common prefix model.

*/

#include "zincludes.h"

/**************************************************/
/* Import the current implementations */

extern NCZMAP_DS_API zmap_nc4;

/**************************************************/

/* Create ; complain if already exists */
int
nczmap_create(NCZM_IMPL impl, const char *path, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    
    if(mapp) *mapp = NULL;

    switch (impl) {
    case NCZM_NC4:
        stat = zmap_nc4.create(path, mode, flags, parameters, &map);
	if(stat) goto done;
	break;
    default:
	{stat = NC_ENOTBUILT; goto done;}
    }
    if(mapp) *mapp = map;
done:
    return stat;
}

/*
Terminology:
protocol: map implementation
format: zarr | tiledb
*/

int
nczmap_open(const char *path0, int mode, size64_t flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NCURI* uri = NULL;
    NCZM_IMPL impl = NCZM_UNDEF;

    if(mapp) *mapp = NULL;

    /* Exploit the path to figure out the implementation */
    if(!ncuriparse(path0,&uri)) {
        if(strcasecmp(uri->protocol,"file")==0) {
            /* Look at the fragment and see if it defines protocol= or proto= */
	    const char* proto = ncurilookup(uri,"proto");
   	    if(proto == NULL)
	        proto = ncurilookup(uri,"protocol");
	    if(proto == NULL)
	        impl = NCZM_NC4; /* Default */
	} else if(strcasecmp(uri->protocol,"s3")==0) {
	    impl = NCZM_S3;
	}
    }
    switch (impl) {
	case NCZM_NC4:
	    stat = zmap_nc4.open(path0,mode,flags,parameters,&map);
	    break;
	default:
	    {stat = NC_ENOTBUILT; goto done;} /* unknown lead protocol */
    }

done:
    if(!stat) {
        if(mapp) *mapp = map;
    }
    return stat;
}

int
nczmap_close(NCZMAP* map, int delete)
{
    int stat = NC_NOERR;
    stat = map->api->close(map,delete);
    return stat;
}

/**************************************************/
/* API Wrapper */

int
nczm_exists(NCZMAP* map, const char* key)
{
    return map->api->exists(map, key);
}

int
nczm_len(NCZMAP* map, const char* key, off64_t* lenp)
{
    return map->api->len(map, key, lenp);
}

int
nczm_read(NCZMAP* map, const char* key, off64_t start, off64_t count, char* content)
{
    return map->api->read(map, key, start, count, content);
}

int
nczm_write(NCZMAP* map, const char* keypath, off64_t start, off64_t count, const char* content)
{
    return map->api->write(map, keypath, start, count, content);
}

/**************************************************/
/* Utilities */

/* Split a path into pieces along '/' character */
int
nczm_split(const char* path, NClist* segments)
{
    int stat = NC_NOERR;
    const char* p = NULL;
    const char* q = NULL;
    ptrdiff_t len = 0;
    char* seg = NULL;

    if(path == NULL || strlen(path)==0 || segments == NULL)
	{stat = NC_EINVAL; goto done;}

    for(p=path;*p;) {
	q = strchr(p,NCZM_SEP);
	if(q==NULL)
	    q = p + strlen(p); /* point to trailing nul */
        len = (q - p);
	if(len == 0)
	    {stat = NC_EURL; goto done;}
	if((seg = malloc(len+1)) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	memcpy(seg,p,len);
	seg[len] = '\0';
	nclistpush(segments,seg);
	seg = NULL; /* avoid mem errors */
	if(*q) p = q+1; else p = q;
    }

done:
    if(seg != NULL) free(seg);
    return stat;
}

/* Join the first nseg segments into a path using '/' character */
int
nczm_joinprefix(NClist* segments, int nsegs, char** pathp)
{
    int stat = NC_NOERR;
    int i;
    NCbytes* buf = NULL;

    if(segments == NULL)
	{stat = NC_EINVAL; goto done;}
    if((buf = ncbytesnew()))
	{stat = NC_ENOMEM; goto done;}
    if(nclistlength(segments) < nsegs)
	nsegs = nclistlength(segments);
    if(nsegs == 0) {
	ncbytesappend(buf,NCZM_SEP);
	goto done;		
    }
    for(i=0;i<nsegs;i++) {
	const char* seg = nclistget(segments,i);
	ncbytesappend(buf,NCZM_SEP);
	ncbytescat(buf,seg);		
    }

done:
    if(!stat) {
	if(pathp) *pathp = ncbytesextract(buf);
    }
    ncbytesfree(buf);
    return stat;
}

/* Convenience: Join all segments into a path using '/' character */
int
nczm_join(NClist* segments, char** pathp)
{
    return nczm_joinprefix(segments,nclistlength(segments),pathp);
}

int
nczm_clear(NCZMAP* map)
{
    if(map) 
	nullfree(map->url);
    return NC_NOERR;
}

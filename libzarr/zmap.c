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

extern NCZMAP_API* zmap_file_api;

/**************************************************/

/*
Convert implementation enum to corresponding API
*/

NCZMAP_API*
nczmap_get_api(NCZM_IMPL impl)
{
    switch (impl) {
    case NCZM_FILE:
	return zmap_file_api;
    default:
	break;
    }
    return NULL;
}

int
nczmap_create(const char *path, NCZM_IMPL impl, int mode, unsigned flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NCZMAP_API* api = NULL;

    if(mapp) *mapp = NULL;
    if((api = nczmap_get_api(impl)) == NULL)
	{stat = NC_ENOTBUILT; goto done;}
    if((stat=api->create(api, path, mode, flags, parameters, &map))) goto done;
    if(mapp) *mapp = map;
done:
    return stat;
}

int
nczmap_open(const char *path, NCZM_IMPL impl, int mode, unsigned flags, void* parameters, NCZMAP** mapp)
{
    int stat = NC_NOERR;
    NCZMAP* map = NULL;
    NCZMAP_API* api = NULL;

    if(mapp) *mapp = NULL;
    if((api = nczmap_get_api(impl)) == NULL)
	{stat = NC_ENOTBUILT; goto done;}
    if((stat=api->open(api, path, mode, flags, parameters, &map))) goto done;
    if(mapp) *mapp = map;
done:
    return stat;
}

/**************************************************/
/* API Wrapper */

int
nczm_close(NCZMAP* map)
{
    return map->api->close(map);
}

int
nczm_clear(NCZMAP* map)
{
    return map->api->clear(map);
}

int
nczm_len(NCZMAP* map, const char* rpath, fileoffset_t* lenp)
{
    return map->api->len(map, rpath, lenp);
}

int
nczm_read(NCZMAP* map, const char* rpath, fileoffset_t start, fileoffset_t count, char** contentp)
{
    return map->api->read(map, rpath, start, count, contentp);
}

int
nczm_write(NCZMAP* map, const char* rpath, fileoffset_t start, fileoffset_t count, const char* content)
{
    return map->api->write(map, rpath, start, count, content);
}

int
nczm_rename(NCZMAP* map, const char* oldrpath, const char* newname)
{
    return map->api->rename(map, oldrpath, newname);
}


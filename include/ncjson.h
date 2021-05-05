/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

#ifndef NCJSON_H
#define NCJSON_H 1

#include "ncexternl.h"

#undef NCJSON_INLINE

/* Json object sorts */
#define NCJ_UNDEF    0
#define NCJ_STRING   1
#define NCJ_INT      2
#define NCJ_DOUBLE   3
#define NCJ_BOOLEAN  4
#define NCJ_DICT     5
#define NCJ_ARRAY    6
#define NCJ_NULL     7

#define NCJ_NSORTS   8

/* External types */
struct NClist;

/* Don't bother with unions: define
   a struct to store primitive values
   as unquoted strings. Sort will 
   provide more info.

   Also, this does not use a true hashmap
   but rather an envv style list where name
   and value alternate. This works under
   the assumption that we are generally
   iterating over the Dict rather than
   probing it.

*/

typedef struct NCjson NCjson;

#ifdef NCJSON_INTERNAL
struct NCjson {
    int sort;
    char* value;
    struct NClist* contents; /* For array|dict */
};
#endif

struct NCJconst {int bval; long long ival; double dval; char* sval;};

#define NCJF_MULTILINE 1

/* Parse */
EXTERNL int NCJparse(const char* text, unsigned flags, NCjson** jsonp);

/* Build */
EXTERNL int NCJnew(int sort, NCjson** object);

/* Convert a nul terminated string value to an NCjson object */
EXTERNL int NCJnewstring(int sort, const char* value, NCjson** jsonp);

/* Convert a counted string value to an NCjson object (+ nul term)*/
EXTERNL int NCJnewstringn(int sort, size_t len, const char* value, NCjson** jsonp);

/* Insert key-value pair into a dict object.
   key will be strdup'd.
*/
EXTERNL int NCJinsert(NCjson* object, char* key, NCjson* value);

/* Insert a string value into a json Dict|Array */
EXTERNL int NCJaddstring(NCjson* dictarray, int sort, const char* value);

/* Get ith pair from dict */
EXTERNL int NCJdictith(const NCjson* object, size_t i, const char** keyp, NCjson** valuep);

/* Get value for key from dict */
EXTERNL int NCJdictget(const NCjson* object, const char* key, NCjson** valuep);

/* Append value to an array or dict object. */
EXTERNL int NCJappend(NCjson* object, NCjson* value);

/* Get ith element from array */
EXTERNL int NCJarrayith(const NCjson* object, size_t i, NCjson** valuep);

/* Unparser to convert NCjson object to text in buffer */
EXTERNL int NCJunparse(const NCjson* json, int flags, char** textp);

/* Utilities */
EXTERNL void NCJreclaim(NCjson*);
EXTERNL int NCJclone(NCjson* json, NCjson** clonep); /* deep clone */

/* dump NCjson* object */
EXTERNL void NCJdump(const NCjson* json, int flags);

/* Convert one json sort to  value of another type; don't bother with union */
EXTERNL int NCJcvt(const NCjson* value, int outsort, struct NCJconst* output);

/* Macro defined functions */
#define NCJlength(json) \
(NCJsort(json) == NCJ_DICT ? (nclistlength(NCJcontents(json))/2) \
                        : (NCJsort(json) == NCJ_ARRAY ? (nclistlength(NCJcontents(json))) \
                        : 1))
#ifdef NCJSON_INLINE
/* Accessor functions */
#define NCJsort(json) ((json)->sort)
#define NCJvalue(json) ((json)->value)
#define NCJcontents(json) ((json)->contents)
#define NCJsetsort(json,x) {(json)->? = x;}
#define NCJvalue(json,x) {(json)->value = x;}
#define NCJcontents(json,x) {(json)->contents = x;}
#else
EXTERNL int NCJsort(const NCjson*);
EXTERNL char* NCJvalue(const NCjson*);
EXTERNL NClist* NCJcontents(const NCjson*);
EXTERNL void NCJsetsort(NCjson*,int);
EXTERNL void NCJsetvalue(NCjson*,char*);
EXTERNL void NCJsetcontents(NCjson*,NClist*);
#endif

#endif /*NCJSON_H*/

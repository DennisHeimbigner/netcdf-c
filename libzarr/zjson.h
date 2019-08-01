/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

#ifndef NCJSON_H
#define NCJSON_H 1

/* Json object sorts */
#define NCJ_UNDEF    0
#define NCJ_STRING   1
#define NCJ_INT      2
#define NCJ_DOUBLE   3
#define NCJ_BOOLEAN  4
#define NCJ_DICT     5
#define NCJ_ARRAY    6
#define NCJ_NULL     7

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
typedef struct NCjson {
    int sort;
    char* value;
    NClist* array;
    NClist* dict;
} NCjson;

#define NCJF_MULTILINE 1

/* Parse */
extern int NCjsonparse(const char* text, unsigned flags, NCjson** jsonp);

/* Build */
extern int NCJnew(int sort, NCjson** object);

/* Insert key-value pair into a dict object.
   key will be strdup'd.
*/
extern int NCJinsert(NCjson* object, char* key, NCjson* value);

/* Get ith pair from dict */
extern int NCJdictith(NCjson* object, size_t i, const char** keyp, NCjson** valuep);

/* Get value for key from dict */
extern int NCJdictget(NCjson* object, const char* key, NCjson** valuep);

/* Append value to an array object.
*/
extern int NCJappend(NCjson* object, NCjson* value);

/* Get ith element from array */
extern int NCJarrayith(NCjson* object, size_t i, NCjson** valuep);

/* Unparser to convert NCjson object to text in buffer */
extern int NCjsonunparse(const NCjson* json, int flags, char** textp);

/* Utilities */
extern void NCJreclaim(NCjson*);


#endif /*NCJSON_H*/

/* Copyright 2018, UCAR/Unidata.
   See the COPYRIGHT file for more information.
*/

#include "zincludes.h"
#include "zjson.h"

#define NCJ_LBRACKET '['
#define NCJ_RBRACKET ']'
#define NCJ_LBRACE '{'
#define NCJ_RBRACE '}'
#define NCJ_COLON ':'
#define NCJ_COMMA ','
#define NCJ_QUOTE '"'
#define NCJ_TRUE "true"
#define NCJ_FALSE "false"

#define NCJ_EOF 0
#define NCJ_ERR -1

#define WORD "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-$"

/*//////////////////////////////////////////////////*/

typedef struct NCJparser {
    char* text;
    char* pos;
    char* yytext; /* string or word */
    long long num;
    int tf;
    int err;
    struct {
        char* yytext;
	int token;
    } pushback;
} NCJparser;

/**************************************************/
/* Forward */
static int NCJparseR(NCJparser* parser, NCjson**);
static int NCJparseArray(NCJparser* parser, NClist** arrayp);
static int NCJparseDict(NCJparser* parser, NClist** dictp);
static int testbool(const char* word);
static int testint(const char* word);
static int testdouble(const char* word);
static int NCJlex(NCJparser* parser);
static int NCJyytext(NCJparser*, char* start, ptrdiff_t pdlen);
static void NCJreclaimArray(NClist*);
static void NCJreclaimDict(NClist*);

/**************************************************/
int
NCjsonparse(const char* text, unsigned flags, NCjson** jsonp)
{
    int stat = NC_NOERR;
    size_t len;
    NCJparser* parser = NULL;
    NCjson* json = NULL;

    if(text == NULL)
	{stat = NC_EINVAL; goto done;}
    if(json == NULL) goto done;
    parser = calloc(1,sizeof(NCJparser));
    if(parser == NULL)
	{stat = NC_ENOMEM; goto done;}
    json = calloc(1,sizeof(NCjson));
    if(json == NULL)
	{stat = NC_ENOMEM; goto done;}
    len = strlen(text);
    parser->text = (char*)malloc(len+1+1);
    if(parser->text == NULL)
	{stat = NC_ENOMEM; goto done;}
    parser->text[0] = '\0';
    strlcat(parser->text,text,len+1);
    parser->text[len] = '\0';
    parser->text[len+1] = '\0';
    if((stat=NCJparseR(parser,&json))) goto done;
    *jsonp = json;

done:
    if(parser != NULL) {
	nullfree(parser->text);
	nullfree(parser->yytext);
	free(parser);
    }
    if(stat != NC_NOERR)
	(void)NCJreclaim(json);
    return stat;
}

/*
Simple recursive descent
intertwined with dict and list parsers.

Invariants:
1. The json argument is provided by caller and filled in by NCJparseR.
2. Each call pushed back last unconsumed token
*/

static int
NCJparseR(NCJparser* parser, NCjson** jsonp)
{
    int stat = NC_NOERR;
    int token = NCJ_ERR;
    NCjson* json = NULL;

    if(jsonp == NULL)
	{stat = NC_EINTERNAL; goto done;}
    if((json = calloc(1,sizeof(NCjson))) == NULL)
	{stat = NC_ENOMEM; goto done;}

    if((token = NCJlex(parser)) == NCJ_ERR)
	{stat = NC_EINVAL; goto done;}
    switch (token) {
    case NCJ_EOF:
	break;
    case NCJ_NULL:
	json->sort = NCJ_NULL;
	break;
    case NCJ_BOOLEAN:
	json->sort = NCJ_BOOLEAN;
	json->value = strdup(parser->yytext);
	break;
    case NCJ_INT:
	json->sort = NCJ_INT;
	json->value = strdup(parser->yytext);
	break;
    case NCJ_DOUBLE:
	json->sort = NCJ_DOUBLE;
	json->value = strdup(parser->yytext);
	break;
    case NCJ_STRING:
	json->sort = NCJ_STRING;
	json->value = strdup(parser->yytext);
	break;
    case NCJ_LBRACE:
	json->sort = NCJ_DICT;
	if((stat = NCJparseDict(parser, &json->dict))) goto done;
	break;
    case NCJ_LBRACKET:
	json->sort = NCJ_ARRAY;
	if((stat = NCJparseArray(parser, &json->array))) goto done;
	break;
    default:
	stat = NC_EINVAL;
	break;
    }
    if(jsonp) {*jsonp = json; json = NULL;}

done:
    if(stat)
	NCJreclaim(json);
    return stat;
}

static int
NCJparseArray(NCJparser* parser, NClist** arrayp)
{
    int stat = NC_NOERR;
    NClist* array = NULL;
    int token = NCJ_ERR;
    NCjson* element = NULL;
    int stop = 0;

    if(arrayp) *arrayp = NULL;

    if((array = nclistnew()) == NULL)
	{stat = NC_ENOMEM; goto done;}
    
    /* [ ^e1,e2, ...en] */

    while(!stop) {
	/* Recurse to get the value ei */
	if((stat = NCJparseR(parser,&element))) goto done;
	/* Next token should be comma or rbracket */
	switch((token = NCJlex(parser))) {
	case NCJ_RBRACKET:
	    stop = 1;
	    /* fall thru */
	case NCJ_COMMA:
	    /* Append the ei to the list */
	    nclistpush(array,element);
	    element = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_ERR:
	default:
	    stat = NC_EINVAL;
	    goto done;
	}	
    }	
    if(arrayp) *arrayp = array;
    array = NULL;

done:
    if(element != NULL)
	NCJreclaim(element);
    if(stat)
	NCJreclaimArray(array);
    return stat;
}

static int
NCJparseDict(NCJparser* parser, NClist** dictp)
{
    int stat = NC_NOERR;
    NClist* dict = NULL;
    int token = NCJ_ERR;
    NCjson* value = NULL;
    char* key = NULL;
    int stop = 0;

    if(dictp) *dictp = NULL;

    if((dict = nclistnew()) == NULL)
	{stat = NC_ENOMEM; goto done;}
    
    /* { ^k1:v1,k2:v2, ...kn:vn] */

    while(!stop) {
	/* Get the key, which must be a word of some sort */
	switch((token = NCJlex(parser))) {
	case NCJ_STRING:	case NCJ_BOOLEAN:
	case NCJ_INT: case NCJ_DOUBLE:
	    key = strdup(parser->yytext);
	    break;
	case NCJ_EOF: case NCJ_ERR:
	default:
	    stat = NC_EINVAL;
	    goto done;
	}
	/* Next token must be colon  */
	switch((token = NCJlex(parser))) {
	case NCJ_COLON: break;
	case NCJ_ERR: case NCJ_EOF:
	default: stat = NC_EINVAL; goto done;
	}    
	/* Get the value */
	if((stat = NCJparseR(parser,&value))) goto done;
        /* Next token must be comma or RBRACE */
	switch((token = NCJlex(parser))) {
	case NCJ_RBRACKET:
	    stop = 1;
	    /* fall thru */
	case NCJ_COMMA:
	    /* Insert key value into dict: key first, then value */
	    if((stat=nclistpush(dict,key))) goto done;
	    if((stat=nclistpush(dict,value))) goto done;
	    if(key) free(key);
	    key = NULL;
	    value = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_ERR:
	default:
	    stat = NC_EINVAL;
	    goto done;
	}	
    }	
    if(dictp) *dictp = dict;
    dict = NULL;
done:
    if(key != NULL)
	free(key);
    if(value != NULL)
	NCJreclaim(value);
    if(stat)
	NCJreclaimDict(dict);
    return stat;
}

static int
NCJlex(NCJparser* parser)
{
    int c;
    int token = 0;
    char* start;
    char* next;

    if(parser->pushback.token != NCJ_EOF) {
	token = parser->pushback.token;
	NCJyytext(parser,parser->pushback.yytext,strlen(parser->pushback.yytext));
	nullfree(parser->pushback.yytext);
	parser->pushback.yytext = NULL;
	parser->pushback.token = 0;
	return token;
    }

    c = *parser->pos;
    if(c == '\0') {
	token = NCJ_EOF;
    } else if(strchr(WORD, c) != NULL) {
        start = parser->pos;
        next = start + 1;
	for(;;) {
	    c = *parser->pos++;
	    if(c <= ' ') break; /* whitespace */
	}
	if(!NCJyytext(parser,start,(next - start))) goto done;
	/* Discriminate the word string to get the proper sort */
	if(testbool(parser->yytext) == NC_NOERR)
	    token = NCJ_BOOLEAN;
	else if(testint(parser->yytext) == NC_NOERR)
	    token = NCJ_INT;
	else if(testdouble(parser->yytext) == NC_NOERR)
	    token = NCJ_DOUBLE;
	else
	    token = NCJ_STRING;
    } else if(c == NCJ_QUOTE) {
	parser->pos++;
	start = parser->pos;
	next = start+1;
	for(;;) {
	    c = *parser->pos++;
	    if(c == NCJ_QUOTE || c == '\0') break;
	}
	if(c == '\0') {
	    parser->err = NC_EINVAL;
	    token = NCJ_ERR;
	    goto done;
	}
	if(!NCJyytext(parser,start,(next - start))) goto done;
	token = NCJ_STRING;
    } else { /* single char token */
	token = *parser->pos++;
    }
done:
    if(parser->err) token = NCJ_ERR;
    return token;
}

static int
testbool(const char* word)
{
    if(strcasecmp(word,NCJ_TRUE)==0
       || strcasecmp(word,NCJ_FALSE)==0)
	return NC_EINVAL;
    return NC_NOERR;
}

static int
testint(const char* word)
{
    int ncvt;
    long long i;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lld",&i);
    return (ncvt == 1 ? NC_NOERR : NC_EINVAL);
}

static int
testdouble(const char* word)
{
    int ncvt;
    double d;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lg",&d);
    return (ncvt == 1 ? NC_NOERR : NC_EINVAL);
}

static int
NCJyytext(NCJparser* parser, char* start, ptrdiff_t pdlen)
{
    size_t len = (size_t)pdlen;
    if(parser->yytext == NULL)
	parser->yytext = (char*)malloc(len+1);
    else
	parser->yytext = (char*) realloc(parser->yytext,len+1);
    if(parser->yytext == NULL) return NC_ENOMEM;
    memcpy(parser->yytext,start,len);
    parser->yytext[len] = '\0';
    return NC_NOERR;
}

#if 0
static void
NCJpushback(NCJparser* parser, int token)
{
    parser->pushback.token = token;
    parser->pushback.yytext = strdup(parser->yytext);
}
#endif

/**************************************************/

void
NCJreclaim(NCjson* json)
{
    if(json == NULL) return;
    switch(json->sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING: 
	nullfree(json->value);
	break;
    case NCJ_DICT:
	NCJreclaimDict(json->dict);
	break;
    case NCJ_ARRAY:
	NCJreclaimArray(json->array);
	break;
    default: break; /* nothing to reclaim */
    }
    free(json);
}

static void
NCJreclaimArray(NClist* array)
{
    int i;
    for(i=0;i<nclistlength(array);i++) {
	NCjson* j = nclistget(array,i);
	NCJreclaim(j);
    }
    nclistfree(array);
}

static void
NCJreclaimDict(NClist* dict)
{
    int i;
    for(i=0;i<nclistlength(dict);i+=2) {
	char* key = NULL;
	NCjson* value = NULL;
	key = nclistget(dict,i);
	value = nclistget(dict,i+1);
	nullfree(key);
	NCJreclaim(value);
    }
    nclistfree(dict);
}

/**************************************************/
/* Build Functions */

int
NCJnew(int sort, NCjson** objectp)
{
    int stat = NC_NOERR;
    NCjson* object = NULL;

    if((object = calloc(1,sizeof(NCjson))) == NULL)
	{stat = NC_ENOMEM; goto done;}
    object->sort = sort;
    switch (sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING:
    case NCJ_NULL:
	break;
    case NCJ_DICT:
	object->dict = nclistnew();
	break;
    case NCJ_ARRAY:
	object->array = nclistnew();
	break;
    default: 
	stat = NC_EINVAL;
	goto done;
    }
    if(objectp) {*objectp = object; object = NULL;}

done:
    if(stat) NCJreclaim(object);
    return stat;
}

/* Insert key-value pair into a dict object.
   key will be strdup'd
*/
int
NCJinsert(NCjson* object, char* key, NCjson* value)
{
    if(object == NULL || object->sort != NCJ_DICT)
	return NC_EINTERNAL;
    nclistpush(object->dict,strdup(key));
    nclistpush(object->dict,value);
    return NC_NOERR;
}

int
NCJdictith(NCjson* object, size_t i, const char** keyp, NCjson** valuep)
{
    if(object == NULL || object->sort != NCJ_DICT)
	return NC_EINTERNAL;
    if(i >= (nclistlength(object->dict)/2))
	return NC_EINVAL;
    if(keyp) *keyp = nclistget(object->dict,2*i);
    if(valuep) *valuep = nclistget(object->dict,(2*i)+1);
    return NC_NOERR;
}

int
NCJdictget(NCjson* object, const char* key, NCjson** valuep)
{
    int i;
    if(object == NULL || object->sort != NCJ_DICT)
	return NC_EINTERNAL;
    if(valuep) *valuep = NULL;
    for(i=0;i<nclistlength(object->dict);i+=2) {
	const char* k = nclistget(object->dict,i);
	assert(k != NULL);
	if(strcmp(k,key)==0) {
            if(valuep) *valuep = nclistget(object->dict,i+1);
	    break;
	}
    }
    return NC_NOERR;
}

/* Append value to an array object.
*/
int
NCJappend(NCjson* object, NCjson* value)
{
    if(object == NULL || object->sort != NCJ_ARRAY)
	return NC_EINTERNAL;
    nclistpush(object->array,value);
    return NC_NOERR;
}

int
NCJarrayith(NCjson* object, size_t i, NCjson** valuep)
{
    if(object == NULL || object->sort != NCJ_DICT)
	return NC_EINTERNAL;
    if(valuep) *valuep = nclistget(object->array,i);
    return NC_NOERR;
}


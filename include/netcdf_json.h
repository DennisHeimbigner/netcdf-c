/* Copyright 2018, UCAR/Unidata.
Copyright 2018 Unidata

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef NCJSON_H
#define NCJSON_H 1

/* Json object sorts (note use of term sort rather than e.g. type or discriminant) */
#define NCJ_UNDEF    0
#define NCJ_STRING   1
#define NCJ_INT      2
#define NCJ_DOUBLE   3
#define NCJ_BOOLEAN  4
#define NCJ_DICT     5
#define NCJ_ARRAY    6
#define NCJ_NULL     7

#define NCJ_NSORTS   8

/* No flags are currently defined, but the argument is a placeholder */

/* Define a struct to store primitive values
   as unquoted strings. The sort will 
   provide more info.
   Do not bother with a union since
   the amount of saved space is minimal.
*/

typedef struct NCjson {
    int sort;     /* of this object */
    char* string; /* sort != DICT|ARRAY */
    struct NCjlist {
	    int len;
	    struct NCjson** contents;
    } list; /* sort == DICT|ARRAY */
} NCjson;

/* Structure to hold result of convertinf one json sort to  value of another type;
   don't use union so we can know when to reclaim sval
*/
struct NCJconst {int bval; long long ival; double dval; char* sval;};

#if defined(__cplusplus)
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#ifdef _WIN32
#define strcasecmp _stricmp
#else
#include <strings.h>
#endif

/* Primary API */

/* int return value is either 1 (ok) or 0 (failure) */

/* Parse */
static int NCJparse(const char* text, unsigned flags, NCjson** jsonp);

/* Build */
static int NCJnew(int sort, NCjson** object);

/* Recursively free NCjson instance */
static void NCJreclaim(NCjson*);

/* Assign a nul terminated string value to an NCjson object as its contents */
static int NCJnewstring(int sort, const char* value, NCjson** jsonp);

/* Assign a counted string value to an NCjson object as its contents */
static int NCJnewstringn(int sort, size_t len, const char* value, NCjson** jsonp);


/* Utilities */
static int NCJdictget(const NCjson* dict, const char* key, NCjson** valuep);

/* Convert one json sort to  value of another type; don't use union so we can know when to reclaim sval */
static int NCJcvt(const NCjson* value, int outsort, struct NCJconst* output);

/* Getters */
#define NCJsort(x) ((x)->sort)
#define NCJstring(x) ((x)->string)
#define NCJlength(x) ((x)==NULL ? 0 : (x)->list.len)
#define NCJcontents(x) ((x)->list.contents)
#define NCJith(x,i) ((x)->list.contents[i])

/* Setters */
#define NCJsetsort(x,s) (x)->sort=(s)
#define NCJsetstring(x,y) (x)->string=(y)
#define NCJsetcontents(x,c) (x)->list.contents=(c)
#define NCJsetlength(x,l) (x)->list.len=(l)

/* Misc */
#define NCJisatomic(j) ((j)->sort != NCJ_ARRAY && (j)->sort != NCJ_DICT && (j)->sort != NCJ_NULL && (j)->sort != NCJ_UNDEF)

#if defined(__cplusplus)
}
#endif

/*
TODO: make utf8 safe
*/

#undef DEBUG

#define NCJ_OK 0
#define NCJ_ERR 1

#define NCJ_EOF -1

#define NCJ_LBRACKET '['
#define NCJ_RBRACKET ']'
#define NCJ_LBRACE '{'
#define NCJ_RBRACE '}'
#define NCJ_COLON ':'
#define NCJ_COMMA ','
#define NCJ_QUOTE '"'
#define NCJ_ESCAPE '\\'
#define NCJ_TAG_TRUE "true"
#define NCJ_TAG_FALSE "false"
#define NCJ_TAG_NULL "null"

/* JSON_WORD Subsumes Number also */
#define JSON_WORD "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$+-."

/*//////////////////////////////////////////////////*/

typedef struct NCJparser {
    char* text;
    char* pos;
    size_t yylen; /* |yytext| */
    char* yytext; /* string or word */
    long long num;
    int tf;
    int err;
} NCJparser;

typedef struct NCJbuf {
    int len; /* |text|; does not include nul terminator */
    char* text; /* NULL || nul terminated */
} NCJbuf;

/**************************************************/
/* Forward */
static int NCJparseR(NCJparser* parser, NCjson**);
static int NCJparseArray(NCJparser* parser, struct NCjlist* array);
static int NCJparseDict(NCJparser* parser, struct NCjlist* dict);
static int testbool(const char* word);
static int testint(const char* word);
static int testdouble(const char* word);
static int testnull(const char* word);
static int NCJlex(NCJparser* parser);
static int NCJyytext(NCJparser*, char* start, size_t pdlen);
static void NCJreclaimArray(struct NCjlist*);
static void NCJreclaimDict(struct NCjlist*);
static int NCJunescape(NCJparser* parser);
static int listappend(struct NCjlist* list, NCjson* element);

#ifdef _WIN32
#define strdup _strdup
#endif

#ifndef nullfree
#define nullfree(x) {if(x)free(x);}
#endif
#ifndef nulldup
#define nulldup(x) ((x)?strdup(x):(x))
#endif

#ifdef DEBUG
static char* tokenname(int token);
#endif

/**************************************************/
static int
NCJparse(const char* text, unsigned flags, NCjson** jsonp)
{
    int stat = NCJ_OK;
    size_t len;
    NCJparser* parser = NULL;
    NCjson* json = NULL;

    /* Need at least 1 character of input */
    if(text == NULL || text[0] == '\0')
	{stat = NCJ_ERR; goto done;}
    if(jsonp == NULL) goto done;
    parser = calloc(1,sizeof(NCJparser));
    if(parser == NULL)
	{stat = NCJ_ERR; goto done;}
    len = strlen(text);
    parser->text = (char*)malloc(len+1+1);
    if(parser->text == NULL)
	{stat = NCJ_ERR; goto done;}
    strcpy(parser->text,text);
    parser->text[len] = '\0';
    parser->text[len+1] = '\0';
    parser->pos = &parser->text[0];
#ifdef DEBUG
fprintf(stderr,"json: |%s|\n",parser->text);
#endif
    if((stat=NCJparseR(parser,&json))) goto done;
    *jsonp = json;
    json = NULL;

done:
    if(parser != NULL) {
	nullfree(parser->text);
	nullfree(parser->yytext);
	free(parser);
    }
    (void)NCJreclaim(json);
    return (stat);
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
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* json = NULL;

    if(jsonp == NULL)
	{stat = NCJ_ERR; goto done;}
    if((token = NCJlex(parser)) == NCJ_UNDEF)
	{stat = NCJ_ERR; goto done;}
    switch (token) {
    case NCJ_EOF:
	break;
    case NCJ_NULL:
        if((stat = NCJnew(NCJ_NULL,&json))) goto done;
	break;
    case NCJ_BOOLEAN:
        if((stat = NCJnew(NCJ_BOOLEAN,&json))) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_INT:
        if((stat = NCJnew(NCJ_INT,&json))) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_DOUBLE:
        if((stat = NCJnew(NCJ_DOUBLE,&json))) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_STRING:
        if((stat = NCJnew(NCJ_STRING,&json))) goto done;
	json->string = strdup(parser->yytext);
	break;
    case NCJ_LBRACE:
        if((stat = NCJnew(NCJ_DICT,&json))) goto done;
	if((stat = NCJparseDict(parser, &json->list))) goto done;
	break;
    case NCJ_LBRACKET:
        if((stat = NCJnew(NCJ_ARRAY,&json))) goto done;
	if((stat = NCJparseArray(parser, &json->list))) goto done;
	break;
    case NCJ_RBRACE: /* We hit end of the dict we are parsing */
	parser->pos--; /* pushback so NCJparseArray will catch */
	json = NULL;
	break;
    case NCJ_RBRACKET:
	parser->pos--; /* pushback so NCJparseDict will catch */
	json = NULL;
	break;
    default:
	stat = NCJ_ERR;
	break;
    }
    if(jsonp && json) {*jsonp = json; json = NULL;}

done:
    NCJreclaim(json);
    return (stat);
}

static int
NCJparseArray(NCJparser* parser, struct NCjlist* arrayp)
{
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* element = NULL;
    int stop = 0;

    /* [ ^e1,e2, ...en] */

    while(!stop) {
	/* Recurse to get the value ei (might be null) */
	if((stat = NCJparseR(parser,&element))) goto done;
	token = NCJlex(parser); /* Get next token */
	/* Next token should be comma or rbracket */
	switch(token) {
	case NCJ_RBRACKET:
	    if(element != NULL) listappend(arrayp,element);
	    element = NULL;
	    stop = 1;
	    break;
	case NCJ_COMMA:
	    /* Append the ei to the list */
	    if(element == NULL) {stat = NCJ_ERR; goto done;} /* error */
	    listappend(arrayp,element);
	    element = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_UNDEF:
	default:
	    stat = NCJ_ERR;
	    goto done;
	}	
    }	

done:
    if(element != NULL)
	NCJreclaim(element);
    return (stat);
}

static int
NCJparseDict(NCJparser* parser, struct NCjlist* dictp)
{
    int stat = NCJ_OK;
    int token = NCJ_UNDEF;
    NCjson* value = NULL;
    NCjson* key = NULL;
    int stop = 0;

    /* { ^k1:v1,k2:v2, ...kn:vn] */

    while(!stop) {
	/* Get the key, which must be a word of some sort */
	token = NCJlex(parser);
	switch(token) {
	case NCJ_STRING:
	case NCJ_BOOLEAN:
	case NCJ_INT: case NCJ_DOUBLE: {
 	    if((stat=NCJnewstring(token,parser->yytext,&key))) goto done;
	    } break;
	case NCJ_RBRACE: /* End of containing Dict */
	    stop = 1;
	    continue; /* leave loop */
	case NCJ_EOF: case NCJ_UNDEF:
	default:
	    stat = NCJ_ERR;
	    goto done;
	}
	/* Next token must be colon*/
   	switch((token = NCJlex(parser))) {
	case NCJ_COLON: break;
	case NCJ_UNDEF: case NCJ_EOF:
	default: stat = NCJ_ERR; goto done;
	}    
	/* Get the value */
	if((stat = NCJparseR(parser,&value))) goto done;
        /* Next token must be comma or RBRACE */
	switch((token = NCJlex(parser))) {
	case NCJ_RBRACE:
	    stop = 1;
	    /* fall thru */
	case NCJ_COMMA:
	    /* Insert key value into dict: key first, then value */
	    listappend(dictp,key);
	    key = NULL;
	    listappend(dictp,value);
	    value = NULL;
	    break;
	case NCJ_EOF:
	case NCJ_UNDEF:
	default:
	    stat = NCJ_ERR;
	    goto done;
	}	
    }	

done:
    if(key != NULL)
	NCJreclaim(key);
    if(value != NULL)
	NCJreclaim(value);
    return (stat);
}

static int
NCJlex(NCJparser* parser)
{
    int c;
    int token = 0;
    char* start;
    size_t count;

    while(token == 0) { /* avoid need to goto when retrying */
	c = *parser->pos;
	if(c == '\0') {
	    token = NCJ_EOF;
	} else if(c <= ' ' || c == '\177') {
	    parser->pos++;
	    continue; /* ignore whitespace */
	} else if(strchr(JSON_WORD, c) != NULL) {
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
		if(c == '\0' || strchr(JSON_WORD,c) == NULL) break; /* end of word */
	    }
	    /* Pushback c if not whitespace */
	    parser->pos--;
	    count = ((parser->pos) - start);
	    if(NCJyytext(parser,start,count)) goto done;
	    /* Discriminate the word string to get the proper sort */
	    if(testbool(parser->yytext) == NCJ_OK)
		token = NCJ_BOOLEAN;
	    /* do int test first since double subsumes int */
	    else if(testint(parser->yytext) == NCJ_OK)
		token = NCJ_INT;
	    else if(testdouble(parser->yytext) == NCJ_OK)
		token = NCJ_DOUBLE;
	    else if(testnull(parser->yytext) == NCJ_OK)
		token = NCJ_NULL;
	    else
		token = NCJ_STRING;
	} else if(c == NCJ_QUOTE) {
	    parser->pos++;
	    start = parser->pos;
	    for(;;) {
		c = *parser->pos++;
		if(c == NCJ_ESCAPE) parser->pos++;
		else if(c == NCJ_QUOTE || c == '\0') break;
	    }
	    if(c == '\0') {
		parser->err = NCJ_ERR;
		token = NCJ_UNDEF;
		goto done;
	    }
	    count = ((parser->pos) - start) - 1; /* -1 for trailing quote */
	    if(NCJyytext(parser,start,count)) goto done;
	    if(NCJunescape(parser)) goto done;
	    token = NCJ_STRING;
	} else { /* single char token */
	    if(NCJyytext(parser,parser->pos,1)) goto done;
	    token = *parser->pos++;
	}
#ifdef DEBUG
fprintf(stderr,"%s(%d): |%s|\n",tokenname(token),token,parser->yytext);
#endif
    } /*for(;;)*/
done:
    if(parser->err) token = NCJ_UNDEF;
    return token;
}

static int
testnull(const char* word)
{
    if(strcasecmp(word,NCJ_TAG_NULL)==0)
	return NCJ_OK;
    return NCJ_ERR;
}

static int
testbool(const char* word)
{
    if(strcasecmp(word,NCJ_TAG_TRUE)==0
       || strcasecmp(word,NCJ_TAG_FALSE)==0)
	return NCJ_OK;
    return NCJ_ERR;
}

static int
testint(const char* word)
{
    int ncvt;
    long long i;
    int count = 0;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lld%n",&i,&count);
    return (ncvt == 1 && strlen(word)==count ? NCJ_OK : NCJ_ERR);
}

static int
testdouble(const char* word)
{
    int ncvt;
    double d;
    int count = 0;
    /* Check for Nan and Infinity */
    if(strcasecmp("nan",word)==0) return NCJ_OK;
    if(strcasecmp("infinity",word)==0) return NCJ_OK;
    if(strcasecmp("-infinity",word)==0) return NCJ_OK;
    /* Allow the XXXf versions as well */
    if(strcasecmp("nanf",word)==0) return NCJ_OK;
    if(strcasecmp("infinityf",word)==0) return NCJ_OK;
    if(strcasecmp("-infinityf",word)==0) return NCJ_OK;
    /* Try to convert to number */
    ncvt = sscanf(word,"%lg%n",&d,&count);
    return (ncvt == 1 && strlen(word)==count ? NCJ_OK : NCJ_ERR);
}

static int
NCJyytext(NCJparser* parser, char* start, size_t pdlen)
{
    size_t len = (size_t)pdlen;
    if(parser->yytext == NULL) {
	parser->yytext = (char*)malloc(len+1);
	parser->yylen = len;
    } else if(parser->yylen <= len) {
	parser->yytext = (char*) realloc(parser->yytext,len+1);
	parser->yylen = len;
    }
    if(parser->yytext == NULL) return NCJ_ERR;
    memcpy(parser->yytext,start,len);
    parser->yytext[len] = '\0';
    return NCJ_OK;
}

/**************************************************/

static void
NCJreclaim(NCjson* json)
{
    if(json == NULL) return;
    switch(json->sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING: 
	nullfree(json->string);
	break;
    case NCJ_DICT:
	NCJreclaimDict(&json->list);
	break;
    case NCJ_ARRAY:
	NCJreclaimArray(&json->list);
	break;
    default: break; /* nothing to reclaim */
    }
    free(json);
}

static void
NCJreclaimArray(struct NCjlist* array)
{
    int i;
    for(i=0;i<array->len;i++) {
	NCJreclaim(array->contents[i]);
    }
    nullfree(array->contents);
    array->contents = NULL;
}

static void
NCJreclaimDict(struct NCjlist* dict)
{
   NCJreclaimArray(dict);
}

/**************************************************/
/* Build Functions */

static int
NCJnew(int sort, NCjson** objectp)
{
    int stat = NCJ_OK;
    NCjson* object = NULL;

    if((object = (NCjson*)calloc(1,sizeof(NCjson))) == NULL)
	{stat = NCJ_ERR; goto done;}
    NCJsetsort(object,sort);
    switch (sort) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING:
    case NCJ_NULL:
	break;
    case NCJ_DICT:
    case NCJ_ARRAY:
	break;
    default: 
	stat = NCJ_ERR;
	goto done;
    }
    if(objectp) {*objectp = object; object = NULL;}

done:
    if(stat) NCJreclaim(object);
    return (stat);
}

static int
NCJnewstring(int sort, const char* value, NCjson** jsonp)
{
    return NCJnewstringn(sort,strlen(value),value,jsonp);
}

static int
NCJnewstringn(int sort, size_t len, const char* value, NCjson** jsonp)
{
    int stat = NCJ_OK;
    NCjson* json = NULL;

    if(jsonp) *jsonp = NULL;
    if(value == NULL)
        {stat = NCJ_ERR; goto done;}
    if((stat = NCJnew(sort,&json)))
	goto done;
    if((json->string = (char*)malloc(len+1))==NULL)
        {stat = NCJ_ERR; goto done;}
    memcpy(json->string,value,len);
    json->string[len] = '\0';
    if(jsonp) *jsonp = json;
    json = NULL; /* avoid memory errors */
done:
    NCJreclaim(json);
    return (stat);
}

static int
NCJdictget(const NCjson* dict, const char* key, NCjson** valuep)
{
    int i,stat = NCJ_OK;

    if(dict == NULL || dict->sort != NCJ_DICT)
        {stat = NCJ_ERR; goto done;}
    if(valuep) {*valuep = NULL;}
    for(i=0;i<NCJlength(dict);i+=2) {
	NCjson* jkey = NCJith(dict,i);
	if(jkey->string != NULL && strcmp(jkey->string,key)==0) {
	    if(valuep) {*valuep = NCJith(dict,i+1); break;}
	}	    
    }

done:
    return stat;
}

/* Unescape the text in parser->yytext; can
   do in place because unescaped string will
   always be shorter */
static int
NCJunescape(NCJparser* parser)
{
    char* p = parser->yytext;
    char* q = p;
    int c;
    for(;(c=*p++);) {
	if(c == NCJ_ESCAPE) {
	    c = *p++;
	    switch (c) {
	    case 'b': c = '\b'; break;
	    case 'f': c = '\f'; break;
	    case 'n': c = '\n'; break;
	    case 'r': c = '\r'; break;
	    case 't': c = '\t'; break;
	    case NCJ_QUOTE: c = c; break;
	    case NCJ_ESCAPE: c = c; break;
	    default: c = c; break;/* technically not Json conformant */
	    }
	}
	*q++ = c;
    }
    *q = '\0';
    return NCJ_OK;    
}

#ifdef DEBUG
static char*
tokenname(int token)
{
    switch (token) {
    case NCJ_STRING: return "NCJ_STRING";
    case NCJ_INT: return "NCJ_INT";
    case NCJ_DOUBLE: return "NCJ_DOUBLE";
    case NCJ_BOOLEAN: return "NCJ_BOOLEAN";
    case NCJ_DICT: return "NCJ_DICT";
    case NCJ_ARRAY: return "NCJ_ARRAY";
    case NCJ_NULL: return "NCJ_NULL";
    default:
	if(token > ' ' && token <= 127) {
	    static char s[4];
	    s[0] = '\'';
	    s[1] = (char)token;
	    s[2] = '\'';
	    s[3] = '\0';
	    return s;
	} else
	    break;
    }
    return "NCJ_UNDEF";
}
#endif


/* Convert a JSON value to an equivalent value of a specified sort */
static int
NCJcvt(const NCjson* jvalue, int outsort, struct NCJconst* output)
{
    int stat = NCJ_OK;

    if(output == NULL) goto done;

#undef CASE
#define CASE(t1,t2) ((t1)<<4 | (t2)) /* the shift constant must be larger than log2(NCJ_NSORTS) */
    switch (CASE(jvalue->sort,outsort)) {

    case CASE(NCJ_BOOLEAN,NCJ_BOOLEAN):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->bval = 0; else output->bval = 1;
	break;
    case CASE(NCJ_BOOLEAN,NCJ_INT):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->ival = 0; else output->ival = 1;
	break;	
    case CASE(NCJ_BOOLEAN,NCJ_DOUBLE):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->dval = 0.0; else output->dval = 1.0;
	break;	
    case CASE(NCJ_BOOLEAN,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_INT,NCJ_BOOLEAN):
	sscanf(jvalue->string,"%lldd",&output->ival);
	output->bval = (output->ival?1:0);
	break;	
    case CASE(NCJ_INT,NCJ_INT):
	sscanf(jvalue->string,"%lld",&output->ival);
	break;	
    case CASE(NCJ_INT,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lld",&output->ival);
	output->dval = (double)output->ival;
	break;	
    case CASE(NCJ_INT,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_DOUBLE,NCJ_BOOLEAN):
	sscanf(jvalue->string,"%lf",&output->dval);
	output->bval = (output->dval == 0?0:1);
	break;	
    case CASE(NCJ_DOUBLE,NCJ_INT):
	sscanf(jvalue->string,"%lf",&output->dval);
	output->ival = (long long)output->dval;
	break;	
    case CASE(NCJ_DOUBLE,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lf",&output->dval);
	break;	
    case CASE(NCJ_DOUBLE,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    case CASE(NCJ_STRING,NCJ_BOOLEAN):
	if(strcasecmp(jvalue->string,NCJ_TAG_FALSE)==0) output->bval = 0; else output->bval = 1;
	break;
    case CASE(NCJ_STRING,NCJ_INT):
	sscanf(jvalue->string,"%lld",&output->ival);
	break;
    case CASE(NCJ_STRING,NCJ_DOUBLE):
	sscanf(jvalue->string,"%lf",&output->dval);
	break;
    case CASE(NCJ_STRING,NCJ_STRING):
        output->sval = nulldup(jvalue->string);
	break;	

    default:
        stat = NCJ_ERR;
	break;
    }

done:
    return stat;
}

static int
listappend(struct NCjlist* list, NCjson* json)
{
    int stat = NCJ_OK;
    NCjson** newcontents = NULL;

    assert(list->len == 0 || list->contents != NULL);
    if(json == NULL)
        {stat = NCJ_ERR; goto done;}
    if(list->len == 0) {
	nullfree(list->contents);
	list->contents = (NCjson**)calloc(2,sizeof(NCjson*));
	if(list->contents == NULL)
	    {stat = NCJ_ERR; goto done;}
	list->contents[0] = json;
	list->len++;
    } else {
        if((newcontents = (NCjson**)calloc((2*list->len)+1,sizeof(NCjson*)))==NULL)
            {stat = NCJ_ERR; goto done;}
        memcpy(newcontents,list->contents,list->len*sizeof(NCjson*));
	newcontents[list->len] = json;
	list->len++;
	free(list->contents);
	list->contents = newcontents; newcontents = NULL;
    }

done:
    nullfree(newcontents);
    return stat;
}

/**************************************************/
/* Additional Unsed API Functions */

#if 0
/* Append value to an array or dict object. */
static int NCJappend(NCjson* object, NCjson* value);

/* Insert key-value pair into a dict object. key will be copied */
static int NCJinsert(NCjson* object, char* key, NCjson* value);

/* Unparser to convert NCjson object to text in buffer */
static int NCJunparse(const NCjson* json, unsigned flags, char** textp);

/* Deep clone a json object */
static int NCJclone(const NCjson* json, NCjson** clonep);

/* dump NCjson* object to output file */
static void NCJdump(const NCjson* json, unsigned flags, FILE*);
/**************************************************/
static int
NCJclone(const NCjson* json, NCjson** clonep)
{
    int stat = NCJ_OK;
    NCjson* clone = NULL;
    if(json == NULL) goto done;
    switch(NCJsort(json)) {
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
    case NCJ_STRING:
	if((stat=NCJnew(NCJsort(json),&clone))) goto done;
	if((NCJstring(clone) = strdup(NCJstring(json))) == NULL)
	    {stat = NCJ_ERR; goto done;}
	break;
    case NCJ_NULL:
	if((stat=NCJnew(NCJsort(json),&clone))) goto done;
	break;
    case NCJ_DICT:
	if((stat=NCJcloneDict(json,&clone))) goto done;
	break;
    case NCJ_ARRAY:
	if((stat=NCJcloneArray(json,&clone))) goto done;
	break;
    default: break; /* nothing to clone */
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return stat;
}

static int
NCJcloneArray(const NCjson* array, NCjson** clonep)
{
    int i, stat=NCJ_OK;
    NCjson* clone = NULL;
    if((stat=NCJnew(NCJ_ARRAY,&clone))) goto done;
    for(i=0;i<NCJlength(array);i++) {
	NCjson* elem = NCJith(array,i);
	NCjson* elemclone = NULL;
	if((stat=NCJclone(elem,&elemclone))) goto done;
	NCJappend(clone,elemclone);
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return stat;
}

static int
NCJcloneDict(const NCjson* dict, NCjson** clonep)
{
    int i, stat=NCJ_OK;
    NCjson* clone = NULL;
    if((stat=NCJnew(NCJ_DICT,&clone))) goto done;
    for(i=0;i<NCJlength(dict);i++) {
	NCjson* elem = NCJith(dict,i);
	NCjson* elemclone = NULL;
	if((stat=NCJclone(elem,&elemclone))) goto done;
	NCJappend(clone,elemclone);
    }
done:
    if(stat == NCJ_OK && clonep) {*clonep = clone; clone = NULL;}
    NCJreclaim(clone);    
    return stat;
}

static int
NCJaddstring(NCjson* json, int sort, const char* s)
{
    int stat = NCJ_OK;
    NCjson* jtmp = NULL;

    if(NCJsort(json) != NCJ_DICT && NCJsort(json) != NCJ_ARRAY)
        {stat = NCJ_ERR; goto done;}
    if((stat = NCJnewstring(sort, s, &jtmp))) goto done;
    if((stat = NCJappend(json,jtmp))) goto done;
    jtmp = NULL;
    
done:
    NCJreclaim(jtmp);
    return stat;
}

/* Insert key-value pair into a dict object. key will be strdup'd */
static int
NCJinsert(NCjson* object, char* key, NCjson* jvalue)
{
    int stat = NCJ_OK;
    NCjson* jkey = NULL;
    if(object == NULL || object->sort != NCJ_DICT || key == NULL || jvalue == NULL)
	{stat = NCJ_ERR; goto done;}
    if((stat = NCJnewstring(NCJ_STRING,key,&jkey))) goto done;
    if((stat = NCJappend(object,jkey))) goto done;
    if((stat = NCJappend(object,jvalue))) goto done;
done:
    return stat;
}

/* Append value to an array or dict object. */
static int
NCJappend(NCjson* object, NCjson* value)
{
    if(object == NULL || value == NULL)
	return NCJ_ERR;
    switch (object->sort) {
    case NCJ_ARRAY:
    case NCJ_DICT:
	listappend(&object->list,value);
	break;
    default:
	return NCJ_ERR;
    }
    return NCJ_OK;
}

/**************************************************/
/* Unparser to convert NCjson object to text in buffer */

static int
NCJunparse(const NCjson* json, unsigned flags, char** textp)
{
    int stat = NCJ_OK;
    NCJbuf buf = {0,NULL};
    if((stat = NCJunparseR(json,&buf,flags)))
	goto done;
    if(textp) {*textp = buf.text; buf.text = NULL; buf.len = 0;}
done:
    nullfree(buf.text);
    return (stat);
}

static int
NCJunparseR(const NCjson* json, NCJbuf* buf, unsigned flags)
{
    int stat = NCJ_OK;
    int i;

    switch (NCJsort(json)) {
    case NCJ_STRING:
	bytesappendquoted(buf,json->string);
	break;
    case NCJ_INT:
    case NCJ_DOUBLE:
    case NCJ_BOOLEAN:
	bytesappend(buf,json->string);
	break;
    case NCJ_DICT:
	bytesappendc(buf,NCJ_LBRACE);
	if(json->list.len > 0 && json->list.contents != NULL) {
	    int shortlist = 0;
	    for(i=0;!shortlist && i < json->list.len;i+=2) {
		if(i > 0) bytesappendc(buf,NCJ_COMMA);
		NCJunparseR(json->list.contents[i],buf,flags); /* key */
		bytesappendc(buf,NCJ_COLON);
		bytesappendc(buf,' ');
		/* Allow for the possibility of a short dict entry */
		if(json->list.contents[i+1] == NULL) { /* short */
	   	    bytesappendc(buf,'?');		
		    shortlist = 1;
		} else {
		    NCJunparseR(json->list.contents[i+1],buf,flags);
		}
	    }
	}
	bytesappendc(buf,NCJ_RBRACE);
	break;
    case NCJ_ARRAY:
	bytesappendc(buf,NCJ_LBRACKET);
	if(json->list.len > 0 && json->list.contents != NULL) {
	    for(i=0;i < json->list.len;i++) {
	        if(i > 0) bytesappendc(buf,NCJ_COMMA);
	        NCJunparseR(json->list.contents[i],buf,flags);
	    }
	}
	bytesappendc(buf,NCJ_RBRACKET);
	break;
    case NCJ_NULL:
	bytesappend(buf,"null");
	break;
    default:
	stat = NCJ_ERR; goto done;
    }
done:
    return (stat);
}

/* Escape a string and append to buf */
static int
escape(const char* text, NCJbuf* buf)
{
    const char* p = text;
    int c;
    for(;(c=*p++);) {
        char replace = 0;
        switch (c) {
	case '\b': replace = 'b'; break;
	case '\f': replace = 'f'; break;
	case '\n': replace = 'n'; break;
	case '\r': replace = 'r'; break;
	case '\t': replace = 't'; break;
	case NCJ_QUOTE: replace = '\''; break;
	case NCJ_ESCAPE: replace = '\\'; break;
	default: break;
	}
	if(replace) {
	    bytesappendc(buf,NCJ_ESCAPE);
	    bytesappendc(buf,replace);
	} else
	    bytesappendc(buf,c);
    }
    return NCJ_OK;    
}

static int
bytesappendquoted(NCJbuf* buf, const char* s)
{
    bytesappend(buf,"\"");
    escape(s,buf);
    bytesappend(buf,"\"");
    return NCJ_OK;
}

static void
NCJdump(const NCjson* json, unsigned flags, FILE* out)
{
    char* text = NULL;
    (void)NCJunparse(json,0,&text);
    if(out == NULL) out = stderr;
    fprintf(out,"%s\n",text);
    fflush(out);
    nullfree(text);
}

static int
bytesappend(NCJbuf* buf, const char* s)
{
    int stat = NCJ_OK;
    char* newtext = NULL;
    if(buf == NULL)
        {stat = NCJ_ERR; goto done;}
    if(s == NULL) s = "";
    if(buf->len == 0) {
	assert(buf->text == NULL);
	buf->text = strdup(s);
	if(buf->text == NULL)
	    {stat = NCJ_ERR; goto done;}
	buf->len = strlen(s);
    } else {
	size_t slen = strlen(s);
	size_t newlen = buf->len + slen + 1;
        if((newtext = (char*)malloc(newlen))==NULL)
            {stat = NCJ_ERR; goto done;}
        strcpy(newtext,buf->text);
	strcat(newtext,s);	
	free(buf->text); buf->text = NULL;
	buf->text = newtext; newtext = NULL;
	buf->len = newlen;
    }

done:
    nullfree(newtext);
    return stat;
}

static int
bytesappendc(NCJbuf* bufp, const char c)
{
    char s[2];
    s[0] = c;
    s[1] = '\0';
    return bytesappend(bufp,s);
}


#endif /*0*/

#endif /*NCJSON_H*/

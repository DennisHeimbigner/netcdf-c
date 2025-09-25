#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "nclist.h"
#include "ncuri.h"
#include "ncutil.h"
#include "ncglob.h"

#undef DEBUG

#ifndef nullfree
#define nullfree(s) {if((s)!=NULL) {free(s);} else {}}
#endif

#ifdef DEBUG
static void report(char* which, const char* glob, const char* elem, const char* elem0);
static const char* strify(const char* s);
//#define RPTCMP  report("compare",glob0,elem,NULL)
//#define RPTMATCH  report("match",glob0,elem,elem0)
#define RPTCMP
#define RPTMATCH
#else
#define RPTCMP
#define RPTMATCH
#endif

#define endelem do{elem += strlen(elem);}while(0)

/*Forward*/
static char* getstr(const char* _begin, const char* _end);

/* Match a URL string against a glob URL string: return 1 if match, 0 otherwise */
int
glob_match(const char* uristr, const char* globstr, GlobMatches* gm)
{
    NCURI* uri = NULL;
    NCURI* glob = NULL;
    int tf = 0;

    if(gm) memset(gm,0,sizeof(GlobMatches));
    if(ncuriparse(uristr,&uri) || uri == NULL) return 0;
    if(ncuriparseglob(globstr,&glob) || glob == NULL) return 0;
    tf = glob_match_uri(uri,glob,gm);
    ncurifree(uri);
    ncurifree(glob);
    return tf;    
}

/* Match an NCURI against a glob NCURI: return 1 if match, 0 otherwise */
int
glob_match_uri(const NCURI* uri, const NCURI* globuri, GlobMatches* gm)
{
    int tf = 0;
    GlobMatches substrs;

    if(gm) memset(gm,0,sizeof(GlobMatches));
    memset(&substrs,0,sizeof(GlobMatches));
    
    /* Match each piece in turn */
    if(!glob_match_piece(uri->protocol,globuri->protocol,NCGNOSEP,&substrs.proto)) {tf = 0; goto done;}
#ifdef DEBUG
    fprintf(stderr,">>> match proto: |%s| => |%s|\n",strify(globuri->protocol),strify(substrs.proto));
#endif
    if(!glob_match_piece(uri->host,globuri->host,'.',&substrs.host)) {tf = 0; goto done;}
#ifdef DEBUG
    fprintf(stderr,">>> match host: |%s| => |%s|\n",strify(globuri->host),strify(substrs.host));
#endif
    if(!glob_match_piece(uri->port,globuri->port,NCGNOSEP,&substrs.port)) {tf = 0; goto done;}
#ifdef DEBUG
    fprintf(stderr,">>> match port: |%s| => |%s|\n",strify(globuri->port),strify(substrs.port));
#endif
    if(!glob_match_piece(uri->path,globuri->path,'/',&substrs.path)) {tf = 0; goto done;}
#ifdef DEBUG
    fprintf(stderr,">>> match path: |%s| => |%s|\n",strify(globuri->path),strify(substrs.path));
#endif
    tf = 1;
done:
    if(gm) {*gm = substrs;} else {globmatchclear(&substrs);}
    return tf;
}

/* Match piece of a URL to an extended gitignore-style glob pattern: return 1 if match, 0 otherwise */
/* Note, this is used to match pieces/elements of a URL: protocol,host,port,or path. */
/* Generally called by client only for testing */
int
glob_match_piece(const char *elem, const char *glob, unsigned char separator, char** substrp)
{
    const char *elem1_backup = NULL;
    const char *glob1_backup = NULL;
    const char *elem2_backup = NULL;
    const char *glob2_backup = NULL;
    char* substr = NULL;
    const char* _begin = NULL;
    const char* _end = NULL;

#ifdef DEBUG
    /* sub-matching ptrs */
    const char* glob0 = NULL;
    const char* elem0 = NULL;
#endif

    _begin = elem;

    /* cases:
       glob       elem    match
       ------------------------
       null       null    yes
       null       !null   yes
       !null      null    test    note: set elem = ""
       !null      !null   test
    */     
    if(glob == NULL) goto done;
    if(glob != NULL && elem == NULL) elem = ""; /*then test*/
    /*if(glob != NULL && elem != NULL) test */
         
    /* If we get here, then we need to run the pattern matching algorithm */
#ifdef DEBUG
    fprintf(stderr,">>> piece test: glob=|%s| elem=|%s| sep=|%c|\n",glob,elem,(separator==NCGNOSEP?(unsigned char)'`':separator));
#endif    

#if 0
    /* match elem if glob contains a separator otherwise match the basename */
    if (*glob == separator)
    {
      /* if pathname starts with separator then ignore it */
      if (*elem == separator)
          elem++;
      glob++;
    }
    else if (strchr(glob, separator) == NULL)
    {
      const char *seppos = strrchr(elem, separator);
      if (seppos)
        elem = seppos + 1;
    }
#endif

    while (*elem != '\0')
    {
#ifdef DEBUG
	elem0 = elem;
        glob0 = glob;
#endif
        switch (*glob)
        {
          case '*':
            RPTCMP;
            if (*++glob == '*')
            {
              /* trailing ** match everything after separator */
              if (*++glob == '\0')
	          {RPTMATCH; endelem; goto done;}
              /* ** followed by a separator match zero or more directories */
              if (*glob != separator)
                {RPTMATCH; goto done;}
              /* new **-loop, discard *-loop */
              elem1_backup = NULL;
              glob1_backup = NULL;
              elem2_backup = elem;
              glob2_backup = ++glob;
	      RPTMATCH;
              continue;
            }
            /* trailing * matches everything except separator */
            elem1_backup = elem;
            glob1_backup = glob;
            RPTMATCH;
            continue;
          case '?':
            /* match any character except separator */
            RPTCMP;
            if (*elem == separator)
              break;
            elem++;
            glob++;
            RPTMATCH;
            continue;
          case '[':
          {
            int lastchr;
            int matched = 0;
            int reverse = glob[1] == '^' || glob[1] == '!' ? 1 : 0;
            RPTCMP;
            /* match any character in [...] except separator */
            if (*elem == separator)
              break;
            /* inverted character class */
            if (reverse)
              glob++;
            /* match character class */
            for (lastchr = 256; *++glob != '\0' && *glob != ']'; lastchr = *glob)
              if (lastchr < 256 && *glob == '-' && glob[1] != ']' && glob[1] != '\0' ?
                  *elem <= *++glob && *elem >= lastchr :
                  *elem == *glob)
                matched = 1;
            if (matched == reverse)
              break;
            elem++;
            if (*glob != '\0')
              glob++;
            RPTMATCH;
            continue;
          }
          case '{':
          {
            size_t i;
            const char* setstart = NULL;
            size_t setlen = 0;
            char* tmp = NULL;
            size_t match = 0;
            NClist* set = nclistnew();

            RPTCMP;
            /* match any simple strings in {...} */
            /* Locate substring to parse */
            setstart = ++glob;
            tmp = strchr(glob,'}');
            if(tmp == NULL)
                {fprintf(stderr,"*** Missing '}' terminator\n"); return 0;}
	    glob = ++tmp;
            setlen = (size_t)(tmp - setstart);
            if((tmp = (char*)malloc(setlen+1))==NULL)
                {fprintf(stderr,"*** Out of memory\n"); return 0;}
            memcpy(tmp,setstart,setlen);
            tmp[setlen] = '\0';

            /* Collect the string fields */
            if(NC_split_delim(tmp,',',set))
                {fprintf(stderr,"*** Malformed {...} pattern\n"); return 0;}
            nullfree(tmp); tmp = NULL;

            /* match */
            for(match=0,i=0;i<nclistlength(set);i++) {
                const char* s = (const char*)nclistget(set,i);
                if(strcmp(elem,s)==0) {match=strlen(s); break;}
            }
            elem += match;
            nclistfreeall(set); set = NULL;
            RPTMATCH;
            continue;
          }
          case '\\':
            RPTCMP;
            /* literal match \-escaped character */
            glob++;
            RPTMATCH;
            /* FALLTHROUGH */
          default:
            RPTCMP;
            /* match the current non-NUL character */
            if (*glob != *elem && !(*glob == separator && *elem == separator))
              break;
            elem++;
            glob++;
            RPTMATCH;
            continue;
        }
        if (glob1_backup != NULL && *elem1_backup != separator)
        {
          /* *-loop: backtrack to the last * but do not jump over separator */
          elem = ++elem1_backup;
          glob = glob1_backup;
          continue;
        }
        if (glob2_backup != NULL)
        {
          /* **-loop: backtrack to the last ** */
          elem = ++elem2_backup;
          glob = glob2_backup;
          continue;
        }
	RPTMATCH;
	goto done;
      }
      /* ignore trailing stars */
      while (*glob == '*')
        glob++;
done:
      _end = elem;
      substr = getstr(_begin,_end);
      if(substrp) {*substrp = substr; substr = NULL;}
      nullfree(substr);
     /* at end of elem means success if nothing else is left to match */
     return (glob == NULL || *glob == '\0') ? 1 : 0;
}

#if 0
static void
globmatchfill(GlobMatches* gm, MatchSubstrs* substrs)
{
    size_t len;

    memset(substrs,0,sizeof(MatchSubstrs));
    len = (size_t)(substrs->proto._end - substrs->proto._begin);
    if(len > 0) {
	gm->proto = (char*)malloc(len+1);
	memcpy(gm->proto,substrs->proto._begin,len);
	gm->proto[len] = '\0';
    }
    len = (size_t)(substrs->host._end - substrs->host._begin);
    if(len < 0) {
	gm->host = (char*)malloc(len+1);
	memcpy(gm->host,substrs->host._begin,len);
	gm->host[len] = '\0';
    }
    len = (size_t)(substrs->port._end - substrs->port._begin);
    if(len > 0) {
	gm->port = (char*)malloc(len+1);
	memcpy(gm->port,substrs->port._begin,len);
	gm->port[len] = '\0';
    }
    len = (size_t)(substrs->path._end - substrs->path._begin);
    if(len > 0) {
	gm->path = (char*)malloc(len+1);
	memcpy(gm->path,substrs->path._begin,len);
	gm->path[len] = '\0';
    }
}
#endif

void
globmatchclear(GlobMatches* gm)
{
    nullfree(gm->proto);
    nullfree(gm->host);
    nullfree(gm->port);
    nullfree(gm->path);
    memset(gm,0,sizeof(GlobMatches));
}

static char*
getstr(const char* _begin, const char* _end)
{
    size_t len;
    char* s = NULL;
    
    if(_begin == NULL || _end == NULL) return NULL;
    len = (size_t)(_end - _begin);
    if(len > 0) {
	if((s = (char*)malloc(len+1))==NULL) return NULL;
	memcpy(s,_begin,len);
	s[len] = '\0';
    }
    return s;
}

#ifdef DEBUG
static void
report(char* which, const char* glob, const char* elem, const char* elem0)
{
    char match[8192];

    if(elem0 == NULL) {
	fprintf(stderr,">>> %s: glob=|%s| elem=|%s|\n",which,glob,elem);
    } else {
	ptrdiff_t matchlen = (elem - elem0);
	memcpy(match,elem0,(size_t)matchlen);
	match[matchlen] = '\0';
	fprintf(stderr,">>> %s: glob=|%s| match=|%s|\n",which,glob,match);
    }
}

static const char*
strify(const char* s)
{
    if(s == NULL) s = "";
    return s;
}

#endif

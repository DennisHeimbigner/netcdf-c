/*********************************************************************
*    Copyright 2018, UCAR/Unidata
*    See netcdf/COPYRIGHT file for copying and redistribution conditions.
* ********************************************************************/

#ifndef NCGLOBPAT_H
#define NCGLOBPAT_H

#define NCGNOSEP ((unsigned char)255)

typedef struct GlobMatches {
    char* proto;
    char* host;
    char* port;
    char* path;
} GlobMatches;

struct GlobSubstr;
struct MatchSubstr;

#if defined(__cplusplus)
extern "C" {
#endif

/* Match a URI string against a glob URI string; return matches in gm */
int glob_match(const char* uristr, const char* globstr, GlobMatches* gm);

/* Match an NCURI against a glob NCURI; return matches in gm */
int glob_match_uri(const NCURI* uri, const NCURI* globuri, GlobMatches* gm);

/* Match piece of a URL to an extended gitignore-style glob pattern: return 1 if match, 0 otherwise */
/* Return match in matchp */
/* Note, this is used to match pieces/elements of a URL: protocol,host,port,or path. */
/* Generally called by client only for testing */
int glob_match_piece(const char *elem, const char *glob, unsigned char separator, char** substr);

void globmatchclear(GlobMatches* gm);

#if defined(__cplusplus)
}
#endif

#endif /*NCGLOBPAT_H*/

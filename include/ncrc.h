/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Common functionality for reading
and accessing rc files (e.g. .daprc).
*/

#ifndef NCRC_H
#define NCRC_H

/* Need these support includes */
#include <stdio.h>
#include "ncuri.h"
#include "nclist.h"
#include "ncbytes.h"

/* getenv() keys */
#define NCRCENVIGNORE "NCRCENV_IGNORE"
#define NCRCENVRC "NCRCENV_RC"
#define NCRCENVHOME "NCRCENV_HOME"

/* Known .aws profile keys */
#define AWS_ACCESS_KEY_ID "aws_access_key_id"
#define AWS_SECRET_ACCESS_KEY "aws_secret_access_key"
#define AWS_REGION "aws_region"

typedef struct NCRCentry {
        char* key;
        char* value;
	NCURI* uri; /* parsed glob URI */
} NCRCentry;

/* collect all the relevant info around the rc file and AWS */
typedef struct NCRCinfo {
	int ignore; /* if 1, then do not use any rc file */
	int loaded; /* 1 => already loaded */
        NClist* entries; /* the rc file entry store fields*/
        char* rcfile; /* specified rcfile; overrides anything else */
        char* rchome; /* Overrides $HOME when looking for .rc files */
	NClist* s3profiles; /* NClist<struct AWSprofile*> */
} NCRCinfo;

/* Opaque structures */

#if defined(__cplusplus)
extern "C" {
#endif

/* From drc.c */
EXTERNL void ncrc_initialize(void);
EXTERNL char* NC_rclookup(const char* key, const char* host, const char* port, const char* path);
EXTERNL char* NC_rclookupentry(NCRCentry* candidate);
EXTERNL char* NC_rclookup_with_uri(const char* key, const char* uri);
EXTERNL char* NC_rclookup_with_ncuri(const char* key, NCURI* uri);
EXTERNL int NC_rcfile_insert(NCRCentry* candidate);
EXTERNL void NC_rcfillfromuri(NCRCentry* dst, NCURI* src);
EXTERNL void NC_rcclearentry(NCRCentry* t);

/* Following are primarily for debugging */
/* Obtain the count of number of entries */
EXTERNL size_t NC_rcfile_length(NCRCinfo*);
/* Obtain the ith entry; return NULL if out of range */
EXTERNL NCRCentry* NC_rcfile_ith(NCRCinfo*,size_t);

/* For internal use */
EXTERNL void NC_rcclear(NCRCinfo* info);
EXTERNL void NC_rcclear(NCRCinfo* info);

#if defined(__cplusplus)
}
#endif

#endif /*NCRC_H*/

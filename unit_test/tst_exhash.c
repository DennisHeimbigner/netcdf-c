/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
Test the Extendible Hash Implementation of ncexhash
*/

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#include "netcdf.h"
#include "ncexhash.h"
#include "nccrc.h"

#define LEAFN 16

#define HMODE 3

#undef VERBOSE

//static unsigned N[] = {1000, 10000, 100000, 1000000, 0};
static unsigned N[] = {100, 0};

#define CHECK(expr) check((expr),__LINE__)
void check(int stat, int line)
{
    if(stat) {
	fprintf(stderr,"%d: (%d)%s\n",line,stat,nc_strerror(stat));
	fflush(stderr);
	exit(1);
    }
}

static exhashkey_t
hkeyfor(unsigned key)
{
    exhashkey_t hashkey = 0;
    int i;

    switch (HMODE) {
    case 1:
        hashkey = ncexhashkey((char*)&key,4);
	break;
    case 2:
        for(i=0;i<32;i++) {
	    hashkey |= (key & 0x1) << (31-i);
	    key = key >> 1;
        }
        break;
    case 3: /* Convert key to a random number using crc32 */
	hashkey = NC_crc64(0,(void*)&key,sizeof(exhashkey_t));
	break;
    default: abort();
    }
    return hashkey;
}

static void
reporttime(unsigned nelems, long* times, const char* tag)
{
    NC_UNUSED(nelems);
    NC_UNUSED(times);
    NC_UNUSED(tag);
#if 0
    double delta;
    double deltasec;
    delta = (double)(times[1] - times[0]);
    deltasec = delta / 1000000.0;
    fprintf(stderr,"\t%s:\t%5.1lf sec",tag,deltasec);
    fprintf(stderr," avg=%5.1lf usec\n",delta/nelems);
#endif
}

static void
xreporttime(unsigned nelems, struct timespec* times, const char* tag)
{
    double delta;
    double deltasec;
    long long nsec[2];
    
    nsec[0] = times[0].tv_nsec+(1000000000 * times[0].tv_sec);
    nsec[1] = times[1].tv_nsec+(1000000000 * times[1].tv_sec);

    delta = (double)(nsec[1] - nsec[0]);
    deltasec = delta / 1000000000.0;
    fprintf(stderr,"\t%s:\t%8.6lf sec",tag,deltasec);
    fprintf(stderr," avg=%5.2lf nsec\n",delta/nelems);
}

int
main(int argc, char** argv)
{
    NCexhash* map = NULL;
    exhashkey_t key;
    struct rusage ru;
    clockid_t clk_id = CLOCK_MONOTONIC;
    struct timespec xinserttime[2];
    struct timespec xreadtime[2];
    long  inserttime[2], readtime[2] ;	/* elapsed time in microseconds */
    unsigned* np;

    fprintf(stderr,"insert:\n");

#if 0
    {
    long microcvt, seccvt;
    struct timespec res;
    if(clock_getres(clk_id, &res) < 0)
	abort();
    fprintf(stderr,"xxx: tv_sec=%lld tv_nsec=%ld\n",(long long)res.tv_sec,res.tv_nsec);
    microcvt = res.tv_nsec;
    seccvt = microcvt * 1000000;
    fprintf(stderr,"xxx: seccvt=%lld microcvt=%lld\n",
		(long long)seccvt,(long long)microcvt);
    }
#endif

    for(np=N;*np;np++) {

    getrusage(RUSAGE_SELF, &ru);
    inserttime[0] = (1000000*(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec)
	     + ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
    clock_gettime(clk_id,&xinserttime[0]);

    map=ncexhashnew(LEAFN);
    if(map == NULL) CHECK(NC_EINVAL);
#ifdef VERBOSE
    fprintf(stderr,"new:\n"); ncexhashprint(map);    
#endif
    for(key=0;key<*np;key++) {
	exhashkey_t hashkey = hkeyfor(key);
#ifdef VERBOSE
        fprintf(stderr,"insert[%08x|%s->%u]:\n",hashkey,ncexbinstr(hashkey,32),key);
#endif
	CHECK(ncexhashput(map,hashkey,(uintptr_t)key));
    }
#ifdef VERBOSE
    fprintf(stderr,"insert.after:");ncexhashprint(map);    
#endif

    clock_gettime(clk_id,&xinserttime[1]);

    getrusage(RUSAGE_SELF, &ru);
    inserttime[1] = (1000000*(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec)
	     + ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
    fprintf(stderr,"read:\n");

    getrusage(RUSAGE_SELF, &ru);
    readtime[0] = (1000000*(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec)
	     + ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
    clock_gettime(clk_id,&xreadtime[0]);

    for(key=0;key<*np;key++) {
	uintptr_t data = 0;
	exhashkey_t hashkey = hkeyfor(key);
	CHECK(ncexhashget(map,hashkey,&data));
#ifdef VERBOSE
        fprintf(stderr,"read[%08x|%s->%u]:\n",hashkey,ncexbinstr(hashkey,EXHASHKEYBITS),(unsigned)data);
#endif
	if(data != key) fprintf(stderr,"\tMISMATCH\n");
    }

    clock_gettime(clk_id,&xreadtime[1]);
    getrusage(RUSAGE_SELF, &ru);
    readtime[1] = (1000000*(ru.ru_utime.tv_sec + ru.ru_stime.tv_sec)
	     + ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);

    fprintf(stderr,"statistics:\n"); ncexhashprintstats(map);    
    fprintf(stderr,"times: N=%u\n",*np);

    reporttime(*np, inserttime, "insert");
    reporttime(*np, readtime, "read");

    xreporttime(*np, xinserttime, "insert");
    xreporttime(*np, xreadtime, "read");

    /* Test iterator */
    /* Test removal */
abort();

    ncexhashfree(map);

    }


    return 0;
}

/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

#ifndef NCZCURLFUNCTIONS_H
#define NCZCURLFUNCTIONS_H

/* Aliases to older names */
#ifndef HAVE_CURLOPT_KEYPASSWD
#define CURLOPT_KEYPASSWD CURLOPT_SSLKEYPASSWD
#endif
#ifndef HAVE_CURLINFO_RESPONSE_CODE
#define CURLINFO_RESPONSE_CODE CURLINFO_HTTP_CODE
#endif

enum CURLFLAGTYPE {CF_UNKNOWN=0,CF_OTHER=1,CF_STRING=2,CF_LONG=3};
struct CURLFLAG {
    const char* name;
    int flag;
    int value;
    enum CURLFLAGTYPE type;
};

extern int NCZ_set_curlopt(NCZINFO* state, int flag, void* value);

extern int NCZ_set_flags_perfetch(NCZINFO*);
extern int NCZ_set_flags_perlink(NCZINFO*);

extern int NCZ_set_curlflag(NCZINFO*,int);

extern void NCZ_curl_debug(NCZINFO* state);

extern struct CURLFLAG* NCZ_curlflagbyname(const char* name);
extern void NCZ_curl_protocols(NCZINFO*);
extern int NCZ_get_rcproperties(NCZINFO* state);

#endif /*NCZCURLFUNCTIONS_H*/

NetCDF .rc File Support
======================================

<!-- double header is needed to workaround doxygen bug -->

NetCDF .rc File Support {#rc}
====================================

[TOC]

## Introduction {#rc_intro}

It is often convenient to have a set of commonly used properties
that can control the operation of the netcdf-c library.
These properties are commonly stored in what is called a ".rc" file.
The exact name for the file is usually library or application dependent.
For the netcdf-c library, two possible names are used, namely
`.ncrc` or `.dodsrc`.
The latter is historical and deprecated, but will be supported indefinitely.

## RC File Processing {#rc_process}
Locating the _.rc_ file is a multi-step process.

### Search Order

The netcdf-c library searches for, and loads from, the following files,
in this order:
1. $HOME/.ncrc
2. $HOME/.dodsrc
3. $CWD/.ncrc
4. $CWD/.dodsrc

*$HOME* is the user's home directory and *$CWD* is the current working directory. Entries in later files override entries from previously loaded .rc files.

It is strongly suggested that you pick a uniform location and a uniform name
and use them always. Otherwise you may observe unexpected results
when the netcdf-c library loads an rc file you did not expect.

### RC File Format

The rc file format is a series of lines of the general form:
* \<key\>=\<value\>
* [\<URL\>]\<key\>=\<value\>
* [#\<word\>]\<key\>=\<value\>

### Tag Constrained RC File Entries

Each line of the rc file can begin with a "tag" enclosed in
square brackets.  This tag can be used to activate entries in
the rc file.  Each tag can have one of two forms: (1) a URL or
(2) an (almost)arbitary string starting with the octotherpe
('#') character.

The URL form currently only pays attention to the "host:port"
part of the URL, and the ":port" portion can be missing.  The
reason that more of the url is not used is that libcurl's
authorization grain is not any finer than host level.

In the *#\<string\>* form, the string is any sequence of UTF-8
characters, except that it cannot include any of the following
ASCII characters: '[' ']' ';' '#'.

Here are some examples.
* HTTP.VERBOSE=1
* [remotetest.unidata.ucar.edu]HTTP.VERBOSE=1
* [fake.ucar.edu:9090]HTTP.VERBOSE=0
* [#opendap]HTTP.VERBOSE=0

If the url request from, say, the _netcdf_open_ method
has a host+port matching one of the prefixes in the rc file, then
the corresponding entry will be used, otherwise ignored.
This means that an entry with a matching host+port will take
precedence over an entry without a host+port.
Activating a \#tag entry requires passing in the tag to one of the
rc file API functions. Such tags can sometimes be specified
using the fragment section of a URL.

For example, the URL
````
    http://remotetest.unidata.ucar.edu/thredds/dodsC/testdata/testData.nc
````
will have HTTP.VERBOSE set to 1 because its host matches the example above.

Similarly,
````
    http://fake.ucar.edu:9090/dts/test.01#rc=opendap
````
will have HTTP.VERBOSE set to 0 because the fragment tag matches the tag
in the example above.

## Appendix A. All RC-File Keys {#rc_allkeys}

For completeness, this is the list of all rc-file keys.  If this
documentation is out of date with respect to the actual code,
the code is definitive.

<table>
<tr><th>Key</th><th>curl_easy_setopt Option</th>
<tr valign="top"><td>HTTP.DEFLATE</td><td>CUROPT_DEFLATE<br>with value "deflate,gzip"</td>
<tr><td>HTTP.VERBOSE</td><td>CUROPT_VERBOSE</td>
<tr><td>HTTP.TIMEOUT</td><td>CUROPT_TIMEOUT</td>
<tr><td>HTTP.USERAGENT</td><td>CUROPT_USERAGENT</td>
<tr><td>HTTP.COOKIEJAR</td><td>CUROPT_COOKIEJAR</td>
<tr><td>HTTP.COOKIE_JAR</td><td>CUROPT_COOKIEJAR</td>
<tr valign="top"><td>HTTP.PROXY.SERVER</td><td>CURLOPT_PROXY,<br>CURLOPT_PROXYPORT,<br>CURLOPT_PROXYUSERPWD</td>
<tr valign="top"><td>HTTP.PROXY_SERVER</td><td>CURLOPT_PROXY,<br>CURLOPT_PROXYPORT,<br>CURLOPT_PROXYUSERPWD</td>
<tr><td>HTTP.SSL.CERTIFICATE</td><td>CUROPT_SSLCERT</td>
<tr><td>HTTP.SSL.KEY</td><td>CUROPT_SSLKEY</td>
<tr><td>HTTP.SSL.KEYPASSWORD</td><td>CUROPT_KEYPASSWORD</td>
<tr><td>HTTP.SSL.CAINFO</td><td>CUROPT_CAINFO</td>
<tr><td>HTTP.SSL.CAPATH</td><td>CUROPT_CAPATH</td>
<tr><td>HTTP.SSL.VERIFYPEER</td><td>CUROPT_SSL_VERIFYPEER</td>
<tr><td>HTTP.CREDENTIALS.USERPASSWORD</td><td>CUROPT_USERPASSWORD</td>
<tr><td>HTTP.CREDENTIALS.USERNAME</td><td>CUROPT_USERNAME</td>
<tr><td>HTTP.CREDENTIALS.PASSWORD</td><td>CUROPT_PASSWORD</td>
<tr><td>HTTP.NETRC</td><td>CURLOPT_NETRC,CURLOPT_NETRC_FILE</td>
</table>

## Point of Contact

__Author__: Dennis Heimbigner<br>
__Email__: dmh at ucar dot edu
__Initial Version__: 06/15/2023<br>
__Last Revised__: 06/15/2023


Appendix G. Environment Variables and .RC files QuickStart {#nc_env_quickstart}
==============================

[TOC]

The netCDF-c library provides several parameterization mechanisms to
control its behavior at run-time. The term _run-time_ means that the
library's behavior can be changed every time the library is initialized

The most commonly used parameterization mechanisms are:
1. Environment Variables -- accessed by the getenv() function.
2. .RC files -- accessed from the file system.

## Environment Variables {#nc_env_vars}

The following table describes (most of) the environment variables
used by the netcdf-c library. There are some not listed that are only
used for specialized debugging.

<table>
<tr><th>Name<th>Description
<tr><td>ALLUSERSPROFILE<td>This is more-or-less the Windows equivalent of "HOME"
<tr><td>AWS_ACCESS_KEY_ID<td>Used by the aws access libraries; overrides ${HOME}/.aws/config entries.
<tr><td>AWS_CONFIG_FILE<td>Used by the aws access libraries; specifies absolute path to the config file.
<tr><td>AWS_DEFAULT_REGION<td>Used by the aws access libraries; specifies default S3 region.
<tr><td>AWS_PROFILE<td>Used by the aws access libraries; specifies default AWS profile.
<tr><td>AWS_REGION<td>Used by the aws access libraries; specifies specific region to use.
<tr><td>AWS_SECRET_ACCESS_KEY<td>Used by the aws access libraries; overrides ${HOME}/.aws/config entries.
<tr><td>CURLOPT_VERBOSE<td>Causes libcurl to produce verbose output.
<tr><td>HOME<td>The user's home directory.
<tr><td>MSYS2_PREFIX<td>If platform is MSYS2, then specify the root prefix.
<tr><td>NC_DEFAULT_CREATE_PERMS<td>For NCZarr, specify the default creation permissions for a file.
<tr><td>NC_DEFAULT_DIR_PERMS<td>For NCZarr, specify the default creation permissions for a directory.
<tr><td>NCLOGGING<td>Specify the log level: one of "OFF","ERR","WARN","NOTE","DEBUG".
<tr><td>NCPATHDEBUG<td>Causes path manager to output debugging information.
<tr><td>NCRCENV_HOME<td>Overrides ${HOME} as the location of the .rc file.
<tr><td>NCRCENV_IGNORE<td>Causes any .rc files to be ignored.
<tr><td>NCRCENV_RC<td>The absolute path to use for the .rc file.
<tr><td>NCTRACING<td>Specify the level of tracing detail.
<tr><td>NCZARRFORMAT<td>Force use of a specific Zarr format version: 2 or 3.
<tr><td>NETCDF_LOG_LEVEL<td>Specify the log level for HDF5 logging (separate from e.g. NCLOGGING).
<tr><td>TEMP<td>For Windows platform, specifies the location of a directory to store temporary files.
<tr><td>USERPROFILE<td>For Windows platform, overrides ${HOME}.
</table>

## Resource Control (.rc) Files {#nc_env_rc}

In addition to using environment variables,
the netcdf-c library supports run-time configuration
of the library using the so-called ".rc" file mechanism.
This means that as part of its initialization, the netcdf-c
library will search for and process a set of files where
these files contain entries specifying (key,value) pairs.
These pairs are compiled into a single internal database
that can be queried by other parts of the netcdf-c library.

Currently the .rc file used by the netcdf library is called ".ncrc".
For historical reasons, two other files can be used, but are deprecated.
These files are ".dodsrc" and ".daprc".

### Locating The _.rc_ Files

For historical reasons, multiple .rc files are allowed.

### Search Order

The netcdf-c library searches for, and loads from, the following files,
in this order:
1. $HOME/.ncrc
2. $HOME/.dapsrc
3. $HOME/.dodsrc
4. $CWD/.ncrc
5. $CWD/.daprc
6. $CWD/.dodsrc

*\$HOME* is the user's home directory and *\$CWD* is the current working directory.
Entries in later files override any of the earlier files

It is strongly suggested that you pick a uniform location and a uniform name
and use them always. Otherwise you may observe unexpected results
when the netcdf-c library loads an rc file you did not expect.

### RC File Format

The rc file format is a series of lines of the general form:
````
    [<GLOB-URL>]<key>=<value>
````
where the bracket-enclosed GLOB-URL is optional. Note that the brackets
are part of the line.

### URL Constrained RC File Entries

Each line of the rc file can begin with a special form of URL enclosed in
square brackets.
The URL is special in that it is a "glob"-style URL.
Let <glob> be any legal .gitignore glob pattern.
The general form of the glob-url is as follows:
````
<glob>://<glob>:<glob>/<glob>
````
So given a URL, it can be applied to the glob-url where:
* The first \<glob\> in the glob-url must match the protocol/scheme from the URL.
* The second \<glob\> must match the host from the URL.
* The third \<glob\> is optional and if present, it must match the port from the URL. The glob-url without a port can be specified using either of these patterns:<br>
    ````<glob>://<glob>/<glob>```` or ````<glob>://<glob>:/<glob>````
* The fourth \<glob\>; must match the path from the URL.

If any \<glob\> is missing, then it is treated as the "**" pattern,
which effectively means that it matches any URL at that \<glob\> position.

Here are some examples.
````
    [*://**:**/**]HTTP.VERBOSE=1 # The most general possible
or
    [{file,http,https}:///*.ucar.edu:9090]HTTP.VERBOSE=0
````

For selection purposes, if the path argument for
_nc_open()_ or _nc_create()_ takes the form of a URL,
then that URL is used to search for .ncrc entries.
A .ncrc entry with glob-url that matches the URL argument will take
precedence over an entry without a glob-url.

For example, passing this URL to _nc_open_
````
    http://remotetest.unidata.ucar.edu/thredds/dodsC/testdata/testData.nc
````
will have HTTP.VERBOSE set to 1 because its host and path match the example above.

Similarly, using this path
````
    http://fake.ucar.edu:9090/dts/test.01
````
will have HTTP.VERBOSE set to 0 because its host+port matches the example above.

### .gitignore Glob patterns

The form of a glob pattern is defined using an extended form of
[.gitignore glob patterns](https://git-scm.com/docs/gitignore#_pattern_format).

The set of glob patterns has been extended with one additional form of pattern:
a list of values to match. The general form is as follows:<br>
````{String1,String2,...}````<br>

### Programmatic Access to .rc File

It is possible for client programs to have limited access to the internal .rc table through the following API.
* ````char* nc_rc_get(const char* key);````
    Get the value corresponding to key or return NULL if not defined. The caller must free the resulting value.
* ````int nc_rc_set(const char* key, const char* value);````
    Set/overwrite the value corresponding to the specified key.

Note that this API does not (currently) support URL prefixed keys, so the client will need to take this into account.

### Defined .rc file keys

There a a number of keys used by the netcdf-c library. Most of them
are authorization-related. The file "auth.md" describes these keys.

Other keys are as follows:
* libdap4/d4curlfunctions.c and oc2/ocinternal.c
    - HTTP.READ.BUFFERSIZE -- set the read buffer size for DAP2/4 connection
    - HTTP.KEEPALIVE -- turn on keep-alive for DAP2/4 connection
* libdispatch/ds3util.c
    - AWS.PROFILE -- alternate way to specify the default AWS profile
    - AWS.REGION --  alternate way to specify the default AWS region
* libnczarr/zinternal.c
    - ZARR.DIMENSION_SEPARATOR -- alternate way to specify the Zarr dimension separator character
* oc2/occurlfunctions.c
    - HTTP.NETRC -- alternate way to specify the path of the .netrc file

## History {#nc_env_history}

__Author__: Dennis Heimbigner<br>
__Email__: dmh at ucar dot edu<br>
__Initial Version__: 01/09/2023<br>
__Last Revised__: 09/04/2025

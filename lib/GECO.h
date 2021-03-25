/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECO.h
 *
 *  Project-wide includes, typedefs, etc.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECO_H__
#define __GECO_H__

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define __USE_GNU
#include <unistd.h>

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <ctype.h>
#include <math.h>

//
// Libxml2 is required:
//
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#if defined(LIBXML_XPATH_ENABLED)
#else
#error libxml2 lacks support for xpath
#endif

extern const char *GECOLibraryVersion;

/*!
  @constant GECODirectoryPrefix
  @discussion
    String constant containing the directory in which GECO was installed.
*/
extern const char *GECODirectoryPrefix;

/*!
  @constant GECODirectoryBin
  @discussion
    String constant containing the directory in which GECO executables are
    installed.
*/
extern const char *GECODirectoryBin;

/*!
  @constant GECODirectoryEtc
  @discussion
    String constant containing the directory in which GECO preferences are
    installed.
*/
extern const char *GECODirectoryEtc;

/*!
  @constant GECODirectoryLib
  @discussion
    String constant containing the directory in which GECO libraries are
    installed.
*/
extern const char *GECODirectoryLib;

/*!
  @constant GECOUnknownJobId
*/
extern const long int GECOUnknownJobId;

/*!
  @constant GECOUnknownJobId
*/
extern const long int GECOUnknownTaskId;

/*!
  @function GECOGetStateDir
  @discussion
    Returns a string constant containing the directory in which GECO will
    store job state data.
*/
const char* GECOGetStateDir(void);
/*!
  @function GECOSetStateDir
  @discussion
    Override the default state dirctory path with stateDir.
*/
bool GECOSetStateDir(const char *stateDir);

/*!
  @function GECO_astrcatm
  @discussion
    Concatenate multiple strings, using malloc() to allocate memory
    for the product.
    
    The function accepts one or more C string pointers and a final
    NULL argument as a terminator.
  @result
    A C string containing the sequence of strings concatenated or
    NULL if sufficient memory was not available.
    
    The caller is responsible for calling free() on the returned string.
*/
char* GECO_astrcatm(const char *s1, ...);

/*!
  @function GECO_apathcatm
  @discussion
    Concatenate multiple strings, using malloc() to allocate memory
    for the product.  The function ensures that a '/' character is
    present between each string argument in concatenated form.
    
    The function accepts one or more C string pointers and a final
    NULL argument as a terminator.
  @result
    A C string containing the sequence of strings concatenated or
    NULL if sufficient memory was not available.
    
    The caller is responsible for calling free() on the returned string.
*/
char* GECO_apathcatm(const char *s1, ...);

/*!
  @function GECO_strtod
  @discussion
    Attempt to parse the leading (non-whitespace) part of str as
    a double-precision floating-point value.
    
    If a value can be parsed, then that value is assigned to *value
    and *endPtr receives a pointer to the first character in str
    which follows the parsed value.
  @result
    Boolean true is returned if a value was parsed, false otherwise.
*/
bool GECO_strtod(const char *str, double *value, const char **endPtr);

/*!
  @function GECO_strtol
  @discussion
    Attempt to parse the leading (non-whitespace) part of str as
    an integer value of C type "long int."
    
    If a value can be parsed, then that value is assigned to *value
    and *endPtr receives a pointer to the first character in str
    which follows the parsed value.
  @result
    Boolean true is returned if a value was parsed, false otherwise.
*/
bool GECO_strtol(const char *str, long *value, const char **endPtr);

/*!
  @function GECO_strtoi
  @discussion
    Attempt to parse the leading (non-whitespace) part of str as
    an integer value of C type "int."
    
    If a value can be parsed, then that value is assigned to *value
    and *endPtr receives a pointer to the first character in str
    which follows the parsed value.
  @result
    Boolean true is returned if a value was parsed, false otherwise.
*/
bool GECO_strtoi(const char *str, int *value, const char **endPtr);

/*!
  @function GECO_strtoull
  @discussion
    Attempt to parse the leading (non-whitespace) part of str as
    an integer value of C type "unsigned long long int."
    
    If a value can be parsed, then that value is assigned to *value
    and *endPtr receives a pointer to the first character in str
    which follows the parsed value.
  @result
    Boolean true is returned if a value was parsed, false otherwise.
*/
bool GECO_strtoull(const char *str, unsigned long long int *value, const char **endPtr);

/*!
  @function GECOChomp
  @discussion
    Removes leading and trailing whitespace from the C string in
    s.  The function is not aware of newlines internal to the
    string, only whitespace directly preceding the terminating
    NUL character.
  @result
    Returns s.
*/
char* GECOChomp(char *s);

/*!
  @function GECOIsDirectory
  @result
    Returns boolean true if path is a valid filesystem path and
    the entity at that path is a directory.
*/
bool GECOIsDirectory(const char *path);

/*!
  @function GECOIsFile
  @result
    Returns boolean true if path is a valid filesystem path and
    the entity at that path is a regular file.
*/
bool GECOIsFile(const char *path);

/*!
  @function GECOIsSocketFile
  @result
    Returns boolean true if path is a valid filesystem path and
    the entity at that path is a socket file.
*/
bool GECOIsSocketFile(const char *path);

/*!
  @function GECOIsFileEmpty
  @discussion
    If path is a valid filesystem path associated with a regular
    file, the file is opened and analyzed:  if all bytes present
    are whitespace, the file is deemed empty.
  @result
    Returns boolean true if an empty file exists at path.
*/
bool GECOIsFileEmpty(const char *path, bool *isEmpty);

/*!
  @function GECOGetFileContents
  @discussion
    If filepath is a valid filesystem path associated with a
    regular file, all bytes present in the file are read
    into a newly-allocated buffer.  If asCString is true then
    the buffer is NUL-terminated, as well.
  @result
    If successful, a memory buffer holding the content of
    filepath.
    
    The caller is responsible for calling free() on the returned
    pointer.
*/
char* GECOGetFileContents(const char *filepath, bool asCString);

/*!
  @function GECOSleepForMicroseconds
  @discussion
    Sleep for the number of microseconds indicated by timeout.
*/
void GECOSleepForMicroseconds(uint64_t timeout);

/*!
  @function GECOGetHostname
  @discussion
    Returns the (cached) shortname of the host, retrieved via uname().
*/
const char* GECOGetHostname(void);

/*!
  @function GECOGetPPidOfPid
  @discussion
    Sets thePPid to the ppid found in /proc/<aPid>/stat.
  @result
    Returns boolean true if the ppid was found.
*/
bool GECOGetPPidOfPid(pid_t aPid, pid_t *thePPid);

/*!
  @function GECOPidIsChildOfGEShepherd
  @discussion
    Checks the ppid chain for aPid to determine if the process is
    the child of an sgeshepherd process.
  @result
    Returns boolean true if a shepherd was found in aPids chain
    of parentage.
*/
bool GECOPidIsChildOfSGEShepherd(pid_t aPid, pid_t *theShepherdPid);

/*!
  @function GECOGetPidInfo
  @discussion
    Retrieves from /proc/<aPid>/stat the ppid, uid, and gid of the
    given process.
  @result
    Returns boolean true if the values were found.
*/
bool GECOGetPidInfo(pid_t aPid, pid_t *thePPid, uid_t *theUid, gid_t *theGid, long long int *startTimeInJiffies);

/*!
  @typedef GECOPidTree
  @discussion
    A node from a tree that represents the Linux process tree.
  @field pid
    The process id
  @field ppid
    The process' parent process id
  @field parent
    Pointer to the process' parent process node in the tree
  @field sibling
    Pointer to additional children of the node's parent process
  @field child
    Pointer to the first child process of this process node
*/
typedef struct __GECOPidTree {
  pid_t                   pid, ppid;
  char                    *cmd;
  struct __GECOPidTree    *parent, *sibling, *child;
} GECOPidTree;

/*!
  @function GECOPidTreeCreate
  @discussion
    Creates a new process tree from the /proc filesystem.  The tree is essentially
    a point-in-time snapshot of the real process tree (some processes may start/end
    while the tree is being created).
*/
GECOPidTree* GECOPidTreeCreate(bool shouldIncludeCmd);

/*!
  @function GECOPidTreeGetNodeWithPid
  @discussion
    Attempts to find the node within theTree which has the given pid.
  @result
    Returns NULL if no such node is found.
*/
GECOPidTree* GECOPidTreeGetNodeWithPid(GECOPidTree *theTree, pid_t pid);

/*!
  @function GECOPidTreeGetNodeWithPPid
  @discussion
    Attempts to find the first node within theTree which has the given parent
    pid.
  @result
    Returns NULL if no such node is found.
*/
GECOPidTree* GECOPidTreeGetNodeWithPPid(GECOPidTree *theTree, pid_t ppid);

/*!
  @function GECOPidTreePrint
  @discussion
    Writes to stdout a hierarchical depiction of the process tree rooted at
    theTree.
  @param showChildren
    For theTree node itself, display all child processes
  @param showSiblings
    For theTree node itself, display all sibling processes
*/
void GECOPidTreePrint(GECOPidTree *theTree, bool showChildren, bool showSiblings);

/*!
  @function GECOPidTreeDestroy
  @discussion
    Destroy theTree.  Please note:  ONLY the root node returned by GECOPidTreeCreate()
    should be passed to this function.
*/
void GECOPidTreeDestroy(GECOPidTree* theTree);

/*!
  @typedef GECODirectoryEnumeratorCallback
  @discussion
    Type of a callback function used to enumerate the items inside
    a directory.
    
    The function should return boolean false to halt enumeration.
*/
typedef bool (*GECODirectoryEnumeratorCallback)(const char *path, const void *context);

/*!
  @function GECOEnumerateDirectory
  @discussion
    Open the given directory and pass all non-trivial paths within it (e.g. not "." or "..")
    to the enumeratorCallback function.
    
    Additional information can be passed to the callback function via the context pointer.
  @result
    Returns boolean true if the directory was readable and all paths were processed by the
    callback without issue.
*/
bool GECOEnumerateDirectory(const char *directory, GECODirectoryEnumeratorCallback enumeratorCallback, const void *context);

/*!
  @typedef GECOFlags
  @discussion
    Project-wide type used for flag bitmasks.
*/
typedef uint32_t GECOFlags;

/*!
  @defined GECOFLAGS_ISSET
  @result
    Returns boolean true if the bitmask GECO_M is set in GECO_F.
*/
#define GECOFLAGS_ISSET(GECO_F, GECO_M) ((((GECO_F) & (GECO_M)) == (GECO_M)) ? true : false)

/*!
  @defined GECOFLAGS_SET
  @discussion
    All bits set in bitmask GECO_M are set in GECO_F.
*/
#define GECOFLAGS_SET(GECO_F, GECO_M)   ((GECO_F) |= (GECO_M))

/*!
  @defined GECOFLAGS_SET
  @discussion
    All bits set in bitmask GECO_M are cleared in GECO_F.
*/
#define GECOFLAGS_UNSET(GECO_F, GECO_M)   ((GECO_F) &= ~(GECO_M))

/*!
  @defined GECOFLAGS_SET
  @discussion
    All bits set in either GECO_M or GECO_F (but not both) are
    set in GECO_F.
*/
#define GECOFLAGS_TOGGLE(GECO_F, GECO_M)   ((GECO_F) ^= (GECO_M))

#endif /* __GECO_H__ */

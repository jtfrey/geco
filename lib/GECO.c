/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECO.c
 *
 *  Project-wide includes, typedefs, etc.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECO.h"

#include <dirent.h>
#include <sys/utsname.h>
#include <glob.h>

//

#ifndef GECO_LIB_VERSION
#error GECO_LIB_VERSION must be defined
#endif
const char  *GECOLibraryVersion = GECO_LIB_VERSION;

//

#ifndef GECO_PREFIX
#error GECO_PREFIX must be defined for the build
#endif

#ifndef GECO_BINDIR
#define GECO_BINDIR   GECO_PREFIX"/bin"
#endif

#ifndef GECO_ETCDIR
#define GECO_ETCDIR   GECO_PREFIX"/etc"
#endif

#ifndef GECO_LIBDIR
#define GECO_LIBDIR   GECO_PREFIX"/lib64"
#endif

const char *GECODirectoryPrefix = GECO_PREFIX;
const char *GECODirectoryBin = GECO_BINDIR;
const char *GECODirectoryEtc = GECO_ETCDIR;
const char *GECODirectoryLib = GECO_LIBDIR;

//

#ifndef GECO_STATE_DIRECTORY
#define GECO_STATE_DIRECTORY    "/opt/geco"
#endif

static char       *GECODefaultStateDir = GECO_STATE_DIRECTORY;
static char       *GECOStateDir = NULL;

const char*
GECOGetStateDir(void)
{
  if ( ! GECOStateDir ) GECOSetStateDir(GECODefaultStateDir);
  return GECOStateDir ? GECOStateDir : GECODefaultStateDir;
}

bool
GECOSetStateDir(
  const char    *stateDir
)
{
  struct stat   fInfo;
  char          path[PATH_MAX];
  int           pathLen;
  
  if ( ! stateDir ) stateDir = GECODefaultStateDir;

  if ( stat(stateDir, &fInfo) != 0 ) {
    if ( mkdir(path, 0771) != 0 ) return false;
  } else if ( ! S_ISDIR(fInfo.st_mode) ) {
    errno = ENOTDIR;
    return false;
  }

  pathLen = snprintf(path, sizeof(path), "%s/resources", stateDir);
  if ( pathLen <= 0 || pathLen >= sizeof(path) ) {
    errno = EOVERFLOW;
    return false;
  }
  if ( stat(path, &fInfo) != 0 ) {
    if ( mkdir(path, 0770) != 0 ) return false;
  }
  
  pathLen = snprintf(path, sizeof(path), "%s/tracefiles", stateDir);
  if ( pathLen <= 0 || pathLen >= sizeof(path) ) {
    errno = EOVERFLOW;
    return false;
  }
  if ( stat(path, &fInfo) != 0 ) {
    if ( mkdir(path, 0771) != 0 ) return false;
  }  
  
  if ( GECOStateDir ) free((void*)GECOStateDir);
  if ( ! (GECOStateDir = strdup(stateDir)) ) {
    errno = ENOMEM;
    return false;
  }
  
  return true;
}

//

const long int GECOUnknownJobId = -1;
const long int GECOUnknownTaskId = -1;

//

//
#if 0
#pragma mark -
#endif
//

char*
GECO_astrcatm(
  const char  *s1,
  ...
)
{
  char        *outS = NULL;
  
  if ( s1 ) {
    va_list   vargs;
    char      *s, *p;
    size_t    totalBytes = strlen(s1);
    
    va_start(vargs, s1);
    while ( (s = va_arg(vargs, char*)) ) totalBytes += strlen(s);
    va_end(vargs);
    
    if ( (outS = malloc(totalBytes + 1)) ) {
      p = outS;
      p = stpcpy(p, s1);
      va_start(vargs, s1);
      while ( (s = va_arg(vargs, char*)) ) p = stpcpy(p, s);
      va_end(vargs); 
    }
  }
  return outS;
}

//

char*
GECO_apathcatm(
  const char  *s1,
  ...
)
{
  char        *outS = NULL;
  
  if ( s1 ) {
    va_list   vargs;
    char      *s, *p;
    size_t    totalBytes = strlen(s1);
    
    va_start(vargs, s1);
    while ( (s = va_arg(vargs, char*)) ) totalBytes += ((*s == '/') ? 0 : 1) + strlen(s);
    va_end(vargs);
    
    if ( (outS = malloc(totalBytes + 1)) ) {
      p = outS;
      p = stpcpy(p, s1);
      va_start(vargs, s1);
      while ( (s = va_arg(vargs, char*)) ) {
        if ( *s != '/' ) *p++ = '/';
        p = stpcpy(p, s);
      }
      va_end(vargs); 
    }
  }
  return outS;
}

//
#if 0
#pragma mark -
#endif
//

bool
GECO_strtod(
  const char    *str,
  double        *value,
  const char    **endPtr
)
{
  char          *localEndPtr = NULL;
  double        v = strtod((char*)str, &localEndPtr);
  
  if ( localEndPtr && (localEndPtr > (char*)str) ) {
    *value = v;
    if ( endPtr ) *endPtr = localEndPtr;
    return true;
  }
  return false;
}

//

bool
GECO_strtol(
  const char    *str,
  long          *value,
  const char    **endPtr
)
{
  char          *localEndPtr = NULL;
  long          v = strtol((char*)str, &localEndPtr, 10);
  
  if ( localEndPtr && (localEndPtr > (char*)str) ) {
    *value = v;
    if ( endPtr ) *endPtr = localEndPtr;
    return true;
  }
  return false;
}

//

bool
GECO_strtoi(
  const char    *str,
  int           *value,
  const char    **endPtr
)
{
  long          v;
  
  if ( GECO_strtol(str, &v, endPtr) ) {
    if ( v >= INT_MIN && v <= INT_MAX ) {
      *value = (int)v;
      return true;
    }
    errno = EOVERFLOW;
  }
  return false;
}

//

bool
GECO_strtoull(
  const char              *str,
  unsigned long long int  *value,
  const char              **endPtr
)
{
  char                    *localEndPtr = NULL;
  unsigned long long int  v = strtoull((char*)str, &localEndPtr, 10);
  
  if ( localEndPtr && (localEndPtr > (char*)str) ) {
    *value = v;
    if ( endPtr ) *endPtr = localEndPtr;
    return true;
  }
  return false;
}

//
#if 0
#pragma mark -
#endif
//

bool
GECOIsDirectory(
  const char    *path
)
{
  struct stat   pathInfo;
  
  return ( (stat(path, &pathInfo) == 0) && (S_ISDIR(pathInfo.st_mode)) ) ? true : false;
}

//

bool
GECOIsFile(
  const char    *path
)
{
  struct stat   pathInfo;
  
  return ( (stat(path, &pathInfo) == 0) && (S_ISREG(pathInfo.st_mode)) ) ? true : false;
}

//

bool
GECOIsSocketFile(
  const char    *path
)
{
  struct stat   pathInfo;
  
  return ( (stat(path, &pathInfo) == 0) && (S_ISSOCK(pathInfo.st_mode)) ) ? true : false;
}

//

bool
GECOIsFileEmpty(
  const char    *path,
  bool          *isEmpty
)
{
  int           fd = open(path, O_RDONLY);
  
  if ( fd >= 0 ) {
    char        buffer[64], *s;
    ssize_t     rc = 0;
    
    *isEmpty = true;
    while ( (rc = read(fd, buffer, sizeof(buffer))) >= 0 ) {
      // Just whitespace?
      s = buffer;
      while ( rc && isspace(*s) ) {
        rc--;
        s++;
      }
      if ( rc > 0 ) {
        *isEmpty = false;
        break;
      }
    }
    close(fd);
    return true;
  }
  return false;
}

//

char*
GECOChomp(
  char      *s
)
{
  char      *s0 = s;
  char      *s1;
  int       s0len;
  
  while ( isspace(*s0) ) s0++;
  s0len = strlen(s0);
  s1 = s0 + s0len;
  while ( (s1 > s0) && isspace(*(--s1)) );
  if ( s1 > s0 ) {
    if ( s0 > s ) memmove(s, s0, s1 - s0 + 1);
    s[s1 - s0 + 1] = '\0';
  } else {
    *s = '\0';
  }
  return s;
}

//

char*
GECOGetFileContents(
  const char  *filepath,
  bool        asCString
)
{
  struct stat   pathInfo;
  char          *buffer = NULL;
  
  if ( stat(filepath, &pathInfo) == 0 ) {
    if ( S_ISREG(pathInfo.st_mode) ) {
      int       fd = open(filepath, O_RDONLY);
      ssize_t   bytesRead;
      
      if ( fd ) {
        if ( pathInfo.st_size == 0 ) {
          char    tmpBuffer[64];
          size_t  byteLen = 0;
          
          while ( (bytesRead = read(fd, tmpBuffer, sizeof(tmpBuffer))) > 0 ) {
            char  *nextBuffer = realloc(buffer, byteLen + bytesRead + (asCString ? 1 : 0));
            
            if ( ! nextBuffer ) break;
            buffer = nextBuffer;
            memcpy(buffer + byteLen, tmpBuffer, bytesRead);
            byteLen += bytesRead;
            if ( asCString ) buffer[byteLen] = '\0';
          }
        } else {
          if ( asCString ) {
            if ( (buffer = malloc(pathInfo.st_size + 1)) ) {
              bytesRead = read(fd, buffer, pathInfo.st_size);
              buffer[bytesRead] = '\0';
            }
          } else {
            if ( (buffer = malloc(pathInfo.st_size)) ) read(fd, buffer, pathInfo.st_size);
          }
        }
        close(fd);
      }
    }
  }
  return buffer;
}

//

void
GECOSleepForMicroseconds(
  uint64_t    timeout
)
{
  if ( timeout > 0 ) {
    struct timespec   sleepTime;
    
    timeout *= 1000;
    if ( timeout >= 1000000000 ) {
      sleepTime.tv_sec = (timeout / 1000000000);
      timeout = (timeout % 1000000000);
    } else {
      sleepTime.tv_sec = 0;
    }
    sleepTime.tv_nsec = timeout;
    nanosleep(&sleepTime, NULL);
  }
}

//

const char*
GECOGetHostname(void)
{
  static char         *GECOHostnameString = NULL;
  
  if ( ! GECOHostnameString ) {
    struct utsname    unameData;
  
    if ( uname(&unameData) == 0 ) GECOHostnameString = strdup(unameData.nodename);
  }
  return GECOHostnameString;
}

//

bool
GECOGetPPidOfPid(
  pid_t     aPid,
  pid_t     *thePPid
)
{
  bool      rc = false;
  char      path[PATH_MAX];
  int       pathLen = snprintf(path, sizeof(path), "/proc/%ld/stat", (long int)aPid);
  
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    FILE      *fPtr = fopen(path, "r");
    int       n;
    
    if ( fPtr ) {
      long int  tmpLongInt;
      
      if ( (n = fscanf(fPtr, "%ld ", &tmpLongInt)) > 0 ) {
        char    c;
        int     level = 0;
        bool    firstFound = false;
        
        while ( (! firstFound || level > 0) && (fread(&c, sizeof(char), 1, fPtr) == 1) ) {
          switch ( c ) {
            case '(': {
              firstFound = true;
              level++;
              break;
            }
            case ')': {
              level--;
              break;
            }
          }
        }
        if ( firstFound && (level == 0) && (fscanf(fPtr, " %*c %ld", &tmpLongInt) > 0) ) {
          *thePPid = tmpLongInt;
          rc = true;
        }
      }
      fclose(fPtr);
    }
  }
  return rc;
}

//

bool
GECOPidIsChildOfSGEShepherd(
  pid_t     aPid,
  pid_t     *theShepherdPid
)
{
  bool      rc = false;
  char      path[PATH_MAX];
  int       pathLen;
  
  while ( 1 ) {
    pid_t   ppid;
    
    if ( ! GECOGetPPidOfPid(aPid, &ppid) ) break;
    
    // What's the ppid's command?
    pathLen = snprintf(path, sizeof(path), "/proc/%ld/exe", (long int)ppid);
    if ( pathLen > 0 && pathLen < sizeof(path) ) {
      char  exe[PATH_MAX];
      
      pathLen = readlink(path, exe, sizeof(exe));
      if ( pathLen > 0 && pathLen < sizeof(exe) ) {
        // readlink() doesn't nul-terminate:
        exe[pathLen] = '\0';
        
        // Shepherd?
        if ( strcmp("/sge_shepherd", exe + pathLen - strlen("/sge_shepherd")) == 0 ) {
          *theShepherdPid = ppid;
          rc = true;
          break;
        }
      } else {
        break;
      }
    } else {
      break;
    }
    aPid = ppid;
  }
  
  return rc;
}

//

bool
GECOGetPidInfo(
  pid_t           aPid,
  pid_t           *thePPid,
  uid_t           *theUid,
  gid_t           *theGid,
  long long int   *startTimeInJiffies
)
{
  bool            rc = false;
  char            path[PATH_MAX];
  int             pathLen = snprintf(path, sizeof(path), "/proc/%ld/stat", (long int)aPid);
  
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    struct stat   fInfo;
    
    if ( stat(path, &fInfo) == 0 ) {
      FILE        *fPtr = fopen(path, "r");
      int         n;
    
      if ( fPtr ) {
        long int          tmpLongInt;
        long long int     tmpLongLongInt;
        
        if ( (n = fscanf(fPtr, "%ld ", &tmpLongInt)) > 0 ) {
          char    c;
          int     level = 0;
          bool    firstFound = false;
          
          while ( (! firstFound || level > 0) && (fread(&c, sizeof(char), 1, fPtr) == 1) ) {
            switch ( c ) {
              case '(': {
                firstFound = true;
                level++;
                break;
              }
              case ')': {
                level--;
                break;
              }
            }
          }
          if ( firstFound && (level == 0) && (fscanf(fPtr, " %*c %ld %*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu %*lu %*lu %*ld %*ld %*ld %*ld %*ld %*ld %llu", &tmpLongInt, &tmpLongLongInt) > 0) ) {
            if ( thePPid ) *thePPid = tmpLongInt;
            if ( startTimeInJiffies ) *startTimeInJiffies =tmpLongLongInt;
            if ( theUid ) *theUid = fInfo.st_uid;
            if ( theGid ) *theGid = fInfo.st_gid;
            rc = true;
          }
        }
        fclose(fPtr);
      }
    }
  }
  return rc;
}

//
#if 0
#pragma mark -
#endif
//

static GECOPidTree *GECOPidTreePool = NULL;

//

GECOPidTree*
__GECOPidTreeAlloc(
  size_t          cmdLen
)
{
  GECOPidTree     *newNode = NULL;
  
  if ( GECOPidTreePool ) {
    newNode = GECOPidTreePool;
    GECOPidTreePool = newNode->sibling;
  } else {
    newNode = malloc(sizeof(GECOPidTree));
  }
  if ( newNode ) {
    newNode->pid = newNode->ppid = -1;
    if ( cmdLen > 0 ) {
      if ( ! (newNode->cmd = malloc(cmdLen + 1)) ) {
        free((void*)newNode);
        return NULL;
      }
    } else {
      newNode->cmd = NULL;
    }
    newNode->parent = newNode->sibling = newNode->child = NULL;
  }
  return newNode;
}

//

void
__GECOPidTreeDealloc(
  GECOPidTree   *aNode
)
{
  if ( aNode->cmd ) free((void*)aNode->cmd);
  aNode->sibling = GECOPidTreePool;
  GECOPidTreePool = aNode;
}

//

GECOPidTree*
GECOPidTreeGetNodeWithPid(
  GECOPidTree     *theTree,
  pid_t           pid
)
{
  GECOPidTree     *foundNode = NULL;
  
  if ( theTree->pid == pid ) return theTree;
  if ( theTree->sibling ) foundNode = GECOPidTreeGetNodeWithPid(theTree->sibling, pid);
  if ( ! foundNode && theTree->child ) foundNode = GECOPidTreeGetNodeWithPid(theTree->child, pid);
  return foundNode;
}

//

GECOPidTree*
GECOPidTreeGetNodeWithPPid(
  GECOPidTree     *theTree,
  pid_t           ppid
)
{
  GECOPidTree     *foundNode = NULL;
  
  if ( theTree->ppid == ppid ) return theTree;
  if ( theTree->sibling ) foundNode = GECOPidTreeGetNodeWithPPid(theTree->sibling, ppid);
  if ( ! foundNode && theTree->child ) foundNode = GECOPidTreeGetNodeWithPPid(theTree->child, ppid);
  return foundNode;
}

//

GECOPidTree*
GECOPidTreeCreate(
  bool            shouldIncludeCmd
)
{
  GECOPidTree     *newTree = NULL;
  GECOPidTree     *nodes = NULL;
  glob_t          globData;
  
  glob("/proc/[0-9]*", GLOB_NOSORT, NULL, &globData);
  if ( globData.gl_pathc > 0 ) {
    unsigned int  globIdx = 0;
    bool          failed = false;
    
    while ( ! failed && (globIdx < globData.gl_pathc) ) {
      char      procPath[64];
      char      cmd[PATH_MAX];
      FILE      *fPtr;
      pid_t     pid, ppid;
      
      snprintf(procPath, sizeof(procPath), "%s/stat", globData.gl_pathv[globIdx++]);
      fPtr = fopen(procPath, "r");
      if ( ! fPtr ) continue;
      if ( fscanf(fPtr, "%d %s %*c %d", &pid, cmd, &ppid) >= 3 ) {
        GECOPidTree   *newNode = __GECOPidTreeAlloc(strlen(cmd));
        
        if ( ! newNode ) {
          fclose(fPtr);
          failed = true;
          break;
        } else {
          newNode->pid = pid;
          newNode->ppid = ppid;
          strcpy((char*)newNode->cmd, cmd);
          newNode->sibling = nodes;
          nodes = newNode;
        }
      }
      fclose(fPtr);
    }
  }
  globfree(&globData);
  
  if ( nodes ) {
    GECOPidTree       *foundNode = nodes, *prevNode = NULL;
    
    // We now have a flat array of tree nodes.  Create the tree:
    newTree = __GECOPidTreeAlloc(0);
    if ( newTree ) {
      newTree->pid = 0;
      while ( nodes ) {
        foundNode = nodes;
        prevNode = NULL;
        while ( foundNode ) {
          GECOPidTree   *parent = GECOPidTreeGetNodeWithPid(newTree, foundNode->ppid);
          
          if ( parent ) {
            GECOPidTree *next = foundNode->sibling;
            
            // Remove this node from the list:
            if ( prevNode ) {
              prevNode->sibling = next;
            } else {
              nodes = next;
            }
            // Set parent pointer:
            foundNode->parent = parent;
            foundNode->child = NULL;
            // Set as child of the parent:
            foundNode->sibling = ( parent->child ? parent->child : NULL );
            parent->child = foundNode;
            foundNode = next;
          } else {
            prevNode = foundNode;
            foundNode = foundNode->sibling;
          }
        }
      }
    } else {
      while ( nodes ) {
        GECOPidTree   *next = nodes->sibling;
        __GECOPidTreeDealloc(nodes);
        nodes = next;
      }
    }
  }
  
  return newTree;
}

//

void
__GECOPidTreePrint(
  GECOPidTree   *theTree,
  unsigned int  level,
  bool          showChildren,
  bool          showSiblings
)
{
  unsigned int  leadIn = level;
  
  while ( leadIn-- ) printf("   ");
  printf("|- %p %ld [%ld] %s (%p %p %p)\n", theTree, (long int)theTree->pid, (long int)theTree->ppid, ( theTree->cmd ? theTree->cmd : "<unknown>" ), theTree->parent, theTree->sibling, theTree->child);
  if ( showChildren && theTree->child ) __GECOPidTreePrint(theTree->child, level + 1, true, true);
  if ( showSiblings && theTree->sibling ) __GECOPidTreePrint(theTree->sibling, level, true, true);
}

void
GECOPidTreePrint(
  GECOPidTree   *theTree,
  bool          showChildren,
  bool          showSiblings
)
{
  if ( theTree ) __GECOPidTreePrint(theTree, 0, showChildren, showSiblings);
}

//

void
GECOPidTreeDestroy(
  GECOPidTree*  theTree
)
{
  if ( theTree->sibling ) GECOPidTreeDestroy(theTree->sibling);
  if ( theTree->child ) GECOPidTreeDestroy(theTree->child);
  __GECOPidTreeDealloc(theTree);
}

//
#if 0
#pragma mark -
#endif
//

bool
GECOEnumerateDirectory(
  const char                        *directory,
  GECODirectoryEnumeratorCallback   enumeratorCallback,
  const void                        *context
)
{
  char                              fullPath[PATH_MAX];
  int                               fullPathLen;
  struct dirent                     dirItem, *dirItemPtr;
  DIR                               *dirH = opendir(directory);
  bool                              rc = false;
  
  if ( dirH ) {
    rc = true;
    while ( rc && (readdir_r(dirH, &dirItem, &dirItemPtr) == 0) && dirItemPtr ) {
      if ( ! strcmp(dirItem.d_name, ".") || ! strcmp(dirItem.d_name, "..") ) continue;
      fullPathLen = snprintf(fullPath, sizeof(fullPath), "%s/%s", directory, dirItem.d_name);
      if( fullPathLen > 0 && fullPathLen < sizeof(fullPath) ) rc = enumeratorCallback(fullPath, context);
    }
    closedir(dirH);
  }
  return rc;
}

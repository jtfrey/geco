/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOCGroup.c
 *
 *  Interfaces/conveniences that work with the Linux cgroup
 *  facilities.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOCGroup.h"
#include "GECOLog.h"
#include "GECOJob.h"

#include <dirent.h>
#include <sys/types.h>
#include <signal.h>

//

typedef enum {
  GECOCGroupSubsystemMask_blkio       = 1 << GECOCGroupSubsystem_blkio,
  GECOCGroupSubsystemMask_cpu         = 1 << GECOCGroupSubsystem_cpu,
  GECOCGroupSubsystemMask_cpuacct     = 1 << GECOCGroupSubsystem_cpuacct,
  GECOCGroupSubsystemMask_cpuset      = 1 << GECOCGroupSubsystem_cpuset,
  GECOCGroupSubsystemMask_devices     = 1 << GECOCGroupSubsystem_devices,
  GECOCGroupSubsystemMask_freezer     = 1 << GECOCGroupSubsystem_freezer,
  GECOCGroupSubsystemMask_memory      = 1 << GECOCGroupSubsystem_memory,
  GECOCGroupSubsystemMask_net_cls     = 1 << GECOCGroupSubsystem_net_cls,
  //
  GECOCGroupSubsystemMask_all         = GECOCGroupSubsystemMask_blkio |
                                        GECOCGroupSubsystemMask_cpu |
                                        GECOCGroupSubsystemMask_cpuacct |
                                        GECOCGroupSubsystemMask_cpuset |
                                        GECOCGroupSubsystemMask_devices |
                                        GECOCGroupSubsystemMask_freezer |
                                        GECOCGroupSubsystemMask_memory |
                                        GECOCGroupSubsystemMask_net_cls
} GECOCGroupSubsystemMask;

static bool GECOCGroupSubsystemInited = false;
static GECOCGroupSubsystemMask GECOCGroupManagedSubsystems = GECOCGroupSubsystemMask_cpuset | GECOCGroupSubsystemMask_memory;

//

#ifndef GECOCGROUP_PREFIX
#define GECOCGROUP_PREFIX   "/cgroup"
#endif
const char      *GECOCGroupDefaultPrefix = GECOCGROUP_PREFIX;
const char      *GECOCGroupPrefix = NULL;

const char*
GECOCGroupGetPrefix(void)
{
  if ( ! GECOCGroupPrefix ) GECOCGroupSetPrefix(GECOCGroupDefaultPrefix);
  return GECOCGroupPrefix;
}

bool
GECOCGroupSetPrefix(
  const char  *cgroupPrefix
)
{
  struct stat   fInfo;
  
  if ( GECOCGroupSubsystemInited ) {
    errno = EBUSY;
    return false;
  }
  
  if ( ! cgroupPrefix ) cgroupPrefix = GECOCGroupDefaultPrefix;
  
  // We at least need an absolute path...
  if ( cgroupPrefix && (*cgroupPrefix != '/') ) {
    errno = EINVAL;
    return false;
  }
  
  // Make sure the directory exists:
  if ( stat(cgroupPrefix, &fInfo) != 0 ) {
    if ( mkdir(cgroupPrefix, 0777) != 0 ) return false;
  } else if ( ! S_ISDIR(fInfo.st_mode) ) {
    errno = ENOTDIR;
    return false;
  }
  
  if ( GECOCGroupPrefix ) free((void*)GECOCGroupPrefix);
  if ( ! (GECOCGroupPrefix = strdup(cgroupPrefix)) ) {
    errno = ENOMEM;
    return false;
  }
  return true;
} 

//

#ifndef GECOCGROUP_SUBGROUP
#define GECOCGROUP_SUBGROUP "GECO"
#endif
const char      *GECOCGroupDefaultSubGroup = GECOCGROUP_SUBGROUP;
const char      *GECOCGroupSubGroup = NULL;

const char*
GECOCGroupGetSubGroup(void)
{
  return ( GECOCGroupSubGroup ? GECOCGroupSubGroup : GECOCGroupDefaultSubGroup );
}

bool
GECOCGroupSetSubGroup(
  const char  *subgroup
)
{
  if ( GECOCGroupSubsystemInited ) {
    errno = EBUSY;
    return false;
  }
  
  if ( subgroup ) {
    // Drop any leading '/' from the fragment:
    while ( *subgroup == '/' ) subgroup++;
    
    // Empty string?  Same as asking for default:
    if ( *subgroup == '\0' ) {
      subgroup = NULL;
    }
    // The fragment cannot be  "." and is not allowed to lead with "..":
    else if ( (*subgroup == '.') && ((*(subgroup + 1) == '\0') || ((*(subgroup + 1) == '.') && ((*(subgroup + 2) == '\0') || (*(subgroup + 2) == '/')))) ) {
      errno = EINVAL;
      return false;
    }
    else {
      // Finally, it must be a single directory component:
      const char  *s = subgroup;
      
      while ( *s ) {
        if ( *s == '/' ) {
          errno = EINVAL;
          return false;
        }
        s++;
      }
    }
  }
  
  if ( GECOCGroupSubGroup) free((void*)GECOCGroupSubGroup);
  GECOCGroupSubGroup = ( subgroup ? strdup(subgroup) : NULL );
  
  return true;
}

//

const char*     GECOCGroupSubsystemNames[] = {
                      "blkio",
                      "cpu",
                      "cpuacct",
                      "cpuset",
                      "devices",
                      "freezer",
                      "memory",
                      "net_cls"
                    };

//

const char*
GECOCGroupSubsystemToCString(
  GECOCGroupSubsystem   theSubsystem
)
{
  if ( theSubsystem >= GECOCGroupSubsystem_min && theSubsystem < GECOCGroupSubsystem_max ) return GECOCGroupSubsystemNames[theSubsystem];
  return NULL;
}

//

GECOCGroupSubsystem
GECOCGroupCStringToSubsystem(
  const char    *subsystemStr
)
{
  GECOCGroupSubsystem     subsysId= GECOCGroupSubsystem_min;
  
  while ( subsysId < GECOCGroupSubsystem_max ) {
    if ( strcasecmp(subsystemStr, GECOCGroupSubsystemNames[subsysId]) == 0 ) return subsysId;
    subsysId++;
  }
  return GECOCGroupSubsystem_invalid;
}

//

int
__GECOCGroupSnprintf(
  char                  *buffer,
  size_t                bufferSize,
  GECOCGroupSubsystem   subsystem,
  const char*           subgroup,
  long int              jobId,
  long int              taskId,
  const char*           leafName
)
{
  const char            *format = NULL;
  
  if ( subgroup ) {
    if ( jobId != GECOUnknownJobId ) {
      if ( taskId == GECOUnknownTaskId ) taskId = 1;
      if ( leafName ) {
        format = "%1$s/%2$s/%3$s/%4$ld.%5$ld/%6$s";
      } else {
        format = "%1$s/%2$s/%3$s/%4$ld.%5$ld";
      }
    } else if ( leafName ) {
      format = "%1$s/%2$s/%3$s/%6$s";
    } else {
      format = "%1$s/%2$s/%3$s";
    }
  } else if ( leafName ) {
    format = "%1$s/%2$s/%6$s";
  } else {
    format = "%1$s/%2$s";
  }
  
  return snprintf(buffer, bufferSize, format,
              GECOCGroupGetPrefix(),
              GECOCGroupSubsystemNames[subsystem],
              subgroup,
              jobId, taskId,
              leafName
            );
}

int
GECOCGroupSnprintf(
  char                  *buffer,
  size_t                bufferSize,
  GECOCGroupSubsystem   subsystem,
  long int              jobId,
  long int              taskId,
  const char*           leafName
)
{
  return __GECOCGroupSnprintf(buffer, bufferSize, subsystem, GECOCGroupGetSubGroup(), jobId, taskId, leafName);
}

//

bool
__GECOCGroupRead(
  const char            *path,
  void                  *buffer,
  size_t                *bufferLen
)
{
  bool                  rc = false;
  int                   fd = open(path, O_RDONLY);
  
  if ( fd >= 0 ) {
    ssize_t           actualLen;
    
    actualLen = read(fd, buffer, *bufferLen);
    if ( actualLen > 0 ) {
      *bufferLen = actualLen;
      rc = true;
    }
    close(fd);
  }
  return rc;
}

//

bool
__GECOCGroupReadCString(
  const char            *path,
  void                  *buffer,
  size_t                *bufferLen
)
{
  bool                  rc = false;
  int                   fd = open(path, O_RDONLY);
  
  if ( fd >= 0 ) {
    ssize_t           actualLen;
    
    actualLen = read(fd, buffer, *bufferLen);
    if ( actualLen > 0 ) {
      if ( actualLen < *bufferLen - 1 ) ((char*)buffer)[actualLen] = '\0';
      *bufferLen = actualLen;
      rc = true;
    }
    close(fd);
  }
  return rc;
}

//

bool
__GECOCGroupWrite(
  const char            *path,
  const void            *buffer,
  size_t                bufferLen
)
{
  bool                  rc = false;
  int                   fd = open(path, O_WRONLY);
  
  if ( fd >= 0 ) {
    ssize_t     actualLen;
    
    rc = true;
    while ( bufferLen ) {
      actualLen = write(fd, buffer, bufferLen);
      if ( actualLen > 0 ) {
        bufferLen -= actualLen;
        buffer += actualLen;
      } else {
        rc = false;
        break;
      }
    }
    close(fd);
  }
  return rc;
}

//

bool
__GECOCGroupCopyPids(
  const char          *fromPath,
  const char          *toPath
)
{
  int                 retryCount = 5, triedCount = 1;
  const char          *failureReason = "unknown";
  bool                rc, forceTryAgain;
  FILE                *s_fPtr = NULL;

retry:
  rc = true;
  forceTryAgain = false;
  s_fPtr = fopen(fromPath, "r");
  
  if ( s_fPtr ) {
    long int          readPid;
    char              readPidStr[24];
    int               d_fd;
    
    while ( rc && ! feof(s_fPtr) ) {
      switch ( fscanf(s_fPtr, "%ld", &readPid) ) {
      
        case EOF: {
          if ( ! feof( s_fPtr) && (errno != 0) ) {
            failureReason = "failed to fscanf() a pid from the source tasks file";
            rc = false;
          }
          break;
        }
        
        case 1: {
          ssize_t         readPidStrLen = snprintf(readPidStr, sizeof(readPidStr), "%ld", readPid);
        
          if ( readPidStrLen > 0 ) {
            d_fd = open(toPath, O_WRONLY);
            if ( d_fd >= 0 ) {
              ssize_t       actualLen = write(d_fd, readPidStr, readPidStrLen);
              
              if ( actualLen != readPidStrLen ) {
                //
                // If errno is ESRCH (no such process) then we can just ignore that...
                //
                if ( errno == ESRCH ) {
                  GECO_WARN("__GECOCGroupCopyPids: kernel claims PID %ld no longer exists, forcing a retry (errno = %d)", readPid, errno);
                  forceTryAgain = true;
                } else {
                  failureReason = "failed to write() a pid to destination tasks file";
                  rc = false;
                }
              }
              close(d_fd);
            } else {
              failureReason = "failed to open() destination tasks file";
              rc = false;
            }
          } else {
            rc = false;
          }
          break;
        }
        
      }
    }
    fclose(s_fPtr);
  } else {
    failureReason = "failed in fopen() source tasks file";
    rc = false;
  }
  
  if ( ! rc ) {
    if ( forceTryAgain ) {
      errno = 0;
      rc = false;
    }
    switch ( errno ) {
    
      case EPERM: {
        // we somehow don't have permission to do this...
        GECO_ERROR("__GECOCGroupCopyPids: failed due to lack of permissions (errno = %d)", errno);
        break;
      }
      
      case ENOENT: {
        // cgroup is gone, okay to exit:
        GECO_WARN("__GECOCGroupCopyPids: cgroup no longer exists (errno = %d)", errno);
        rc = true;
        break;
      }
      
      default: {
        if ( retryCount-- ) {
          if ( ! forceTryAgain ) {
            GECO_ERROR("__GECOCGroupCopyPids: failed copying PIDs (errno = %d), will retry (try %d)", errno, triedCount);
            GECO_ERROR("__GECOCGroupCopyPids: failure reason: %s", failureReason);
          }
          failureReason = "unknown";
          GECOSleepForMicroseconds(triedCount++ * 1000000);
          goto retry;
        } else {
          GECO_ERROR("__GECOCGroupCopyPids: retry limit exceeded");
        }
        break;
      }
    
    }
  }
  return rc;
}

//

bool
__GECOCGroupCopy(
  const char          *fromPath,
  const char          *toPath
)
{
  bool                rc;
  int                 s_fd, d_fd;
  char                buffer[1024];
  ssize_t             readLen, actualLen;
  
  if ( (s_fd = open(fromPath, O_RDONLY)) < 0 ) {
    GECO_ERROR("__GECOCGroupCopy: failed to open() source file `%s` (errno = %d)", fromPath, errno);
    return false;
  }
  if ( (d_fd = open(toPath, O_WRONLY)) < 0 ) {
    close(s_fd);
    GECO_ERROR("__GECOCGroupCopy: failed to open() destination file `%s` (errno = %d)", toPath, errno);
    return false;
  }
    
  rc = true;
  while ( rc && (readLen = read(s_fd, buffer, sizeof(buffer))) > 0 ) {
    while ( rc && readLen && ((actualLen = write(d_fd, buffer, readLen)) < readLen) ) {
      if ( actualLen < 0 ) {
        if ( errno != EINTR ) {
          GECO_ERROR("__GECOCGroupCopy: failed to write data to `%s` (errno = %d)", toPath, errno);
          rc = false;
          break;
        }
      } else {
        readLen -= actualLen;
      }
    }
  }
  if ( readLen > 0 ) rc = false;
  close(s_fd);
  close(d_fd);
  return rc;
}

//

bool
GECOCGroupReadLeaf(
  GECOCGroupSubsystem   theCGroupSubsystem,
  long int              jobId,
  long int              taskId,
  const char            *leafName,
  void                  *buffer,
  size_t                *bufferLen
)
{
  char                  canonPath[PATH_MAX];
  int                   canonPathLen = __GECOCGroupSnprintf(
                                          canonPath, sizeof(canonPath),
                                          theCGroupSubsystem,
                                          GECOCGroupGetSubGroup(),
                                          jobId,
                                          taskId,
                                          leafName
                                        );
  if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) return __GECOCGroupRead(canonPath, buffer, bufferLen);
  return false;
}

//

bool
GECOCGroupWriteLeaf(
  GECOCGroupSubsystem   theCGroupSubsystem,
  long int              jobId,
  long int              taskId,
  const char            *leafName,
  const void            *buffer,
  size_t                bufferLen
)
{
  char                  canonPath[PATH_MAX];
  int                   canonPathLen = __GECOCGroupSnprintf(
                                          canonPath, sizeof(canonPath),
                                          theCGroupSubsystem,
                                          GECOCGroupGetSubGroup(),
                                          jobId,
                                          taskId,
                                          leafName
                                        );
  if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) return __GECOCGroupWrite(canonPath, buffer, bufferLen);
  return false;
}

//

bool
GECOCGroupInitForJobIdentifier(
  long int                jobId,
  long int                taskId,
  GECOCGroupInitCallback  initCallback,
  const void              *initCallbackContext
)
{
  bool                      rc = true;
  
  if ( GECOCGroupManagedSubsystems ) {
    GECOCGroupSubsystem     subsystemId = GECOCGroupSubsystem_min;
    char                    path[PATH_MAX];
    int                     pathLen;
    
    while ( rc && (subsystemId < GECOCGroupSubsystem_max) ) {
      if ( (GECOCGroupManagedSubsystems & (1 << subsystemId)) ) {
        // Construct the path to the GECO sub-group:
        pathLen = GECOCGroupSnprintf(
                      path, sizeof(path),
                      subsystemId,
                      jobId,
                      taskId,
                      NULL
                    );
        if ( pathLen > 0 && pathLen < sizeof(path) ) {
          bool        didMkdir = false;
          
          // If it doesn't exist, create it:
          if ( ! GECOIsDirectory(path) ) {
            if ( mkdir(path, 0755) != 0 ) {
              GECO_ERROR("GECOCGroupInitForJobIdentifier: unable to create %s (errno = %d)", path, errno);
              rc = false;
              subsystemId++;
              continue;
            } else {
              GECO_INFO("created %s", path);
              didMkdir = true;
            }
          }
          
          // If all is okay and we have a callback, let it do its work now:
          if ( initCallback ) {
            if ( ! initCallback(jobId, taskId, initCallbackContext, subsystemId, path, didMkdir) ) {
              rc = false;
              continue;
            }
          }

#ifdef GECO_CGROUP_ALWAYS_NOTIFY_ON_RELEASE
          // Ask for notification when the last task exits:
          pathLen = GECOCGroupSnprintf(
                        path, sizeof(path),
                        subsystemId,
                        jobId,
                        taskId,
                        "notify_on_release"
                      );
          if ( pathLen > 0 && pathLen < sizeof(path) ) {
            if ( __GECOCGroupWrite(path, "0", 1) ) {
              GECO_INFO("set %s = 0", path);
            } else {
              GECO_EMERGENCY("GECOCGroupInitForJobIdentifier: failed to set %s = 0 (errno = %d)", path, errno);
            }
          } else {
            GECO_EMERGENCY("GECOCGroupInitForJobIdentifier: notify_on_release path exceeds buffer (%d >= %d)", pathLen, sizeof(path));
          }
#endif
        } else {
          GECO_ERROR("GECOCGroupInitForJobIdentifier: error in GECOCGroupSnprintf (%d >= %d)", pathLen, sizeof(path));
        }
      }
      subsystemId++;
    }
  }
  return rc;
}

//

bool
GECOCGroupDeinitForJobIdentifier(
  long int                  jobId,
  long int                  taskId,
  GECOCGroupDeinitCallback  deinitCallback,
  const void                *deinitCallbackContext
)
{
  bool                      rc = true;
  
  if ( GECOCGroupManagedSubsystems ) {
    GECOCGroupSubsystem     subsystemId = GECOCGroupSubsystem_min;
    char                    path[PATH_MAX];
    int                     pathLen;
    
    while ( subsystemId < GECOCGroupSubsystem_max ) {
      if ( (GECOCGroupManagedSubsystems & (1 << subsystemId)) ) {
        // First, attempt to move any processes still hanging around:
        bool                ok = GECOCGroupRemoveTasks(subsystemId, jobId, taskId);
      
        if ( ok ) {
          // Construct the path to the GECO sub-group:
          pathLen = GECOCGroupSnprintf(
                        path, sizeof(path),
                        subsystemId,
                        jobId,
                        taskId,
                        NULL
                      );
          if ( pathLen > 0 && pathLen < sizeof(path) ) {
            // If it exists, destroy it:
            if ( GECOIsDirectory(path) ) {
              // If we have a callback, let it do its work now:
              if ( deinitCallback ) {
                if ( ! deinitCallback(jobId, taskId, deinitCallbackContext, subsystemId, path) ) rc = false;
              }
              if ( rmdir(path) != 0 ) {
                GECO_ERROR("GECOCGroupDeinitForJobIdentifier: unable to remove %s (errno = %d)", path, errno);
                rc = false;
              } else {
                GECO_INFO("removed %s", path);
              }
            }
          } else {
            GECO_ERROR("GECOCGroupDeinitForJobIdentifier: error in GECOCGroupSnprintf (%d >= %d)", pathLen, sizeof(path));
          }
        } else {
          GECO_ERROR("GECOCGroupDeinitForJobIdentifier: failed to move orphaned processes");
        }
      }
      subsystemId++;
    }
  }
  return rc;
}

//

bool
GECOCGroupAddTask(
  GECOCGroupSubsystem   theCGroupSubsystem,
  long int              jobId,
  long int              taskId,
  pid_t                 aPid
)
{
  return GECOCGroupAddTaskAndChildren(theCGroupSubsystem, jobId, taskId, aPid, false);
}

//

bool
__GECOCGroupAddTaskWalkPidTree(
  GECOPidTree           *theTree,
  const char            *tasksFile
)
{
  char                  pidStr[32];
  bool                  rc = __GECOCGroupWrite(tasksFile, pidStr, snprintf(pidStr, sizeof(pidStr), "%ld", (long int)theTree->pid));

  GECO_INFO("task %ld %s to %s", (long int)theTree->pid, ( rc ? "added" : "not added" ), tasksFile);
  if ( rc && theTree->child ) __GECOCGroupAddTaskWalkPidTree(theTree->child, tasksFile);
  if ( rc && theTree->sibling ) __GECOCGroupAddTaskWalkPidTree(theTree->sibling, tasksFile);
  return rc;
}

bool
GECOCGroupAddTaskAndChildren(
  GECOCGroupSubsystem   theCGroupSubsystem,
  long int              jobId,
  long int              taskId,
  pid_t                 aPid,
  bool                  addChildPids
)
{
  char                  aPIDString[32];
  size_t                aPIDStringLen;
  bool                  rc = true;
  
  aPIDStringLen = snprintf(aPIDString, sizeof(aPIDString), "%d", (int)aPid);
  if ( aPIDStringLen > 0 ) {
    char                canonPath[PATH_MAX];
    int                 canonPathLen;
    
    if ( theCGroupSubsystem == GECOCGroupSubsystem_all ) {
      theCGroupSubsystem = GECOCGroupSubsystem_max;
      while ( theCGroupSubsystem-- > GECOCGroupSubsystem_min ) {
        if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
         canonPathLen = __GECOCGroupSnprintf(
                              canonPath, sizeof(canonPath),
                              theCGroupSubsystem,
                              GECOCGroupGetSubGroup(),
                              jobId,
                              taskId,
                              "tasks"
                            );
          if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) {
            bool    ok = __GECOCGroupWrite(canonPath, aPIDString, aPIDStringLen);
            if ( ok ) {
              GECO_INFO("task %ld added to %s", (long int)aPid, canonPath);
          
              // Add children?
              if ( addChildPids ) {
                GECOPidTree   *processTree = GECOPidTreeCreate(false);
                
                if ( processTree ) {
                  GECOPidTree *subTree = GECOPidTreeGetNodeWithPid(processTree, aPid);
                  
                  if ( subTree ) {
                    if ( subTree->child ) {
                      __GECOCGroupAddTaskWalkPidTree(subTree->child, canonPath);
                    }
                  } else {
                    GECO_ERROR("GECOCGroupAddTask: unable to find pid %ld in the process tree", (long int)aPid);
                    rc = false;
                  }
                  GECOPidTreeDestroy(processTree);
                } else {
                  GECO_ERROR("GECOCGroupAddTask: unable to create process tree for pid %ld child addition (errno = %d)", (long int)aPid, errno);
                  rc = false;
                }
              }
            } else {
              GECO_ERROR("GECOCGroupAddTask: unable to add pid %ld to %s (errno = %d)", (long int)aPid, canonPath, errno);
              rc = false;
            }
          } else {
            GECO_ERROR("GECOCGroupAddTask: max path length exceeded: [%d]", canonPathLen);
            errno = ENAMETOOLONG;
            rc = false;
          }
        }
      }
    } else if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
     canonPathLen = __GECOCGroupSnprintf(
                          canonPath, sizeof(canonPath),
                          theCGroupSubsystem,
                          GECOCGroupGetSubGroup(),
                          jobId,
                          taskId,
                          "tasks"
                        );
      if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) {
        rc = __GECOCGroupWrite(canonPath, aPIDString, aPIDStringLen);
        if ( rc ) {
          GECO_INFO("task %ld added to %s", (long int)aPid, canonPath);
          
          // Add children?
          if ( addChildPids ) {
            GECOPidTree   *processTree = GECOPidTreeCreate(false);
            
            if ( processTree ) {
              GECOPidTree *subTree = GECOPidTreeGetNodeWithPid(processTree, aPid);
              
              if ( subTree ) {
                if ( subTree->child ) {
                  __GECOCGroupAddTaskWalkPidTree(subTree->child, canonPath);
                }
              } else {
                GECO_ERROR("GECOCGroupAddTask: unable to find pid %ld in the process tree", (long int)aPid);
                rc = false;
              }
              GECOPidTreeDestroy(processTree);
            } else {
              GECO_ERROR("GECOCGroupAddTask: unable to create process tree for pid %ld child addition (errno = %d)", (long int)aPid, errno);
              rc = false;
            }
          }
        } else {
          GECO_ERROR("GECOCGroupAddTask: unable to add pid %ld to %s (errno = %d)", (long int)aPid, canonPath, errno);
        }
      } else {
        GECO_ERROR("GECOCGroupAddTask: max path length exceeded: [%d]", canonPathLen);
        errno = ENAMETOOLONG;
        rc = false;
      }
    }
  } else {
    rc = false;
  }
  return rc;
}

//

bool
GECOCGroupRemoveTasks(
  GECOCGroupSubsystem theCGroupSubsystem,
  long int            jobId,
  long int            taskId
)
{
  bool                rc = true;
  char                canonPath[PATH_MAX], rootPath[PATH_MAX];
  int                 canonPathLen, rootPathLen;
  
  if ( theCGroupSubsystem == GECOCGroupSubsystem_all ) {
    theCGroupSubsystem = GECOCGroupSubsystem_max;
    while ( theCGroupSubsystem-- > GECOCGroupSubsystem_min ) {
      if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
       canonPathLen = __GECOCGroupSnprintf(
                            canonPath, sizeof(canonPath),
                            theCGroupSubsystem,
                            GECOCGroupGetSubGroup(),
                            jobId,
                            taskId,
                            "tasks"
                          );
       rootPathLen = __GECOCGroupSnprintf(
                            rootPath, sizeof(rootPath),
                            theCGroupSubsystem,
                            NULL,
                            -1,
                            -1,
                            "tasks"
                          );
        if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) && rootPathLen > 0 && rootPathLen < sizeof(rootPath) ) {
          bool    ok = __GECOCGroupCopyPids(canonPath, rootPath);
          if ( ok ) {
            GECO_INFO("tasks in %s moved to %s", canonPath, rootPath);
          } else {
            GECO_ERROR("GECOCGroupRemoveTasks: unable to move tasks in %s to %s (errno = %d)", canonPath, rootPath, errno);
            rc = false;
          }
        } else {
          GECO_ERROR("GECOCGroupRemoveTasks: max path length exceeded: [%d] [%d]", canonPathLen, rootPathLen);
          errno = ENAMETOOLONG;
          rc = false;
        }
      }
    }
  } else if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
   canonPathLen = __GECOCGroupSnprintf(
                        canonPath, sizeof(canonPath),
                        theCGroupSubsystem,
                        GECOCGroupGetSubGroup(),
                        jobId,
                        taskId,
                        "tasks"
                      );
   rootPathLen = __GECOCGroupSnprintf(
                        rootPath, sizeof(rootPath),
                        theCGroupSubsystem,
                        NULL,
                        -1,
                        -1,
                        "tasks"
                      );
    if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) && rootPathLen > 0 && rootPathLen < sizeof(rootPath) ) {
      bool    ok = __GECOCGroupCopyPids(canonPath, rootPath);
      if ( ok ) {
        GECO_INFO("tasks in %s moved to %s", canonPath, rootPath);
      } else {
        GECO_ERROR("GECOCGroupRemoveTasks: unable to move tasks in %s to %s (errno = %d)", canonPath, rootPath, errno);
        rc = false;
      }
    } else {
      GECO_ERROR("GECOCGroupRemoveTasks: max path length exceeded: [%d] [%d]", canonPathLen, rootPathLen);
      errno = ENAMETOOLONG;
      rc = false;
    }
  }
  return rc;
}

//

bool
GECOCGroupSignalTasks(
  GECOCGroupSubsystem theCGroupSubsystem,
  long int            jobId,
  long int            taskId,
  int                 signum
)
{
  bool                rc = true;
  char                canonPath[PATH_MAX];
  int                 canonPathLen;
  
  if ( theCGroupSubsystem == GECOCGroupSubsystem_all ) {
    theCGroupSubsystem = GECOCGroupSubsystem_max;
    while ( theCGroupSubsystem-- > GECOCGroupSubsystem_min ) {
      if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
       canonPathLen = __GECOCGroupSnprintf(
                            canonPath, sizeof(canonPath),
                            theCGroupSubsystem,
                            GECOCGroupGetSubGroup(),
                            jobId,
                            taskId,
                            "tasks"
                          );
        if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) {
          FILE        *fPtr = fopen(canonPath, "r");
          
          if ( fPtr ) {
            long int  pid;
            
            while ( fscanf(fPtr, "%ld", &pid) >= 1 ) {
              if ( kill((pid_t)pid, signum) == 0 ) {
                GECO_INFO("  pid %ld from %s killed", pid, canonPath);
              } else {
                GECO_WARN("GECOCGroupSignalTasks: failed to kill pid %ld from %s (errno = %d)", pid, canonPath, errno);
                rc = false;
              }
            }
            fclose(fPtr);
          } else {
            GECO_ERROR("GECOCGroupSignalTasks: unable to open %s for reading (errno = %d)", canonPath, errno);
            rc = false;
          }
        }
      }
    }
  } else if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) ) {
   canonPathLen = __GECOCGroupSnprintf(
                        canonPath, sizeof(canonPath),
                        theCGroupSubsystem,
                        GECOCGroupGetSubGroup(),
                        jobId,
                        taskId,
                        "tasks"
                      );
    if ( canonPathLen > 0 && canonPathLen < sizeof(canonPath) ) {
      FILE        *fPtr = fopen(canonPath, "r");
          
      if ( fPtr ) {
        long int  pid;
        
        while ( fscanf(fPtr, "%ld", &pid) >= 1 ) {
          if ( kill((pid_t)pid, signum) == 0 ) {
            GECO_INFO("  pid %ld from %s killed", pid, canonPath);
          } else {
            GECO_WARN("GECOCGroupSignalTasks: failed to kill pid %ld from %s (errno = %d)", pid, canonPath, errno);
            rc = false;
          }
        }
        fclose(fPtr);
      } else {
        GECO_ERROR("GECOCGroupSignalTasks: unable to open %s for reading (errno = %d)", canonPath, errno);
        rc = false;
      }
    }
  }
  return rc;
}

//

bool
GECOCGroupGetMemoryLimit(
  long int      jobId,
  long int      taskId,
  size_t        *m_mem_free
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_memory,
                                      jobId,
                                      taskId,
                                      "memory.limit_in_bytes"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char                limitStr[32];
    size_t              limitStrLen = sizeof(limitStr);
    
    if ( __GECOCGroupReadCString(path, limitStr, &limitStrLen) && (limitStrLen > 0 && limitStrLen < sizeof(limitStr)) ) {
      unsigned long long int    limit;
      
      if ( GECO_strtoull(limitStr, &limit, NULL) ) {
        *m_mem_free = (size_t)limit;
        rc = true;
      }
    }
  }
  return rc;
}

//

bool
GECOCGroupSetMemoryLimit(
  long int      jobId,
  long int      taskId,
  size_t        m_mem_free
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_memory,
                                      jobId,
                                      taskId,
                                      "memory.limit_in_bytes"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char                limitStr[24];
    size_t              limitStrLen;
    
    limitStrLen = snprintf(limitStr, sizeof(limitStr), "%llu", (unsigned long long int)m_mem_free);
    if ( limitStrLen > 0 ) rc = __GECOCGroupWrite(path, limitStr, limitStrLen);
  }
  return rc;
}

//

bool
GECOCGroupGetVirtualMemoryLimit(
  long int      jobId,
  long int      taskId,
  size_t        *h_vmem
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_memory,
                                      jobId,
                                      taskId,
                                      "memory.memsw.limit_in_bytes"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char                limitStr[24];
    size_t              limitStrLen = sizeof(limitStr);
    
    if ( __GECOCGroupReadCString(path, limitStr, &limitStrLen) && (limitStrLen > 0 && limitStrLen < sizeof(limitStr)) ) {
      unsigned long long int    limit;
      
      if ( GECO_strtoull(limitStr, &limit, NULL) ) {
        *h_vmem = (size_t)limit;
        rc = true;
      }
    }
  }
  return rc;
}

//

bool
GECOCGroupSetVirtualMemoryLimit(
  long int      jobId,
  long int      taskId,
  size_t        h_vmem
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_memory,
                                      jobId,
                                      taskId,
                                      "memory.memsw.limit_in_bytes"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char                limitStr[24];
    size_t              limitStrLen;
    
    limitStrLen = snprintf(limitStr, sizeof(limitStr), "%llu", (unsigned long long int)h_vmem);
    if ( limitStrLen > 0 ) rc = __GECOCGroupWrite(path, limitStr, limitStrLen);
  }
  return rc;
}

//

bool
GECOCGroupGetIsUnderOOM(
  long int    jobId,
  long int    taskId,
  bool        *isUnderOOM
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_memory,
                                      jobId,
                                      taskId,
                                      "memory.oom_control"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    FILE        *fPtr = fopen(path, "r");
    
    if ( fPtr ) {
      int       kill = -1, oom = -1;
      
      if ( fscanf(fPtr, "oom_kill_disable %d\nunder_oom %d", &kill, &oom) >= 2 ) {
        *isUnderOOM = ( oom ? true : false );
        rc = true;
      }
      fclose(fPtr);
    }
  }
  return rc;
}

//

static bool GECOCGroupHasScannedGECOCGroups = false;

hwloc_bitmap_t
__GECOCGroupGetAllocatedCpuset(void)
{
  static hwloc_bitmap_t GECOAllocatedCpuset = NULL;
  if ( GECOAllocatedCpuset == NULL ) GECOAllocatedCpuset = hwloc_bitmap_alloc();
  return GECOAllocatedCpuset;
}

hwloc_bitmap_t
__GECOCGroupGetAvailableCpuset(void)
{
  static hwloc_bitmap_t GECOAvailableCpuset = NULL;
  if ( GECOAvailableCpuset == NULL ) {
    if ( (GECOAvailableCpuset = hwloc_bitmap_alloc()) ) hwloc_bitmap_fill(GECOAvailableCpuset);
  }
  return GECOAvailableCpuset;
}

//

bool
GECOCGroupScanActiveCpusetBindings(void)
{
  bool            rc = false;
  hwloc_bitmap_t  cpuMask = hwloc_bitmap_alloc();
  time_t          delay;
  char            path[PATH_MAX];
  int             pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_cpuset,
                                      GECOUnknownJobId,
                                      GECOUnknownTaskId,
                                      NULL
                                    );
  if ( ! cpuMask ) return false;
  
  GECO_INFO("Scanning for active CPU bindings:");
  
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    DIR               *gecoDir = opendir(path);
    hwloc_bitmap_t    availableCpuset = __GECOCGroupGetAvailableCpuset();
  
    hwloc_bitmap_zero(__GECOCGroupGetAllocatedCpuset());
    hwloc_bitmap_fill(availableCpuset);
    
    if ( gecoDir ) {
      struct dirent   item, *itemPtr;
      long int        jobId, taskId;
      char            cpuInfo[PATH_MAX];
      char            *bitmapStr;
      
      if ( GECOCGroupGetCpusetCpus(GECOUnknownJobId, GECOUnknownTaskId, &availableCpuset) ) {
        hwloc_bitmap_list_asprintf(&bitmapStr, availableCpuset);
        if ( bitmapStr ) {
          GECO_INFO("  Succeeded reading CPU allocation from %s/cpuset.cpus = %s", path, bitmapStr);
          free(bitmapStr);
        } else {
          GECO_INFO("  Succeeded reading CPU allocation from %s/cpuset.cpus", path);
        }
      } else {
        GECO_WARN("GECOCGroupScanActiveCpusetBindings: failed to read available cpuset.cpus from %s/cpuset.cpus", path);
      }
      
      GECO_INFO("  Scanning %s for extant per-job CPU allocations", path);
      while ( (readdir_r(gecoDir, &item, &itemPtr) == 0) && itemPtr ) {
        if ( sscanf(item.d_name, "%ld.%ld", &jobId, &taskId) == 2 ) {
          size_t    cpuInfoLen = sizeof(cpuInfo);
    
          GECO_INFO("    Found GECO subgroup for job %ld.%ld", jobId, taskId);
          if ( GECOResourceSetIsJobRunningOnHost(jobId, taskId, 5) ) {
            if ( cpuMask ) hwloc_bitmap_zero(cpuMask);
            if ( GECOCGroupGetCpusetCpus(jobId, taskId, &cpuMask) ) {
#ifndef GECO_DEBUG_DISABLE
              hwloc_bitmap_list_asprintf(&bitmapStr, cpuMask);
              if ( bitmapStr ) {
                GECO_INFO("      Job is using cpuset.cpus %s", bitmapStr);
                free(bitmapStr);
              }
#endif
              hwloc_bitmap_or(__GECOCGroupGetAllocatedCpuset(), __GECOCGroupGetAllocatedCpuset(), cpuMask);
              hwloc_bitmap_andnot(availableCpuset, availableCpuset, cpuMask);
            }
          } else {
            //
            // Attempt to scrub all cgroup sub-group for this job id:
            //
            if ( GECOCGroupDeinitForJobIdentifier(jobId, taskId, NULL, NULL) ) {
              GECO_INFO("      %ld.%ld does not appear to be valid on host, removing orphaned cgroups", jobId, taskId);
            } else {
              GECO_ERROR("      %ld.%ld does not appear to be valid on host, but unable to remove orphaned cgroups", jobId, taskId);
            }
          }
        }
      }
      closedir(gecoDir);
      GECOCGroupHasScannedGECOCGroups = rc = true;

#ifndef GECO_INFO_DISABLE
      hwloc_bitmap_list_asprintf(&bitmapStr, __GECOCGroupGetAllocatedCpuset());
      if ( bitmapStr ) {
        GECO_INFO("  In-use CPUs = %s", bitmapStr);
        free(bitmapStr);
      }
      hwloc_bitmap_list_asprintf(&bitmapStr, availableCpuset);
      if ( bitmapStr ) {
        GECO_INFO("  Available CPUs = %s", bitmapStr);
        free(bitmapStr);
      }
#endif
    }
  } else {
    GECO_ERROR("GECOCGroupScanActiveCpusetBindings: path limit exceeded (%d >= %d)", pathLen, sizeof(path));
  }
  hwloc_bitmap_free(cpuMask);
  return rc;
}

//

bool
GECOCGroupAllocateCores(
  unsigned int        nCores,
  hwloc_bitmap_t      *outCpuset
)
{
  bool                rc = false;
  hwloc_topology_t    topology = NULL;
  int                 maxCores;
  int                 rootCount;
  
#ifdef LIBGECO_PRE_V101
  if ( ! GECOCGroupHasScannedGECOCGroups ) GECOCGroupScanActiveCpusetBindings();
#else
  //
  // ALWAYS scan active bindings since cleanup doesn't seem to work too well...
  //
  GECOCGroupScanActiveCpusetBindings();
#endif

  // Allocate a new topology object:
  hwloc_topology_init(&topology);
  if ( ! topology ) return false;
  
  // Ignore everything except the NUMA/memory/cpu components:
  hwloc_topology_ignore_type(topology, HWLOC_OBJ_BRIDGE | HWLOC_OBJ_MISC | HWLOC_OBJ_GROUP);
  // Detect those components:
  hwloc_topology_load(topology);
  if ( ! hwloc_bitmap_iszero(__GECOCGroupGetAvailableCpuset()) ) {
    // If there aren't enough bits in the bitmask, we have a problem:
    if ( hwloc_bitmap_weight(__GECOCGroupGetAvailableCpuset()) >= nCores ) {
      // Discount any cpus we've already allocated:
      hwloc_topology_restrict(topology, __GECOCGroupGetAvailableCpuset(), HWLOC_RESTRICT_FLAG_ADAPT_DISTANCES);
    } else {
      GECO_WARN("GECOCGroupAllocateCores: available cpuset contains %d cores, %d requested\n", hwloc_bitmap_weight(__GECOCGroupGetAvailableCpuset()), nCores);
      rc = false;
      goto early_exit;
    }
  }
  
  // How many root objects were found?
  rootCount = hwloc_get_nbobjs_by_depth(topology, 0);
  GECO_DEBUG("hwloc found %u root objects", rootCount);

  GECO_DEBUG("hwloc found %u sockets", hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_SOCKET));
  
  // Count the number of cores:
  maxCores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
  GECO_DEBUG("hwloc found %u cores", maxCores);
  
  if ( nCores > maxCores ) {
    GECO_WARN("GECOCGroupAllocateCores: system contains %d cores, %d requested\n", maxCores, nCores);
  } else {
#ifndef GECO_INFO_DISABLE
    int               maxCpuDigits = ceil(log10(maxCores));
    size_t            cgroup_cpus_len = maxCores * (maxCpuDigits + 1);
    char              cgroup_cpus[cgroup_cpus_len];
#endif
    hwloc_bitmap_t    cpusets[nCores];
    hwloc_obj_t       roots[rootCount];
    int               i;
    
    // Initialize the roots:
    for (i = 0; i < rootCount; i++) roots[i] = hwloc_get_obj_by_depth(topology, 0, i);
    
    //
    // Create nCores cpuset bitmaps that spread across all available cores.  E.g. on 
    // a dual 10C box, nCores = 2 might yield:
    //
    //      0x00055555
    //      0x000aaaaa
    //
    hwloc_distrib(topology, roots, rootCount, cpusets, nCores, INT_MAX, 0);
    
    //
    // For each of the cpuset bitmaps, reduce the bitmap to a single selected
    // bit/core and collapse into a single bitmap:
    //
    hwloc_bitmap_singlify(cpusets[0]);
    for (i = 1; i < nCores; i++) {
      hwloc_bitmap_singlify(cpusets[i]);
      hwloc_bitmap_or(cpusets[0], cpusets[0], cpusets[i]);
      hwloc_bitmap_free(cpusets[i]);
    }
    
    //
    // Check to be sure the number of set bits == nCores:
    //
    if ( (i = hwloc_bitmap_weight(cpusets[0])) < nCores ) {
      GECO_ERROR("GECOCGroupAllocateCores: optimal selection of %d core%s yielded only %d core%s", nCores, ((nCores != 1) ? "s" : ""), i, ((i != 1) ? "s" : ""));
      hwloc_bitmap_free(cpusets[0]);
      rc = false;
    } else {
#ifndef GECO_INFO_DISABLE
      hwloc_bitmap_list_snprintf(cgroup_cpus, cgroup_cpus_len, cpusets[0]);
      GECO_INFO("optimal cgroup.cpus calculated as %s (%d)", cgroup_cpus, hwloc_bitmap_weight(cpusets[0]));
#endif
      if ( outCpuset ) {
        *outCpuset = cpusets[0];
        hwloc_bitmap_or(__GECOCGroupGetAllocatedCpuset(), __GECOCGroupGetAllocatedCpuset(), cpusets[0]);
        hwloc_bitmap_andnot(__GECOCGroupGetAvailableCpuset(), __GECOCGroupGetAvailableCpuset(), cpusets[0]);
      } else {
        hwloc_bitmap_free(cpusets[0]);
      }
      rc = true;
    }
  }
early_exit:
  hwloc_topology_destroy(topology);
  return rc;
}

//

void
GECOCGroupDeallocateCores(
  hwloc_bitmap_t      theCpuset
)
{
  if ( theCpuset ) {
#ifndef GECO_INFO_DISABLE
    char          *str;
    
    hwloc_bitmap_list_asprintf(&str, theCpuset);
    if ( str ) {
      GECO_INFO("deallocating cgroup.cpus %s", str);
      free(str);
    }
#endif
    hwloc_bitmap_andnot(__GECOCGroupGetAllocatedCpuset(), __GECOCGroupGetAllocatedCpuset(), theCpuset);
    hwloc_bitmap_or(__GECOCGroupGetAvailableCpuset(), __GECOCGroupGetAvailableCpuset(), theCpuset);
    hwloc_bitmap_free(theCpuset);
  }
}

//

bool
GECOCGroupGetCpusetCpus(
  long int          jobId,
  long int          taskId,
  hwloc_bitmap_t    *cpulist
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_cpuset,
                                      jobId,
                                      taskId,
                                      "cpuset.cpus"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char        cpulist_str[PATH_MAX];
    size_t      cpulist_str_len = sizeof(cpulist_str);
    
    if ( __GECOCGroupReadCString(path, cpulist_str, &cpulist_str_len) && (cpulist_str_len < sizeof(cpulist_str)) ) {
      // Drop any trailing newlines:
      GECOChomp(cpulist_str);
      GECO_INFO("%s = %s (errno = %d)", path, cpulist_str, errno);
      if ( ! *cpulist && ! (*cpulist = hwloc_bitmap_alloc()) ) return false;
      if ( hwloc_bitmap_list_sscanf(*cpulist, cpulist_str) == 0 ) rc = true;
    }
  }
  return rc;
}

//

bool
GECOCGroupSetCpusetCpus(
  long int          jobId,
  long int          taskId,
  hwloc_bitmap_t    cpulist
)
{
  bool          rc = false;
  char          path[PATH_MAX];
  int           pathLen = GECOCGroupSnprintf(
                                      path, sizeof(path),
                                      GECOCGroupSubsystem_cpuset,
                                      jobId,
                                      taskId,
                                      "cpuset.cpus"
                                    );
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    char        *cpulist_str = NULL;
    
    hwloc_bitmap_list_asprintf(&cpulist_str, cpulist);
    if ( cpulist_str ) {
      int       retryCount = 5, triedCount = 1;
      
retry_cpu_bind:
      rc = __GECOCGroupWrite(path, cpulist_str, strlen(cpulist_str));
      
      if ( ! rc ) {
        if ( triedCount < retryCount ) {
          GECO_WARN("GECOCGroupSetCpusetCpus: failed while writing CPU list '%s' to %s (errno = %d); retry %d", cpulist_str, path, errno, triedCount);
          //triedCount++;
          GECOSleepForMicroseconds(triedCount++ * 1000000);
          goto retry_cpu_bind;
        } else {
          GECO_ERROR("GECOCGroupSetCpusetCpus: failed while writing CPU list '%s' to %s (errno = %d)", cpulist_str, path, errno);
        }
      }
      
      free(cpulist_str);
      
      if ( rc ) {
        char    path2[PATH_MAX];
        int     path2Len;
        
        //
        // Be sure to set cpuset.mems, too:
        //
        pathLen = GECOCGroupSnprintf(
                        path, sizeof(path),
                        GECOCGroupSubsystem_cpuset,
                        GECOUnknownJobId,
                        GECOUnknownTaskId,
                        "cpuset.mems"
                      );
        path2Len = GECOCGroupSnprintf(
                        path2, sizeof(path2),
                        GECOCGroupSubsystem_cpuset,
                        jobId,
                        taskId,
                        "cpuset.mems"
                      );
        rc = false;
        if ( pathLen > 0 && pathLen < sizeof(path) && path2Len > 0 && path2Len < sizeof(path2) ) {
          if ( __GECOCGroupCopy(path, path2) ) {
            GECO_INFO("copied %s to %s", path, path2);
            rc = true;
          } else {
            GECO_ERROR("GECOCGroupSetCpusetCpus: failed to copy %s to %s (errno = %d)", path, path2, errno);
          }
        } else {
          GECO_ERROR("GECOCGroupSetCpusetCpus: error in GECOCGroupSnprintf (%d %d)", pathLen, path2Len);
        }
        
        if ( rc ) {
          //
          // Set the CPU exclusive flag on the GECO subgroup:
          //
          pathLen = GECOCGroupSnprintf(
                          path, sizeof(path),
                          GECOCGroupSubsystem_cpuset,
                          jobId,
                          taskId,
                          "cpuset.cpu_exclusive"
                        );
          rc = false;
          if ( pathLen > 0 && pathLen < sizeof(path) ) {
            char        *enableStr = "1";
            
            if ( __GECOCGroupWrite(path, enableStr, sizeof(enableStr)) ) {
              GECO_INFO("set %s to %s", path, enableStr);
              rc = true;
            } else {
              GECO_ERROR("GECOCGroupSetCpusetCpus: failed to set %s to %s (errno = %d)", path, enableStr, errno);
            }
          } else {
            GECO_ERROR("GECOCGroupSetCpusetCpus: error in GECOCGroupSnprintf (%d)", pathLen);
          }
        }
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

bool
GECOCGroupGetSubsystemIsManaged(
  GECOCGroupSubsystem   theCGroupSubsystem
)
{
  if ( theCGroupSubsystem == GECOCGroupSubsystem_all ) {
    return (GECOCGroupManagedSubsystems == GECOCGroupSubsystemMask_all) ? true : false;
  }
  if ( (theCGroupSubsystem >= GECOCGroupSubsystem_min) && (theCGroupSubsystem < GECOCGroupSubsystem_max) ) {
    if ( (GECOCGroupManagedSubsystems & (1 << theCGroupSubsystem)) != 0 ) return true;
  }
  return false;
}

//

void
GECOCGroupSetSubsystemIsManaged(
  GECOCGroupSubsystem   theCGroupSubsystem,
  bool                  isManaged
)
{
  if ( GECOCGroupSubsystemInited ) return;
  
  if ( theCGroupSubsystem == GECOCGroupSubsystem_all ) {
    GECOCGroupManagedSubsystems = GECOCGroupSubsystemMask_all;
  }
  else if ( (theCGroupSubsystem >= GECOCGroupSubsystem_min) && (theCGroupSubsystem < GECOCGroupSubsystem_max) ) {
    if ( isManaged ) {
      GECOCGroupManagedSubsystems |= (1 << theCGroupSubsystem);
    } else {
      GECOCGroupManagedSubsystems &= ~(1 << theCGroupSubsystem);
    }
  }
}

//

bool
GECOCGroupInitSubsystems(void)
{
  bool                      rc = true;
  
  if ( GECOCGroupManagedSubsystems ) {
    GECOCGroupSubsystem     subsystemId = GECOCGroupSubsystem_min;
    struct stat             fInfo;
    char                    path[PATH_MAX];
    int                     pathLen;
    
    while ( rc && (subsystemId < GECOCGroupSubsystem_max) ) {
      if ( (GECOCGroupManagedSubsystems & (1 << subsystemId)) ) {
        // Ensure the subsystem is mounted:
        pathLen = __GECOCGroupSnprintf(
                      path, sizeof(path),
                      subsystemId,
                      NULL,
                      GECOUnknownJobId,
                      GECOUnknownTaskId,
                      NULL
                    );
        if ( pathLen > 0 && pathLen < sizeof(path) ) {
          if ( stat(path, &fInfo) != 0 ) {
            GECO_ERROR("GECOCGroupInitSubsystems: subsystem not mounted: %s", path);
            subsystemId++;
            rc = false;
            continue;
          }
          if ( ! S_ISDIR(fInfo.st_mode) ) {
            GECO_ERROR("GECOCGroupInitSubsystems: mount point is not a directory: %s", path);
            subsystemId++;
            rc = false;
            continue;
          }
        } else {
          GECO_ERROR("GECOCGroupInitSubsystems: error in __GECOCGroupSnprintf (%d)", pathLen);
          subsystemId++;
          rc = false;
          continue;
        }
        
        // Construct the path to the GECO sub-group:
        pathLen = GECOCGroupSnprintf(
                      path, sizeof(path),
                      subsystemId,
                      GECOUnknownJobId,
                      GECOUnknownTaskId,
                      NULL
                    );
        if ( pathLen > 0 && pathLen < sizeof(path) ) {
          bool        didMkdir = false;
          
          // If it doesn't exist, create it:
          if ( ! GECOIsDirectory(path) ) {
            if ( mkdir(path, 0755) != 0 ) {
              GECO_ERROR("GECOCGroupInitSubsystems: unable to create %s (errno = %d)", path, errno);
              rc = false;
              subsystemId++;
              continue;
            } else {
              GECO_INFO("created %s", path);
              didMkdir = true;
            }
          }
          
#ifdef GECO_CGROUP_USE_RELEASE_AGENTS
          // Setup the release agent:
          pathLen = __GECOCGroupSnprintf(
                        path, sizeof(path),
                        subsystemId,
                        NULL,
                        GECOUnknownJobId,
                        GECOUnknownTaskId,
                        "release_agent"
                      );
          char    *releaseAgent = GECO_apathcatm(GECODirectoryBin, GECOCGroupSubsystemNames[subsystemId], NULL);
          if ( releaseAgent ) {
            if ( __GECOCGroupWrite(path, releaseAgent, strlen(releaseAgent)) ) {
              GECO_INFO("set %s = %s", path, releaseAgent);
            } else {
              GECO_EMERGENCY("GECOCGroupInitSubsystems: failed while setting %s = %s (errno = %d)", path, releaseAgent, errno);
            }
            free(releaseAgent);
          } else {
            GECO_EMERGENCY("GECOCGroupInitSubsystems: unable to allocate release agent path");
          }
#endif

          // Any special processing?
          switch ( subsystemId ) {
          
            case GECOCGroupSubsystem_cpuset: {
              //
              // We only muck around with cpuset.mems and cpuset.cpus if we had to create the directory
              // ourself.  Otherwise, assume that whoever did so knew what s/he was doing.
              //
              if ( didMkdir ) {
                char          path2[PATH_MAX];
                int           path2Len;
                
                //
                // Copy the parent's cpuset.mems into the GECO subgroup:
                //
                pathLen = __GECOCGroupSnprintf(
                                path, sizeof(path),
                                GECOCGroupSubsystem_cpuset,
                                NULL,
                                GECOUnknownJobId,
                                GECOUnknownTaskId,
                                "cpuset.mems"
                              );
                path2Len = GECOCGroupSnprintf(
                                path2, sizeof(path2),
                                GECOCGroupSubsystem_cpuset,
                                GECOUnknownJobId,
                                GECOUnknownTaskId,
                                "cpuset.mems"
                              );
                if ( pathLen > 0 && pathLen < sizeof(path) && path2Len > 0 && path2Len < sizeof(path2) ) {
                  if ( __GECOCGroupCopy(path, path2) ) {
                    GECO_INFO("copied %s to %s", path, path2);
                  } else {
                    GECO_ERROR("GECOCGroupInitSubsystems: failed to copy %s to %s (errno = %d)", path, path2, errno);
                    rc = false;
                  }
                } else {
                  GECO_ERROR("GECOCGroupInitSubsystems: error in GECOCGroupSnprintf (%d %d)", pathLen, path2Len);
                  rc = false;
                }
                
                if ( rc ) {
                  //
                  // Copy the parent's cpuset.cpus into the GECO subgroup:
                  //
                  pathLen = __GECOCGroupSnprintf(
                                  path, sizeof(path),
                                  GECOCGroupSubsystem_cpuset,
                                  NULL,
                                  GECOUnknownJobId,
                                  GECOUnknownTaskId,
                                  "cpuset.cpus"
                                );
                  path2Len = GECOCGroupSnprintf(
                                  path2, sizeof(path2),
                                  GECOCGroupSubsystem_cpuset,
                                  GECOUnknownJobId,
                                  GECOUnknownTaskId,
                                  "cpuset.cpus"
                                );
                  if ( pathLen > 0 && pathLen < sizeof(path) && path2Len > 0 && path2Len < sizeof(path2) ) {
                    if ( __GECOCGroupCopy(path, path2) ) {
                      GECO_INFO("copied %s to %s", path, path2);
                    } else {
                      GECO_ERROR("GECOCGroupInitSubsystems: failed to copy %s to %s (errno = %d)", path, path2, errno);
                      rc = false;
                    }
                  } else {
                    GECO_ERROR("GECOCGroupInitSubsystems: error in GECOCGroupSnprintf (%d %d)", pathLen, path2Len);
                    rc = false;
                  }
                }
              }
              
              if ( rc ) {
                //
                // Set the CPU exclusive flag on the GECO subgroup:
                //
                pathLen = GECOCGroupSnprintf(
                                path, sizeof(path),
                                GECOCGroupSubsystem_cpuset,
                                GECOUnknownJobId,
                                GECOUnknownTaskId,
                                "cpuset.cpu_exclusive"
                              );
                if ( pathLen > 0 && pathLen < sizeof(path) ) {
                  char        *enableStr = "1";
                  
                  if ( __GECOCGroupWrite(path, enableStr, sizeof(enableStr)) ) {
                    GECO_INFO("set %s to %s", path, enableStr);
                  } else {
                    GECO_ERROR("GECOCGroupInitSubsystems: failed to set %s to %s (errno = %d)", path, enableStr, errno);
                    rc = false;
                  }
                } else {
                  GECO_ERROR("GECOCGroupInitSubsystems: error in GECOCGroupSnprintf (%d)", pathLen);
                  rc = false;
                }
              }
              break;
            }
          
          }
        } else {
          GECO_ERROR("GECOCGroupInitSubsystems: error in GECOCGroupSnprintf (%d)", pathLen);
        }
      }
      subsystemId++;
    }
    GECOCGroupSubsystemInited = true;
    if ( ! rc ) {
      GECOCGroupShutdownSubsystems();
    }
  }
  return rc;
}

//

bool
GECOCGroupShutdownSubsystems(void)
{
  bool                      rc = true;
  
  if ( GECOCGroupSubsystemInited ) {
    GECOCGroupSubsystem     subsystemId = GECOCGroupSubsystem_min;
    char                    path[PATH_MAX];
    int                     pathLen;
    
    while ( rc && (subsystemId < GECOCGroupSubsystem_max) ) {
      if ( (GECOCGroupManagedSubsystems & (1 << subsystemId)) ) {
        // Construct the path to the GECO sub-group:
        pathLen = GECOCGroupSnprintf(
                      path, sizeof(path),
                      subsystemId,
                      GECOUnknownJobId,
                      GECOUnknownTaskId,
                      NULL
                    );
        if ( pathLen > 0 && pathLen < sizeof(path) ) {
          // If it exists, remove it:
          if ( GECOIsDirectory(path) ) {
            if ( rmdir(path) != 0 ) {
              GECO_ERROR("GECOCGroupShutdownSubsystems: unable to remove %s (errno = %d)", path, errno);
              rc = false;
              subsystemId++;
              continue;
            } else {
              GECO_INFO("removed %s", path);
            }
          }
          
#ifdef GECO_CGROUP_USE_RELEASE_AGENTS
          // Drop the release agent:
          pathLen = __GECOCGroupSnprintf(
                        path, sizeof(path),
                        subsystemId,
                        NULL,
                        GECOUnknownJobId,
                        GECOUnknownTaskId,
                        "release_agent"
                      );
          if ( __GECOCGroupWrite(path, "", 1) ) {
            GECO_INFO("set %s = NULL", path);
          } else {
            GECO_ERROR("GECOCGroupInitSubsystems: failed while setting %s = 0 (errno = %d)", path, errno);
          }
#endif
        } else {
          GECO_ERROR("GECOCGroupShutdownSubsystems: error in GECOCGroupSnprintf (%d)", pathLen);
        }
      }
      subsystemId++;
    }
    GECOCGroupSubsystemInited = false;
  }
}

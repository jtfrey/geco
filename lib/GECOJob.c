/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOJob.c
 *
 *  Wrapper for a Grid Engine job and state associated with it.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOJob.h"
#include "GECOCGroup.h"

#include <sys/eventfd.h>
#include <signal.h>

//

#define GECO_TRACE_DEBUG(GECO_JOB, GECO_FORMAT, ...) { GECO_DEBUG(GECO_FORMAT, ##__VA_ARGS__); if ( (GECO_JOB)->traceFile ) GECOLogPrintf((GECO_JOB)->traceFile, GECOLogLevelDebug, "(%s:%d) "GECO_FORMAT, __FILE__, __LINE__, ##__VA_ARGS__); }
#define GECO_TRACE_INFO(GECO_JOB, GECO_FORMAT, ...) { GECO_INFO(GECO_FORMAT, ##__VA_ARGS__); if ( (GECO_JOB)->traceFile ) GECOLogPrintf((GECO_JOB)->traceFile, GECOLogLevelInfo, "(%s:%d) "GECO_FORMAT, __FILE__, __LINE__, ##__VA_ARGS__); }
#define GECO_TRACE_WARN(GECO_JOB, ...) { GECO_WARN(__VA_ARGS__); if ( (GECO_JOB)->traceFile ) GECOLogPrintf((GECO_JOB)->traceFile, GECOLogLevelWarn, __VA_ARGS__); }
#define GECO_TRACE_ERROR(GECO_JOB, ...) { GECO_ERROR(__VA_ARGS__); if ( (GECO_JOB)->traceFile ) GECOLogPrintf((GECO_JOB)->traceFile, GECOLogLevelError, __VA_ARGS__); }

//

typedef struct _GECOJob {
  //
  // Reference count:
  //
  unsigned int              refCount;
  //
  // Grid Engine job identifiers:
  //
  long int                  jobId, taskId;
  //
  // Stash the ppid of the first pid we receive:
  //
  pid_t                     firstSeenParentPid;
  long long int             firstSeenParentStartTime;
  //
  // CGroup init'ed states:
  //
  GECOFlags                 cgroupInitStates;
  //
  // Log file for user-requested execution traces:
  //
  GECOLogRef                traceFile;
  //
  // Resource information associated with the job:
  //
  GECOResourceSetRef        resourceInfo;
  GECOResourcePerNodeRef    hostResourceInfo;
  hwloc_bitmap_t            allocatedCpuSet;
  //
  // File descriptors for OOM-tracking for the job:
  //
  int                       oomEventFd, oomEntityFd;
  //
  // Runloop in which we're scheduled:
  //
  GECORunloopRef            scheduledInRunloop;
  //
  // Implemented as a linked list of active jobs:
  //
  struct _GECOJob           *link;
} GECOJob;

//

static GECOJob  *__GECOJobPool = NULL;
static GECOJob  *__GECOJobList = NULL;
static bool     __GECOJobInited = false;

//

void
__GECOJobInit(void)
{
  //
  // Let's pre-allocate 20 contiguous job records and load them into the
  // __GECOJobPool
  //
  size_t        stride;
  void          *newRegion;
  
  stride = sizeof(GECOJob) / 8;
  stride += (((sizeof(GECOJob) % 8) == 0) ? 0 : 1);
  stride *= 8;
  newRegion = calloc(20, stride);
  
  if ( newRegion ) {
    int         nJobs = 20;
    
    while ( nJobs-- > 0 ) {
      GECOJob   *nextJob = newRegion;
      
      newRegion += stride;
      nextJob->link = __GECOJobPool;
      __GECOJobPool = nextJob;
    }
    __GECOJobInited = true;
  }
}

//

GECOJob*
__GECOJobAlloc(void)
{
  GECOJob     *newJob = NULL;
  
  if ( ! __GECOJobInited ) __GECOJobInit();
  
  if ( __GECOJobPool ) {
    newJob = __GECOJobPool;
    __GECOJobPool = newJob->link;
  } else {
    newJob = malloc(sizeof(GECOJob));
  }
  if ( newJob ) {
    memset(newJob, 0, sizeof(*newJob));
    newJob->refCount = 1;
    newJob->oomEventFd = newJob->oomEntityFd = -1;
    newJob->firstSeenParentPid = -1;
  }
  return newJob;
}

//

void
__GECOJobDealloc(
  GECOJob   *theJob
)
{
  theJob->link = __GECOJobPool;
  __GECOJobPool = theJob;
}

//

void
__GECOJobSetupTraceFile(
  GECOJob   *theJob
)
{
  GECOLogLevel    traceLevel = GECOResourceSetGetTraceLevel(theJob->resourceInfo);
  
  if ( traceLevel > GECOLogLevelQuiet ) {
    char                  path[PATH_MAX];
    int                   pathLen;
    const char            *thisHostname = GECOGetHostname();
    
    if ( thisHostname ) {
      pathLen = snprintf(path, sizeof(path), "%s/tracefiles/%ld.%ld.%s", GECOGetStateDir(), theJob->jobId, theJob->taskId, thisHostname);
      if ( pathLen >= sizeof(path) ) {
        GECO_ERROR("__GECOJobSetupTraceFile: path exceeds PATH_MAX (%d >= %d)", pathLen, (int)PATH_MAX);
        return;
      }
      theJob->traceFile = GECOLogCreateWithFilePath(traceLevel, path);
      if ( theJob->traceFile ) {
        GECO_TRACE_INFO(theJob, "trace file opened for job %ld.%ld on host %s", theJob->jobId, theJob->taskId, thisHostname);
      } else {
        GECO_ERROR("__GECOJobSetupTraceFile: could not create trace file for %ld.%ld at %s", theJob->jobId, theJob->taskId, path);
      }
    } else {
      GECO_ERROR("__GECOJobSetupTraceFile: could not determine hostname");
    }
  }
}

//

bool
__GECOJobSetupOOMDescriptors(
  GECOJob     *theJob
)
{
  char        path[PATH_MAX];
  int         pathLen;
  
  //
  // Get the memory.oom_control file opened:
  //
  pathLen = GECOCGroupSnprintf(path, sizeof(path), GECOCGroupSubsystem_memory, theJob->jobId, theJob->taskId, "memory.oom_control");
  if ( pathLen > 0 && pathLen < sizeof(path) ) {
    //
    // Disable the oom kill:
    //
    theJob->oomEntityFd = open(path, O_WRONLY);
    if ( theJob->oomEntityFd >= 0 ) {
      if ( write(theJob->oomEntityFd, "1", 1) == 1 ) {
        GECO_TRACE_DEBUG(theJob, "oom setup: oom_kill disabled for %ld.%ld opened at %s", theJob->jobId, theJob->taskId, path);
      } else {
        GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: failed to disable oom_kill (errno = %d)", path, errno);
      }
      close(theJob->oomEntityFd);
      theJob->oomEntityFd = -1;
    } else {
      GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: failed to open %s for oom_kill disabling (errno = %d)", path, errno);
    }
    
    //
    // Start watching for events:
    //
    theJob->oomEntityFd = open(path, O_RDONLY);
    if ( theJob->oomEntityFd >= 0 ) {
      GECO_TRACE_DEBUG(theJob, "oom setup: oom control for %ld.%ld opened at %s", theJob->jobId, theJob->taskId, path);
      
      //
      // Get the event file opened:
      //
      theJob->oomEventFd = eventfd(0, EFD_NONBLOCK);
      if ( theJob->oomEntityFd >= 0 ) {
        GECO_TRACE_DEBUG(theJob, "oom setup: event file descriptor for %ld.%ld opened", theJob->jobId, theJob->taskId);
        
        //
        // Initialize the event control:
        //
        pathLen = snprintf(path, sizeof(path), "%d %d", theJob->oomEventFd, theJob->oomEntityFd);
        if ( GECOCGroupWriteEventControl(GECOCGroupSubsystem_memory, theJob->jobId, theJob->taskId, path, pathLen) ) {
          GECO_TRACE_DEBUG(theJob, "oom setup: registered with cgroup event control for %ld.%ld", theJob->jobId, theJob->taskId);
          return true;
        } else {
          close(theJob->oomEntityFd);
          theJob->oomEntityFd = -1;
          close(theJob->oomEventFd);
          theJob->oomEventFd = -1;
          GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: failed to register with cgroup event control for %ld.%ld (errno = %d)", theJob->jobId, theJob->taskId, errno);
        }
      } else {
        close(theJob->oomEntityFd);
        theJob->oomEntityFd = -1;
        GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: failed to create event file descriptor for %ld.%ld (errno = %d)", theJob->jobId, theJob->taskId, errno);
      }
    } else {
      GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: failed to open %s for monitoring (errno = %d)", path, errno);
    }
  } else {
    GECO_TRACE_ERROR(theJob, "__GECOJobSetupOOMDescriptors: path exceeds PATH_MAX (%d >= %d)", pathLen, (int)sizeof(path));
  }
  return false;
}

//

void
__GECOJobDestroy(
  GECOJobRef  theJob
)
{
  if ( theJob->scheduledInRunloop ) {
    GECO_TRACE_DEBUG(theJob, "unscheduling job %ld.%ld from runloop", theJob->jobId, theJob->taskId);
    GECORunloopRemovePollingSource(theJob->scheduledInRunloop, theJob);
    theJob->scheduledInRunloop = false;
  }
  
  if ( theJob->resourceInfo ) {
#ifdef GECO_JOB_ALWAYS_DESTROY_RESOURCE_CACHE
    // If this is the master task, then try to remove the resource file:
    if ( (GECOResourceSetGetNodeCount(theJob->resourceInfo) > 1) && ! GECOResourcePerNodeGetIsSlave(theJob->hostResourceInfo) ) {
      char                path[PATH_MAX];
      
      snprintf(path, sizeof(path), "%s/resources/%ld.%ld", GECOGetStateDir(), theJob->jobId, theJob->taskId);
      if ( unlink(path) == 0 ) {
        GECO_TRACE_DEBUG(theJob, "removed serialized resource information at %s", path);
      } else {
        GECO_TRACE_WARN(theJob, "failed to remove serialized resource information at %s (errno = %d)", path, errno);
      }
    }
#endif
    
    GECO_TRACE_DEBUG(theJob, "destroying in-memory resource information for job %ld.%ld", theJob->jobId, theJob->taskId);
    GECOResourceSetDestroy(theJob->resourceInfo);
    theJob->resourceInfo = NULL;
  }

  if ( theJob->oomEntityFd >= 0 ) {
    GECO_TRACE_DEBUG(theJob, "closing OOM monitored fd %d for job %ld.%ld", theJob->oomEntityFd, theJob->jobId, theJob->taskId);
    close(theJob->oomEntityFd);
    theJob->oomEntityFd = -1;
  }
  
  if ( theJob->oomEventFd >= 0 ) {
    GECO_TRACE_DEBUG(theJob, "closing OOM event fd %d for job %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
    close(theJob->oomEventFd);
    theJob->oomEventFd = -1;
  }
  
#ifdef LIBGECO_PRE_V101
  if ( theJob->allocatedCpuSet ) {
    char        *cpulist_str = NULL;
    
    hwloc_bitmap_list_asprintf(&cpulist_str, theJob->allocatedCpuSet);
    if ( cpulist_str ) {
      GECO_TRACE_INFO(theJob, "returning granted cpuset for job %ld.%ld: %s", theJob->jobId, theJob->taskId, cpulist_str);
      free(cpulist_str);
      cpulist_str = NULL;
    }
    GECOCGroupDeallocateCores(theJob->allocatedCpuSet);
    theJob->allocatedCpuSet = NULL;
  }
#else
  if ( theJob->allocatedCpuSet ) {
    hwloc_bitmap_free(theJob->allocatedCpuSet);
    theJob->allocatedCpuSet = NULL;
  }
#endif
  
  if ( theJob->traceFile ) {
    GECO_TRACE_INFO(theJob, "closing trace file for job %ld.%ld", theJob->jobId, theJob->taskId);
    GECOLogDestroy(theJob->traceFile);
    theJob->traceFile = NULL;
  }
  
#ifdef LIBGECO_PRE_V101
  GECOJobCGroupDeinit(theJob);
#endif
  
  // Remove job from the list:
  GECOJobRef        prev = NULL, node = __GECOJobList;
  
  while ( node && (node != theJob) ) {
    prev = node;
    node = node->link;
  }
  if ( node ) {
    if ( prev ) {
      prev->link = node->link;
    } else {
      __GECOJobList = node->link;
    }
  }
  
  // Hand the record back to our pool:
  __GECOJobDealloc(theJob);
}

//
#if 0
#pragma mark -
#endif
//

void
GECOJobInit(void)
{
  if ( ! __GECOJobInited ) __GECOJobInit();
}

//

void
GECOJobDeinit(void)
{
  while ( __GECOJobList ) __GECOJobDestroy(__GECOJobList);
}

//

GECOJobRef
__GECOJobCreateWithJobIdentifier(
  long int  jobId,
  long int  taskId,
  bool      shouldOnlyInitFromResourceCache
)
{
  GECOJob                 *newJob = __GECOJobList, *prev = NULL;
  
  while ( newJob ) {
    if ( newJob->jobId > jobId ) {
      newJob = NULL;
      break;
    }
    if ( newJob->jobId == jobId ) {
      if ( newJob->taskId > taskId ) {
        newJob = NULL;
        break;
      }
      if ( newJob->taskId == taskId ) break;
    }
    prev = newJob;
    newJob = newJob->link;
  }
  if ( ! newJob ) {
    GECOResourceSetRef      jobResources = NULL;
    GECOResourcePerNodeRef  jobPerNodeResources = NULL;
    char                    path[PATH_MAX];
    int                     pathLen;
    bool                    shouldExportResourceFile = false;
    
    if ( taskId <= 0 ) taskId = 1;
    
    pathLen = snprintf(path, sizeof(path), "%s/resources/%ld.%ld", GECOGetStateDir(), jobId, taskId);
    if ( pathLen >= sizeof(path) ) {
      GECO_ERROR("__GECOJobCreateWithJobIdentifier: path exceeds PATH_MAX (%d >= %d)", pathLen, (int)sizeof(path));
      return NULL;
    }
    if ( GECOIsFile(path) ) {
      int       try = 0;
      
retry:
      GECO_INFO("loading resource information for %ld.%ld from %s", jobId, taskId, path);
      jobResources = GECOResourceSetDeserialize(path);
      if ( ! jobResources ) {
        if ( try < 5 ) {
          GECO_ERROR("unable to deserialize %s, will try again...", path);
          GECOSleepForMicroseconds(1000000 * ++try);
          goto retry;
        }
        GECO_ERROR("failed to deserialize %s", path);
      }
    } else if ( ! shouldOnlyInitFromResourceCache ) {
      GECOResourceSetCreateFailure    failureReason;
      
      GECO_INFO("loading resource information for %ld.%ld via qstat", jobId, taskId);
      jobResources = GECOResourceSetCreate(jobId, taskId, 5, &failureReason);
      switch ( failureReason ) {
  
        case GECOResourceSetCreateFailureCheckErrno: {
          GECO_ERROR("__GECOJobCreateWithJobIdentifier: failed to find resource information for job %ld.%ld (errno = %d)", jobId, taskId, errno);
          return NULL;
        }
  
        case GECOResourceSetCreateFailureQstatFailure: {
          GECO_ERROR("__GECOJobCreateWithJobIdentifier: failed to find resource information for job %ld.%ld, general qstat failure", jobId, taskId);
          return NULL;
        }
  
        case GECOResourceSetCreateFailureMalformedQstatXML: {
          GECO_ERROR("__GECOJobCreateWithJobIdentifier: failed to find resource information for job %ld.%ld, qstat output is malformed", jobId, taskId);
          return NULL;
        }
  
        case GECOResourceSetCreateFailureJobDoesNotExist: {
          GECO_ERROR("__GECOJobCreateWithJobIdentifier: job %ld.%ld is not known to the qmaster", jobId, taskId);
          return NULL;
        }
        
      }
      shouldExportResourceFile = true;
    }
    
    if ( jobResources ) {
      jobPerNodeResources = GECOResourceSetGetPerNodeForHost(jobResources);
      if ( jobPerNodeResources ) {
        if ( (GECOResourceSetGetNodeCount(jobResources) > 1) && shouldExportResourceFile ) {
          GECO_DEBUG("serializing job resource information for %ld.%ld to %s", jobId, taskId, path);
          if ( ! GECOResourceSetSerialize(jobResources, path) ) {
            GECO_WARN("__GECOJobCreateWithJobIdentifier: failed to serialize job resource information for %ld.%ld to %s", jobId, taskId, path);
          }
        }
        
        // Create the new object:
        newJob = __GECOJobAlloc();
        if ( newJob ) {
          newJob->jobId               = jobId;
          newJob->taskId              = taskId;
          
          newJob->resourceInfo        = jobResources;
          newJob->hostResourceInfo    = jobPerNodeResources;
          
          __GECOJobSetupTraceFile(newJob);
          
          if ( prev ) {
            newJob->link = prev->link;
            prev->link = newJob;
          } else {
            newJob->link = __GECOJobList;
            __GECOJobList = newJob;
          }
        } else {
          GECO_ERROR("__GECOJobCreateWithJobIdentifier: unable to allocate GECOJob object for %ld.%ld", jobId, taskId);
          GECOResourceSetDestroy(jobResources);
        }
        
      } else {
        GECO_ERROR("__GECOJobCreateWithJobIdentifier: job %ld.%ld has no resource information for this node", jobId, taskId);
        GECOResourceSetDestroy(jobResources);
      }
    } else {
      GECO_ERROR("__GECOJobCreateWithJobIdentifier: could not load resource information for %ld.%ld", jobId, taskId);
    }
  } else {
    // Increase reference count:
    GECOJobRetain(newJob);
  }
  return newJob;
}

//

GECOJobRef
GECOJobCreateWithJobIdentifier(
  long int  jobId,
  long int  taskId
)
{
  return __GECOJobCreateWithJobIdentifier(jobId, taskId, false);
}

//

GECOJobRef
GECOJobCreateWithJobIdentifierFromResourceCache(
  long int  jobId,
  long int  taskId
)
{
  return __GECOJobCreateWithJobIdentifier(jobId, taskId, true);
}

//

GECOJobRef
GECOJobGetExistingObjectForJobIdentifier(
  long int  jobId,
  long int  taskId
)
{
  GECOJob                 *newJob = __GECOJobList, *prev = NULL;
  
  while ( newJob ) {
    if ( newJob->jobId > jobId ) {
      newJob = NULL;
      break;
    }
    if ( newJob->jobId == jobId ) {
      if ( newJob->taskId > taskId ) {
        newJob = NULL;
        break;
      }
      if ( newJob->taskId == taskId ) return newJob;
    }
    prev = newJob;
    newJob = newJob->link;
  }
  return NULL;
}

//

unsigned int
GECOJobGetReferenceCount(
  GECOJobRef  theJob
)
{
  return theJob->refCount;
}

//

void
GECOJobRetain(
  GECOJobRef  theJob
)
{
  theJob->refCount++;
}

//

void
GECOJobRelease(
  GECOJobRef  theJob
)
{
  if ( --(theJob->refCount) == 0 ) __GECOJobDestroy(theJob);
}

//

bool
GECOJobIdentiferExistsInResourceCache(
  long int  jobId,
  long int  taskId
)
{
  char      path[PATH_MAX];
  int       pathLen;
  
  if ( taskId <= 0 ) taskId = 1;
  
  pathLen = snprintf(path, sizeof(path), "%s/resources/%ld.%ld", GECOGetStateDir(), jobId, taskId);
  if ( pathLen >= sizeof(path) ) {
    GECO_ERROR("__GECOJobCreateWithJobIdentifier: path exceeds PATH_MAX (%d >= %d)", pathLen, (int)sizeof(path));
    return false;
  }
  return GECOIsFile(path);
}

//

long int
GECOJobGetJobId(
  GECOJobRef      theJob
)
{
  return theJob->jobId;
}

//

long int
GECOJobGetTaskId(
  GECOJobRef      theJob
)
{
  return theJob->taskId;
}

//

typedef struct {
  GECOJobRef      theJob;
  GECORunloopRef  theRunloop;
} GECOJobCGroupInitCallbackContext;

bool
__GECOJobCGroupInitCallback(
  long int              jobId,
  long int              taskId,
  const void            *context,
  GECOCGroupSubsystem   theSubsystem,
  const char            *cgroupPath,
  bool                  isNewSubgroup
)
{
  GECOJobCGroupInitCallbackContext    *CONTEXT = (GECOJobCGroupInitCallbackContext*)context;
  GECOJobRef                          theJob = CONTEXT->theJob;
  bool                                rc = true;
  
  if ( isNewSubgroup ) {
    GECOResourcePerNodeData rsrcLimits;
    
    GECOResourcePerNodeGetNodeData(theJob->hostResourceInfo, &rsrcLimits);
    switch ( theSubsystem ) {
    
      case GECOCGroupSubsystem_memory: {
        if ( GECOCGroupGetSubsystemIsManaged(GECOCGroupSubsystem_memory) ) {
          bool                        limitWasSet = false;
          
          if ( GECOFLAGS_ISSET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_memory)) ) {
            //
            // Cleanup from previous run that exited and caused the subgroup to be destroyed:
            //
            if ( theJob->scheduledInRunloop ) {
              GECO_TRACE_DEBUG(theJob, "unscheduling older OOM watch for job %ld.%ld from runloop", theJob->jobId, theJob->taskId);
              GECORunloopRemovePollingSource(theJob->scheduledInRunloop, theJob);
              theJob->scheduledInRunloop = false;
            }
            if ( theJob->oomEntityFd >= 0 ) {
              GECO_TRACE_DEBUG(theJob, "closing older OOM monitored fd %d for job %ld.%ld", theJob->oomEntityFd, theJob->jobId, theJob->taskId);
              close(theJob->oomEntityFd);
              theJob->oomEntityFd = -1;
            }
            if ( theJob->oomEventFd >= 0 ) {
              GECO_TRACE_DEBUG(theJob, "closing older OOM event fd %d for job %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
              close(theJob->oomEventFd);
              theJob->oomEventFd = -1;
            }
          }
          GECOFLAGS_SET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_memory));
          //
          // Set the real memory limit first:
          //
          if ( rsrcLimits.memoryLimit > 0 ) {
            if ( GECOCGroupSetMemoryLimit(theJob->jobId, theJob->taskId, rsrcLimits.memoryLimit) ) {
              GECO_TRACE_INFO(theJob, "memory limit of %.0lf set for %ld.%ld", rsrcLimits.memoryLimit, theJob->jobId, theJob->taskId);
              limitWasSet = true;
            } else {
              GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: failed to set memory limit of %.0lf for %ld.%ld", rsrcLimits.memoryLimit, theJob->jobId, theJob->taskId);
              rc = false;
            }
          }
          //
          // Set the virtual memory limit next:
          //
          if ( rsrcLimits.virtualMemoryLimit > 0 ) {
            if ( GECOCGroupSetMemoryLimit(theJob->jobId, theJob->taskId, rsrcLimits.virtualMemoryLimit) ) {
              GECO_TRACE_INFO(theJob, "virtual memory limit of %.0lf set for %ld.%ld", rsrcLimits.virtualMemoryLimit, theJob->jobId, theJob->taskId);
              limitWasSet = true;
            } else {
              GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: failed to set virtual memory limit of %.0lf for %ld.%ld", rsrcLimits.virtualMemoryLimit, theJob->jobId, theJob->taskId);
              rc = false;
            }
          }
          //
          // Watch for OOM events if necessary:
          //
          if ( limitWasSet && CONTEXT->theRunloop ) {
            if ( GECOJobScheduleOOMWatchInRunloop(theJob, CONTEXT->theRunloop) ) {
              GECO_TRACE_INFO(theJob, "registered to observe OOM events on %ld.%ld", theJob->jobId, theJob->taskId);
            } else {
              GECO_TRACE_WARN(theJob, "failed to register to observe OOM events on %ld.%ld", theJob->jobId, theJob->taskId);
            }
          }
        }
        break;
      }
    
      case GECOCGroupSubsystem_cpuset: {
        if ( GECOCGroupGetSubsystemIsManaged(GECOCGroupSubsystem_cpuset) ) {
#ifdef LIBGECO_PRE_V101
          bool              firstTry = true;
          
          if ( GECOFLAGS_ISSET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_cpuset)) ) {
            //
            // Cleanup from previous run that exited and caused the subgroup to be destroyed:
            //
            if ( theJob->allocatedCpuSet ) {
              char        *cpulist_str = NULL;
              
              hwloc_bitmap_list_asprintf(&cpulist_str, theJob->allocatedCpuSet);
              if ( cpulist_str ) {
                GECO_TRACE_INFO(theJob, "reusing granted cpuset for job %ld.%ld: %s", theJob->jobId, theJob->taskId, cpulist_str);
                free(cpulist_str);
              }
            }
          }
          GECOFLAGS_SET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_cpuset));
          //
          // Attempt to allocate a cpuset for the job:
          //
cpuset_tryagain:
          if ( ! theJob->allocatedCpuSet ) {
            if ( GECOCGroupAllocateCores(rsrcLimits.slotCount, &theJob->allocatedCpuSet) ) {
              char        *cpulist_str = NULL;
              
              GECO_TRACE_INFO(theJob, "%ld core%s allocated to %ld.%ld", rsrcLimits.slotCount, ((rsrcLimits.slotCount == 1) ? "" : "s"), theJob->jobId, theJob->taskId);
              hwloc_bitmap_list_asprintf(&cpulist_str, theJob->allocatedCpuSet);
              if ( cpulist_str ) {
                GECO_TRACE_INFO(theJob, "  => %s", cpulist_str);
                free(cpulist_str);
              }
            } else if ( firstTry ) {
              if ( GECOCGroupScanActiveCpusetBindings() ) {
                GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: retrying core allocation after rescan of available cores");
                firstTry = false;
                goto cpuset_tryagain;
              } else {
                GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: unable to allocate %ld core%s for %ld.%ld (failed rescan of available cores)", rsrcLimits.slotCount, ((rsrcLimits.slotCount == 1) ? "" : "s"), theJob->jobId, theJob->taskId);
                rc = false;
              }
            }
          }
          if ( rc && theJob->allocatedCpuSet ) {
            if ( GECOCGroupSetCpusetCpus(theJob->jobId, theJob->taskId, theJob->allocatedCpuSet) ) {
              GECO_TRACE_INFO(theJob, "%ld.%ld successfully bound to allocated cpuset", theJob->jobId, theJob->taskId);
              rc = true;
            } else {
              GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: failed to bind %ld.%ld to allocated cpuset", theJob->jobId, theJob->taskId);
              GECOCGroupDeallocateCores(theJob->allocatedCpuSet);
              theJob->allocatedCpuSet = NULL;
              rc = false;
            }
          } else {
            GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: unable to allocate %ld core%s for %ld.%ld", rsrcLimits.slotCount, ((rsrcLimits.slotCount == 1) ? "" : "s"), theJob->jobId, theJob->taskId);
            rc = false;
          }
#else
          //
          // Version 1.0.1 and later:
          //
          // Ask for a cpuset, try to bind it, retry after 5 seconds if it fails, and do that for ~ 1 minute.
          //
          int         retryNumber = 0, maxRetryCount = 12;

          if ( GECOFLAGS_ISSET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_cpuset)) ) {
            //
            // Cleanup from previous run that exited and caused the subgroup to be destroyed:
            //
            if ( theJob->allocatedCpuSet ) {
              hwloc_bitmap_free(theJob->allocatedCpuSet);
              theJob->allocatedCpuSet = NULL;
            }
          }
          GECOFLAGS_SET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_cpuset));
cpuset_tryagain:
          if ( GECOCGroupAllocateCores(rsrcLimits.slotCount, &theJob->allocatedCpuSet) ) {
            if ( ! GECOCGroupSetCpusetCpus(theJob->jobId, theJob->taskId, theJob->allocatedCpuSet) ) {
              GECOCGroupDeallocateCores(theJob->allocatedCpuSet);
              theJob->allocatedCpuSet = NULL;
            }
          } else {
            GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: unable to allocate %ld core%s for %ld.%ld", rsrcLimits.slotCount, ((rsrcLimits.slotCount == 1) ? "" : "s"), theJob->jobId, theJob->taskId);
          }
          if ( ! theJob->allocatedCpuSet ) {
            if ( retryNumber++ < maxRetryCount ) {
              GECO_TRACE_WARN(theJob, "GECOJobCGroupInit: %ld.%ld will retry in 5 seconds (%d of %d)", theJob->jobId, theJob->taskId, retryNumber, maxRetryCount);
              sleep(5);
              goto cpuset_tryagain;
            } else {
              GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: %ld.%ld failed all retries", theJob->jobId, theJob->taskId);
              rc = false;
            }
          } else {
            char        *cpulist_str = NULL;
            
            hwloc_bitmap_list_asprintf(&cpulist_str, theJob->allocatedCpuSet);
            if ( cpulist_str ) {
              GECO_TRACE_INFO(theJob, "%ld.%ld successfully bound to cpuset %s", theJob->jobId, theJob->taskId, cpulist_str);
              free(cpulist_str);
            } else {
              GECO_TRACE_INFO(theJob, "%ld.%ld successfully bound to allocated cpuset", theJob->jobId, theJob->taskId);
            }
            rc = true;
          }
#endif
        }
        break;
      }
    
    }
  }
  return rc;
}

bool
GECOJobCGroupInit(
  GECOJobRef      theJob,
  GECORunloopRef  theRunloop
)
{
  bool                              rc = false;
  GECOJobCGroupInitCallbackContext  callbackContext = {
                                        .theJob = theJob,
                                        .theRunloop = theRunloop
                                      };
  
  
  rc = GECOCGroupInitForJobIdentifier(theJob->jobId, theJob->taskId, __GECOJobCGroupInitCallback, &callbackContext);
  if ( ! rc ) {
    GECO_TRACE_ERROR(theJob, "GECOJobCGroupInit: unable to initialize cgroup support for %ld.%ld", theJob->jobId, theJob->taskId);
  }
  return rc;
}

//

bool
GECOJobCGroupDeinit(
  GECOJobRef    theJob
)
{
  bool          rc = true;
  
  if ( theJob->cgroupInitStates ) {
    if ( GECOCGroupDeinitForJobIdentifier(theJob->jobId, theJob->taskId, NULL, NULL) ) {
      GECO_TRACE_INFO(theJob, "deinitialized all cgroup support for %ld.%ld", theJob->jobId, theJob->taskId);
      theJob->cgroupInitStates = 0;
    } else {
      GECO_TRACE_ERROR(theJob, "GECOJobCGroupDeinit: unable to deinitialize cgroup support for %ld.%ld", theJob->jobId, theJob->taskId);
      rc = false;
    }
  }
  return rc;
}

//

bool
GECOJobCGroupAddPidAndChildren(
  GECOJobRef    theJob,
  pid_t         aPid,
  bool          shouldAddChildProcesses
)
{
  bool          rc = GECOCGroupAddTaskAndChildren(GECOCGroupSubsystem_all, theJob->jobId, theJob->taskId, aPid, shouldAddChildProcesses);
  
  if ( rc ) {
    GECO_TRACE_INFO(theJob, "pid %ld quarantined to all cgroups for %ld.%ld", (long int)aPid, theJob->jobId, theJob->taskId);
    if ( theJob->firstSeenParentPid == -1 ) {
      if ( GECOGetPPidOfPid(aPid, &theJob->firstSeenParentPid) ) {
        GECOGetPidInfo(theJob->firstSeenParentPid, NULL, NULL, NULL, &theJob->firstSeenParentStartTime);
        GECO_TRACE_INFO(theJob, "stashed ppid %ld (start time %lld) of pid %ld for %ld.%ld", (long int)theJob->firstSeenParentPid, theJob->firstSeenParentStartTime, (long int)aPid, theJob->jobId, theJob->taskId);
      }
    }
  } else {
    GECO_TRACE_ERROR(theJob, "GECOJobCGroupAddPid: failed to quarantine pid %ld to all cgroups for %ld.%ld", (long int)aPid, theJob->jobId, theJob->taskId);
  }
  return rc;
}

//

bool
GECOJobCGroupAddPid(
  GECOJobRef    theJob,
  pid_t         aPid
)
{
  return GECOJobCGroupAddPidAndChildren(theJob, aPid, false);
}

//

bool
GECOJobHasExited(
  GECOJobRef    theJob
)
{
  if ( theJob->firstSeenParentPid >= 0 ) {
    long long int     startTime = -1000;
    
    if ( GECOGetPidInfo(theJob->firstSeenParentPid, NULL, NULL, NULL, &startTime) && (startTime == theJob->firstSeenParentStartTime) ) {
      GECO_INFO("Job appears to still be running:  pid %ld with start time %lld", (long int)theJob->firstSeenParentPid, theJob->firstSeenParentStartTime);
      return false;
    }
    if ( startTime == -1000 ) {
      GECO_INFO("Job appears to have exited:  pid %ld not present", (long int)theJob->firstSeenParentPid);
    } else {
      GECO_INFO("Job appears to have exited:  pid %ld shows start time %lld != %lld", (long int)theJob->firstSeenParentPid, startTime, theJob->firstSeenParentStartTime);
    }
    return true;
  }
  return false;
}

//

int
__GECOJobPollingSourceFileDescriptorForPolling(
  GECOPollingSource   theSource
)
{
  GECOJob             *theJob = (GECOJob*)theSource;
  
  return theJob->oomEventFd;
}

void
__GECOJobPollingSourceDidReceiveDataAvailable(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECOJob             *theJob = (GECOJob*)theSource;
  uint64_t            counter;
  
  if ( read(theJob->oomEventFd, &counter, sizeof(counter)) == sizeof(counter) ) {
    bool              isUnderOOM = false;
    //
    // Is the cgroup still there?
    //
    if ( GECOCGroupGetIsUnderOOM(theJob->jobId, theJob->taskId, &isUnderOOM) && isUnderOOM ) {
      GECO_TRACE_WARN(theJob, "GECOJob(oom-notification): out-of-memory event asserted on job %ld.%ld (counter = %llu)", theJob->jobId, theJob->taskId, (unsigned long long int)counter);
      
      GECOCGroupSignalTasks(GECOCGroupSubsystem_memory, theJob->jobId, theJob->taskId, SIGKILL);
      
      // Attempt to fork and run as the user in order to write to his/her working
      // directory for the job:
      char            path[PATH_MAX];
      const char      *jobName = GECOResourceSetGetJobName(theJob->resourceInfo);
      bool            isArrayJob = GECOResourceSetGetIsArrayJob(theJob->resourceInfo);
      int             pathLen;
      const char      *pathFormat;
      const char      *nodeName = GECOResourcePerNodeGetNodeName(theJob->hostResourceInfo);
      
      if ( jobName ) {
        pathFormat = isArrayJob ? "%1$s.oom%2$ld.%3$ld" : "%1$s.oom%2$ld";
      } else {
        pathFormat = isArrayJob ? "oom%2$ld.%3$ld" : "oom%2$ld";
      }
      pathLen = snprintf(path, sizeof(path), pathFormat, jobName, theJob->jobId, theJob->taskId);
      
      if ( pathLen > 0 && pathLen < sizeof(path) ) {
        pid_t         childPid;
        
        GECO_TRACE_INFO(theJob, "GECOJob(oom-notification): oom notification for job %ld.%ld to file %s", theJob->jobId, theJob->taskId, path);
        sync();
        switch ( (childPid = fork()) ) {
        
          case 0: {
            // Reset signal handling:
            signal(SIGHUP, SIG_DFL);
            signal(SIGTERM, SIG_DFL);
            signal(SIGINT, SIG_DFL);
            
            // Flush-and-close any files that are still open:
            if ( theJob->traceFile ) GECOLogDestroy(theJob->traceFile);
            for (int fd=3; fd<256; fd++) (void) close(fd);
            
            if ( GECOResourceSetExecuteAsOwner(theJob->resourceInfo) ) {
              FILE    *oomFPtr = fopen(path, "w");
              
              if ( oomFPtr ) {
                fprintf(oomFPtr, "%s: Job %ld.%ld exceeded its memory limits and was killed.\n", (nodeName ? nodeName : "<unknown node>"), theJob->jobId, theJob->taskId);
                fclose(oomFPtr);
              }
            }
            exit(0);
          }
          
          case -1: {
            GECO_TRACE_ERROR(theJob, "GECOJob(oom-notification): unable to fork() to write user's notification file (errno = %d)", errno);
            break;
          }
          
          default: {
            // We don't wait for the child to exit, we'll catch SIGCHLD elsewhere and reap it.
            GECO_TRACE_INFO(theJob, "GECOJob(oom-notification): user notification file process %ld forked", (long int)childPid);
            break;
          }
          
        }
      } else {
        GECO_TRACE_ERROR(theJob, "GECOJob(oom-notification): maximum path length exceeded for oom notification for job %ld.%ld", theJob->jobId, theJob->taskId);
      }
    } else {
      GECO_TRACE_DEBUG(theJob, "GECOJob(oom-notification): spurious oom event caught for job %ld.%ld", theJob->jobId, theJob->taskId);
    }
  }
}

void
__GECOJobPollingSourceDidReceiveClose(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECOJob             *theJob = (GECOJob*)theSource;
  
  if ( theJob->oomEntityFd >= 0 ) {
    GECO_TRACE_DEBUG(theJob, "closing OOM monitored fd %d for job %ld.%ld", theJob->oomEntityFd, theJob->jobId, theJob->taskId);
    close(theJob->oomEntityFd);
    theJob->oomEntityFd = -1;
  }
  
  if ( theJob->oomEventFd >= 0 ) {
    GECO_TRACE_DEBUG(theJob, "closing OOM event fd %d for job %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
    close(theJob->oomEventFd);
    theJob->oomEventFd = -1;
  }
}

//

void
__GECOJobPollingSourceDidRemoveAsSource(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECOJob             *theJob = (GECOJob*)theSource;
  
  GECO_TRACE_DEBUG(theJob, "OOM event fd %d was removed from runloop for %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
}

//

bool
GECOJobScheduleOOMWatchInRunloop(
  GECOJobRef      theJob,
  GECORunloopRef  theRunloop
)
{
  bool          rc = true;
  
  if ( GECOFLAGS_ISSET(theJob->cgroupInitStates, (1 << GECOCGroupSubsystem_memory)) ) {
    //
    // Attempt to setup the OOM descriptors:
    //
    if ( __GECOJobSetupOOMDescriptors(theJob) ) {
      GECOPollingSourceCallbacks    oomEventCallbacks = {
                                            .destroySource = NULL,
                                            .fileDescriptorForPolling = __GECOJobPollingSourceFileDescriptorForPolling,
                                            .shouldSourceClose = NULL,
                                            .willRemoveAsSource = NULL,
                                            .didAddAsSource = NULL,
                                            .didBeginPolling = NULL,
                                            .didReceiveDataAvailable = __GECOJobPollingSourceDidReceiveDataAvailable,
                                            .didEndPolling = NULL,
                                            .didReceiveClose = __GECOJobPollingSourceDidReceiveClose,
                                            .didRemoveAsSource = __GECOJobPollingSourceDidRemoveAsSource
                                          };
      //
      // Register as a polling source with the given runloop:
      //
      if ( GECORunloopAddPollingSource(theRunloop, theJob, &oomEventCallbacks, GECOPollingSourceFlagStaticFileDescriptor | GECOPollingSourceFlagHighPriority) ) {
        GECO_TRACE_INFO(theJob, "OOM event fd %d registered with runloop for %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
        theJob->scheduledInRunloop = theRunloop;
      } else {
        GECO_TRACE_ERROR(theJob, "GECOJobScheduleOOMWatchInRunloop: unable to register event fd %d with runloop for job %ld.%ld", theJob->oomEventFd, theJob->jobId, theJob->taskId);
        __GECOJobPollingSourceDidReceiveClose(theJob, NULL);
        rc = false;
      }
    } else {
      rc = false;
    }
  } else {
    GECO_TRACE_INFO(theJob, "no memory quarantine for %ld.%ld, not scheduling OOM watch", theJob->jobId, theJob->taskId);
  }
  return rc;
}

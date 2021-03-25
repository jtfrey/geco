/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  gecod.c
 *  
 *  Daemon that intercepts process launches in search of Grid Engine
 *  job pieces, and automatically creates cgroups to mirror requested
 *  resource limits.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECORunloop.h"
#include "GECOResource.h"
#include "GECOCGroup.h"
#include "GECOIntegerSet.h"
#include "GECOPidMap.h"
#include "GECOJob.h"
#include "GECOLog.h"

//

#ifndef GECOD_STARTUP_RETRY_COUNT
#define GECOD_STARTUP_RETRY_COUNT   6
#endif

static int GECODDefaultStartupRetryCount = GECOD_STARTUP_RETRY_COUNT;

//

static GECORunloopRef GECODRunloop = NULL;

//

static GECOIntegerSetRef GECODAllowedUids = NULL;
static GECOIntegerSetRef GECODAllowedGids = NULL;

static inline bool
gecod_isUidAllowed(
  uid_t     theUid
)
{
  return ( GECODAllowedUids && GECOIntegerSetContains(GECODAllowedUids, theUid) ) ? true : false;
}

static inline bool
gecod_isGidAllowed(
  gid_t     theGid
)
{
  return ( GECODAllowedGids && GECOIntegerSetContains(GECODAllowedGids, theGid) ) ? true : false;
}

//

#include <signal.h>

#include <sys/socket.h>
#include <linux/connector.h>
#include <linux/netlink.h>
#include <linux/cn_proc.h>

#define GECOD_SEND_MESSAGE_LEN (NLMSG_LENGTH(sizeof(struct cn_msg) + \
				       sizeof(enum proc_cn_mcast_op)))
#define GECOD_RECV_MESSAGE_LEN (NLMSG_LENGTH(sizeof(struct cn_msg) + \
				       sizeof(struct proc_event)))

#define GECOD_SEND_MESSAGE_SIZE    (NLMSG_SPACE(GECOD_SEND_MESSAGE_LEN))
#define GECOD_RECV_MESSAGE_SIZE    (NLMSG_SPACE(GECOD_RECV_MESSAGE_LEN))

#define GECOD_max(x,y) ((y)<(x)?(x):(y))
#define GECOD_min(x,y) ((y)>(x)?(x):(y))

#define GECOD_NLMSG_BUFFER_SIZE (GECOD_max(GECOD_max(GECOD_SEND_MESSAGE_SIZE, GECOD_RECV_MESSAGE_SIZE), 4096))
#define GECOD_MIN_RECV_SIZE (GECOD_min(GECOD_SEND_MESSAGE_SIZE, GECOD_RECV_MESSAGE_SIZE))

#define GECOD_PROC_CN_MCAST_LISTEN (1)
#define GECOD_PROC_CN_MCAST_IGNORE (2)

//
#if 0
#pragma mark -
#endif
//

static GECOIntegerSetRef GECODTrackedParentProcesses = NULL;

void
GECODTrackedParentProcessesAdd(
  pid_t       parentPid
)
{
  if ( ! GECODTrackedParentProcesses ) GECODTrackedParentProcesses = GECOIntegerSetCreate();
  if ( GECODTrackedParentProcesses ) {
    unsigned int    count;
    
    GECOIntegerSetAddInteger(GECODTrackedParentProcesses, (GECOInteger)parentPid);
    count = GECOIntegerSetGetCount(GECODTrackedParentProcesses);
    GECO_DEBUG("process tracking table contains %d pid%s", count, (count == 1) ? "" : "s" );
  }
}

bool
GECODTrackedParentProcessesIsKnownPid(
  pid_t       parentPid
)
{
  if ( GECODTrackedParentProcesses ) return GECOIntegerSetContains(GECODTrackedParentProcesses, (GECOInteger)parentPid);
  return false;
}

void
GECODTrackedParentProcessesRemove(
  pid_t       parentPid
)
{
  if ( GECODTrackedParentProcesses ) {
    unsigned int    count;
    
    GECOIntegerSetRemoveInteger(GECODTrackedParentProcesses, (GECOInteger)parentPid);
    count = GECOIntegerSetGetCount(GECODTrackedParentProcesses);
    GECO_DEBUG("process tracking table contains %d pid%s", count, (count == 1) ? "" : "s" );
  }
}

//

typedef enum {
  GECODProcessCommSGEShepherd = 0,
  GECODProcessCommSSHD,
  GECODProcessCommUnhandled
} GECODProcessComm;

const char* GECODProcessCommString[] = {
                  "sge_shepherd",
                  "sshd"
                };

GECODProcessComm
GECODProcessCommForPid(
  pid_t               aPid
)
{
  GECODProcessComm    procComm = GECODProcessCommUnhandled;
  char                comm[64];
  int                 commFd;
  
  snprintf(comm, sizeof(comm), "/proc/%ld/comm", (long int)aPid);
  commFd = open(comm, O_RDONLY);
  if ( commFd >= 0 ) {
    ssize_t             count = read(commFd, comm, sizeof(comm));
    
    close(commFd);
    if ( count > 0 ) {
      char              *s = comm + ( (count < sizeof(comm)) ? count : --count );
      
      *s = '\0';
      while ( isspace(*(--s)) ) {
        *s = '\0';
        count--;
      }
      
      procComm = GECODProcessCommSGEShepherd;
      while ( procComm < GECODProcessCommUnhandled ) {
        if ( (count == strlen(GECODProcessCommString[procComm])) && (strncmp(comm, GECODProcessCommString[procComm], count) == 0) ) break;
        procComm++;
      }
    }
  }
  return procComm;
}

//

const char*
GECODProcessReadCommForPid(
  pid_t               aPid
)
{
  GECODProcessComm    procComm = GECODProcessCommUnhandled;
  static char         comm[PATH_MAX];
  int                 commFd;
  
  snprintf(comm, sizeof(comm), "/proc/%ld/comm", (long int)aPid);
  commFd = open(comm, O_RDONLY);
  if ( commFd >= 0 ) {
    ssize_t             count = read(commFd, comm, sizeof(comm));
    
    close(commFd);
    if ( count > 0 ) {
      char              *s = comm + ( (count < sizeof(comm)) ? count : --count );
      
      *s = '\0';
      while ( count && isspace(*(--s)) ) {
        *s = '\0';
        count--;
      }
      return comm;
    }
  }
  return NULL;
}

//

bool
GECODFetchSGEShepherdJobIdentifier(
  pid_t         shepherdPid,
  long int      *jobId,
  long int      *taskId
)
{
  char          cwdPath[64];
  char          cwdBuffer[PATH_MAX];
  ssize_t       cwdBufferLen = 0;
  
  snprintf(cwdPath, sizeof(cwdPath), "/proc/%ld/cwd", (long int)shepherdPid);
  cwdBufferLen = readlink(cwdPath, cwdBuffer, sizeof(cwdBuffer));
  if ( (cwdBufferLen > 0) && (cwdBufferLen <= PATH_MAX) ) {
    char        *s = cwdBuffer + cwdBufferLen;
    ssize_t     sLen = cwdBufferLen;
    int         tries = 0;
    
    *s = '\0';

retry:
    while ( sLen ) {
      if ( *(--s) == '/' ) break;
      sLen--;
    }
    if ( sLen && (tries < 2) ) {
      long int  v1, v2;
      
      if ( sscanf(s, "/%ld.%ld", &v1, &v2) == 2 ) {
        *jobId = v1;
        *taskId = v2;
        return true;
      }
      tries++;
      goto retry;
    }
  }
  return false;
}

//

bool
GECODFetchEnvironJobIdentifier(
  pid_t         aPid,
  long int      *jobId,
  long int      *taskId
)
{
  char          envPath[64];
  int           envFd;
  
  snprintf(envPath, sizeof(envPath), "/proc/%ld/environ", (long int)aPid);
  if ( (envFd = open(envPath, O_RDONLY)) ) {
    char        staticBuffer[1024];
    char        *buffer = staticBuffer;
    size_t      bufferLen = sizeof(staticBuffer);
    long int    foundJobId = -1, foundTaskId = -1;
    
    //
    // Scan through the file, reading-in NUL-terminated strings:
    //
    while ( 1 ) {
      size_t    i = 0, tokenIdx = 0;
      
      while ( (read(envFd, buffer + i, 1) == 1) ) {
        if ( buffer[i] == '\0' ) break;
        if ( buffer[i] == '=' ) tokenIdx = i;
        if ( ++i == bufferLen ) {
          if ( buffer == staticBuffer ) {
            buffer = malloc(bufferLen + 256);
            if ( ! buffer ) {
              goto earlyExit;
            }
            bufferLen += 256;
          } else {
            buffer = realloc(buffer, bufferLen + 256);
            if ( ! buffer ) {
              goto earlyExit;
            }
            bufferLen += 256;
          }
        }
      }
      if ( (i > 0) && (tokenIdx > 0) ) {
        if ( (tokenIdx == 6) && (strncmp(buffer, "JOB_ID", 6) == 0) ) {
          GECO_strtol(buffer + tokenIdx + 1, &foundJobId, NULL);
        }
        else if ( (tokenIdx == 11) && (strncmp(buffer, "SGE_TASK_ID", 11) == 0) ) {
          GECO_strtol(buffer + tokenIdx + 1, &foundTaskId, NULL);
        }
      } else {
        break;
      }
    }
    if ( buffer != staticBuffer ) free(buffer);
    
earlyExit:
    close(envFd);
    
    if ( foundJobId >= 0 ) {
      *jobId = foundJobId;
      *taskId = (foundTaskId >= 0) ? foundTaskId : 1;
      return true;
    }
  }
  return false;
}

//
#if 0
#pragma mark -
#endif
//

typedef struct {
  int                 fd;
  char                msgBuffer[GECOD_NLMSG_BUFFER_SIZE];
} GECODNetlinkSocket;

//

void
GECODNetlinkSocketDestroySource(
  GECOPollingSource   theSource
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  if ( src->fd >= 0 ) {
    close(src->fd);
    src->fd = -1;
  }
}

int
GECODNetlinkSocketFileDescriptorForPolling(
  GECOPollingSource   theSource
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  return src->fd;
}

void
GECODNetlinkSocketDidReceiveDataAvailable(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  ssize_t             msgSize;
  struct nlmsghdr     *nl_hdr;
  struct cn_msg       *cn_hdr;
  
  msgSize = read(src->fd, src->msgBuffer, sizeof(src->msgBuffer));
  if ( msgSize > 0 ) {
    bool            isDecoding = true;
    
    GECO_DEBUG("read %d byte%s on netlink socket %d", msgSize, ((msgSize != 1) ? "s" : ""), src->fd);
    
    //
    // Decode one or more messages:
    //
    nl_hdr = (struct nlmsghdr*)&src->msgBuffer;
    while ( isDecoding && NLMSG_OK(nl_hdr, msgSize) ) {
      switch ( nl_hdr->nlmsg_type ) {
      
        case NLMSG_NOOP:
          break;
          
        case NLMSG_ERROR:
        case NLMSG_OVERRUN:
          isDecoding = false;
          break;
        
        case NLMSG_DONE:
        default: {
          struct proc_event     *event;
          
          cn_hdr = NLMSG_DATA(nl_hdr);
          event = (struct proc_event *)cn_hdr->data;
          switch ( event->what ) {
          
            //
            // A process called one of the exec*() functions.
            //
            case PROC_EVENT_EXEC: {
              long int          jobId = -1, taskId = -1;
              const char        *jobName = NULL;
              bool              foundJobIds = false;
              pid_t             execPid = event->event_data.exec.process_pid;
              GECODProcessComm  procComm = GECODProcessCommForPid(execPid);
              pid_t             ppid = -1;
              uid_t             puid = -1;
              gid_t             pgid = -1;
              bool              hasBeenHandled = false;
              
              if ( procComm != GECODProcessCommUnhandled ) {
                //
                // The process's command is something we react to:
                //
                GECO_DEBUG("%s exec event for pid %ld", GECODProcessCommString[procComm], (long int)execPid);
                if ( GECOGetPidInfo(execPid, &ppid, &puid, &pgid) ) {
                  switch ( procComm ) {
                  
                    case GECODProcessCommSGEShepherd: {
                      GECODTrackedParentProcessesAdd(execPid);
                      GECO_DEBUG("GE shepherd pid %ld added to tracking table", (long int)execPid);
                      hasBeenHandled = true;
                      break;
                    }
                    
                    case GECODProcessCommSSHD: {
                      //
                      // We can short-cut this if the process is a child of an sge_shepherd:
                      //
                      if ( GECOPidIsChildOfSGEShepherd(execPid, &ppid) ) {
                        foundJobIds = GECODFetchSGEShepherdJobIdentifier(ppid, &jobId, &taskId);
                        if ( foundJobIds ) {
                          GECO_INFO("sshd pid %ld (descendent of %ld) implies %ld.%ld", (long int)execPid, (long int)ppid, jobId, taskId);
                        }
                        // Don't mark hasBeenHandled = true.  This allows the generic exec
                        // code to pickup this process and create/locate a job wrapper and
                        // perform cgroup quarantine.
                      }
                      if ( ! foundJobIds ) {
                        GECODTrackedParentProcessesAdd(execPid);
                        GECO_DEBUG("sshd pid %ld added to tracking table", (long int)execPid);
                        hasBeenHandled = true;
                      }
                      break;
                    }
                    
                  }
                } else {
                  //
                  // If we can't get ppid/uid/gid, then this is a defunct process anyway
                  // so there's nothing we need to do:
                  //
                  GECO_DEBUG("failed to read info for pid %ld", (long int)execPid);
                  hasBeenHandled = true;
                }
              }
              
              if ( ! hasBeenHandled ) {
                //
                // Handle the exec of any other process that we don't consider "special"
                //
                GECO_DEBUG("handling generic exec event for pid %ld", (long int)execPid);
                
                //
                // If this pid was previously noted on a fork, then remove it -- it's no longer special
                // and we don't wish to track it:
                //
                if ( GECODTrackedParentProcessesIsKnownPid(execPid) ) {
                  GECODTrackedParentProcessesRemove(execPid);
                  GECO_INFO("pid %ld (forked special process, exec'ed as something else) removed from tracking table", (long int)execPid);
                }
                
                //
                // We can short-cut this if the process is a child of an sge_shepherd:
                //
                if ( ! foundJobIds && GECOPidIsChildOfSGEShepherd(execPid, &ppid) ) {
                  foundJobIds = GECODFetchSGEShepherdJobIdentifier(ppid, &jobId, &taskId);
                  if ( foundJobIds ) {
                    GECOJobRef    extantJob = GECOJobGetExistingObjectForJobIdentifier(jobId, taskId);
                    
                    GECO_INFO("pid %ld (descendent of %ld) implies %ld.%ld", (long int)execPid, (long int)ppid, jobId, taskId);
                    if ( extantJob ) {
                      if ( GECOJobCGroupAddPid(extantJob, execPid) ) {
                        GECO_INFO("job wrapper for %ld.%ld exists, pid %ld added to cgroups", jobId, taskId, (long int)execPid);
                        hasBeenHandled = true;
                      } else {
                        int       killStatus = kill(execPid, SIGKILL);
                        
                        if ( killStatus == 0 ) {
                          GECO_INFO("job wrapper for %ld.%ld exists, pid %ld has been terminated (rc = %d)", jobId, taskId, (long int)execPid, killStatus);
                        } else {
                          GECO_INFO("job wrapper for %ld.%ld exists, pid %ld has already terminated", jobId, taskId, (long int)execPid);
                        }
                        hasBeenHandled = true;
                      }
                    }
                  }
                }
                
                if ( ! hasBeenHandled && ! foundJobIds ) {
                  //
                  // Try to get some vital process metadata -- parent pid, uid, gid:
                  //
                  if ( GECOGetPidInfo(execPid, &ppid, &puid, &pgid) ) {
                    //
                    // If the parent process of the execPid is being tracked, then try to find Grid Engine
                    // job information in execPid's environment.
                    //
                    if ( GECODTrackedParentProcessesIsKnownPid(ppid) ) {
                      foundJobIds = GECODFetchEnvironJobIdentifier(execPid, &jobId, &taskId);
                      if ( foundJobIds ) {
                        //
                        // The execPid environment contained variables specifying the Grid Engine job id:
                        //
                        GECO_INFO("pid %ld (child of %ld) includes environment variables indicating %ld.%ld", (long int)execPid, (long int)ppid, jobId, taskId);
                      } else {
                        //
                        // The execPid environment did not contain variables specifying a Grid Engine job id.  Consider
                        // this a rogue process if its uid is above the threshold and kill it immediately.  Otherwise,
                        // just ignore it (e.g. could be nagios, Grid Engine, or even a root ssh from the head node).
                        //
                        if ( gecod_isUidAllowed(puid) || gecod_isGidAllowed(pgid) ) {
                          GECO_INFO("ignoring pid %ld (uid/gid = %d/%d, child of %ld) - uid/gid inside allowed range", (long int)execPid, (int)puid, (int)pgid, (long int)ppid);
                        } else {
                          const char  *execCmd = GECODProcessReadCommForPid(execPid);
                          int         killStatus = kill(execPid, SIGKILL);
                          
                          // Kill this process immediately:
                          if ( killStatus == 0 ) {
                            GECO_WARN("pid %ld (uid/gid = %d/%d, child of %ld, %s) does not include job variables in environment; terminated (rc = %d)", (long int)execPid, (int)puid, (int)pgid, (long int)ppid, execCmd, killStatus);
                          } else {
                            GECO_WARN("pid %ld (uid/gid = %d/%d, child of %ld, %s) did not include job variables in environment; already terminated", (long int)execPid, (int)puid, (int)pgid, (long int)ppid, execCmd);
                          }
                        }
                      }
                    } else {
                      GECO_DEBUG("parent (%ld) of pid %ld is not being tracked", (long int)ppid, (long int)execPid);
                    }
                  } else {
                    GECO_DEBUG("failed to read info for pid %ld", (long int)execPid);
                  }
                }
              }
              if ( ! hasBeenHandled ) {
                if ( foundJobIds ) {
                  GECOJobRef    newJob = GECOJobCreateWithJobIdentifier(jobId, taskId);
                  
                  if ( newJob ) {
                    GECO_DEBUG("job wrapper created for %ld.%ld", jobId, taskId);
                    
                    if ( GECOJobGetReferenceCount(newJob) == 1 ) {
                      if ( GECOJobCGroupInit(newJob, GECODRunloop) ) {
                        GECO_DEBUG("cgroup init for %ld.%ld successful", jobId, taskId);
                      } else {
                        GECO_ERROR("cgroup init for %ld.%ld failed", jobId, taskId);
                      }
                    }
                    GECOPidMapAdd(execPid, jobId, taskId);
                    if ( GECOJobCGroupAddPid(newJob, execPid) ) {
                      GECO_DEBUG("added pid %ld to all cgroups for %ld.%ld", (long int)execPid, jobId, taskId);
                    } else {
                      int         killStatus = kill(execPid, SIGKILL);
                      
                      if ( killStatus == 0 ) {
                        GECO_ERROR("failed to add pid %ld to all cgroups for %ld.%ld; terminated (rc = %d)", (long int)execPid, jobId, taskId, (long int)execPid, killStatus);
                      } else {
                        GECO_ERROR("failed to add pid %ld to all cgroups for %ld.%ld; already terminated", (long int)execPid, jobId, taskId, (long int)execPid);
                      }
                      GECOJobRelease(newJob);
                      GECOPidMapRemovePid(execPid);
                    }
                  } else {
                    int         killStatus = kill(execPid, SIGKILL);
                    
                    if ( killStatus == 0 ) {
                      GECO_ERROR("unable to create job wrapper for %ld.%ld; terminated (rc = %d)", (long int)execPid, jobId, taskId, (long int)execPid, killStatus);
                    } else {
                      GECO_ERROR("unable to create job wrapper for %ld.%ld; already terminated", (long int)execPid, jobId, taskId, (long int)execPid);
                    }
                  }
                }
              }
              break;
            }
            
          
            //
            // A process called fork().
            //
            case PROC_EVENT_FORK: {
              pid_t             parentPid = event->event_data.fork.parent_pid;
              GECODProcessComm  parentProcComm = GECODProcessCommForPid(parentPid);
                
              GECO_DEBUG("fork event noted for pid %ld", (long int)parentPid);
              
              if ( parentProcComm == GECODProcessCommSSHD ) {
                GECO_INFO("speculatively adding pid %ld (forked child of sshd %ld) to tracking table", (long int)event->event_data.fork.child_pid, (long int)parentPid);
                GECODTrackedParentProcessesAdd(event->event_data.fork.child_pid);
              }
              break;
            }
            
          
            //
            // A process has exited.
            //
            case PROC_EVENT_EXIT: {
              long int          jobId = -1, taskId = 1;
              pid_t             exitPid = event->event_data.exit.process_pid;
                
              GECO_DEBUG("exit event noted for pid %ld", (long int)exitPid);
              
              // Something we're tracking?
              if ( GECODTrackedParentProcessesIsKnownPid(exitPid) ) {
                GECODTrackedParentProcessesRemove(exitPid);
                GECO_DEBUG("pid %ld removed from tracking table", (long int)exitPid);
              } else {
                // See if this process was registered as a Grid Engine job:
                if ( GECOPidMapGetJobAndTaskIdForPid(exitPid, &jobId, &taskId) ) {
                  GECOJobRef    theJob = GECOJobGetExistingObjectForJobIdentifier(jobId, taskId);
                  
                  if ( theJob ) GECOJobRelease(theJob);
                  GECOPidMapRemovePid(exitPid);
                }
              }
              break;
            }
            
            
          }
          break;
        }
      }
      nl_hdr = NLMSG_NEXT(nl_hdr, msgSize);
    }
  }
}

void
GECODNetlinkSocketDidReceiveClose(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  close(src->fd);
  src->fd = -1;
}

//

GECOPollingSourceCallbacks    GECODNetlinkSocketCallbacks = {
                                            .destroySource = GECODNetlinkSocketDestroySource,
                                            .fileDescriptorForPolling = GECODNetlinkSocketFileDescriptorForPolling,
                                            .shouldSourceClose = NULL,
                                            .willRemoveAsSource = NULL,
                                            .didAddAsSource = NULL,
                                            .didBeginPolling = NULL,
                                            .didReceiveDataAvailable = GECODNetlinkSocketDidReceiveDataAvailable,
                                            .didEndPolling = NULL,
                                            .didReceiveClose = GECODNetlinkSocketDidReceiveClose,
                                            .didRemoveAsSource = NULL
                                          };

//

int
GECODNetlinkSocketInit(
  GECODNetlinkSocket    *nlSocket
)
{
	struct sockaddr_nl    localNLAddr;
  int                   localSocket;
  int                   rc;
  ssize_t               count;
	struct nlmsghdr       *nl_hdr;
	struct cn_msg         *cn_hdr;
	enum proc_cn_mcast_op *mcop_msg;
  
  // Start with an invalid fd in the nlSocket:
  nlSocket->fd = -1;
  
  // Allocate a netlink socket:
  localSocket = socket(PF_NETLINK, SOCK_DGRAM, NETLINK_CONNECTOR);
	if (localSocket == -1) {
    GECO_ERROR("GECODInitNLSocket: unable to create netlink socket (errno = %d)", errno);
		return errno;
	}
  
  // Setup this process's netlink address:
	localNLAddr.nl_family   = AF_NETLINK;
	localNLAddr.nl_groups   = CN_IDX_PROC;
	localNLAddr.nl_pid      = getpid();
  
  // Bind the socket to our netlink address:
	rc = bind(localSocket, (struct sockaddr *)&localNLAddr, sizeof(localNLAddr));
	if (rc == -1) {
    GECO_ERROR("GECODInitNLSocket: unable to bind netlink socket to process address (errno = %d)", errno);
		close(localSocket);
    return errno;
	}
  
  // Using the msgBuffer, setup various structural pointers that make
  // up our netlink packet:
  nl_hdr = (struct nlmsghdr *)&nlSocket->msgBuffer;
  cn_hdr = (struct cn_msg *)NLMSG_DATA(nl_hdr);
	mcop_msg = (enum proc_cn_mcast_op*)&cn_hdr->data[0];
  
  // Zero-out all fields of the packet (by zeroing the msgBuffer):
  memset(&nlSocket->msgBuffer, 0, sizeof(nlSocket->msgBuffer));
  // Fill-in netlink header:
	nl_hdr->nlmsg_len = GECOD_SEND_MESSAGE_LEN;
	nl_hdr->nlmsg_type = NLMSG_DONE;
	nl_hdr->nlmsg_flags = 0;
	nl_hdr->nlmsg_seq = 0;
	nl_hdr->nlmsg_pid = getpid();
	// Fill-in the connector header:
	cn_hdr->id.idx = CN_IDX_PROC;
	cn_hdr->id.val = CN_VAL_PROC;
	cn_hdr->seq = 0;
	cn_hdr->ack = 0;
	cn_hdr->len = sizeof(enum proc_cn_mcast_op);
  // Fill-in the payload:
	*mcop_msg = GECOD_PROC_CN_MCAST_LISTEN;
  
  // Send the packet:
  count = send(localSocket, nl_hdr, nl_hdr->nlmsg_len, 0);
  if ( count != nl_hdr->nlmsg_len ) {
    GECO_ERROR("GECODInitNLSocket: unable to register netlink socket attributes (errno = %d)", errno);
    close(localSocket);
    return errno;
  }
    
  // The kernel's netlink address uses pid 1:
  localNLAddr.nl_family  = AF_NETLINK;
  localNLAddr.nl_groups  = CN_IDX_PROC;
  localNLAddr.nl_pid     = 1;
  
  if ( connect(localSocket, (struct sockaddr *)&localNLAddr, sizeof(localNLAddr)) != 0 ) {
    GECO_ERROR("GECODInitNLSocket: unable to connect netlink socket to kernel socket (errno = %d)", errno);
    close(localSocket);
    return errno;
  }
  GECO_INFO("netlink socket %d created and connected to kernel", localSocket);
  nlSocket->fd = localSocket;
  return 0;
}

//
#if 0
#pragma mark -
#endif
//

#include <getopt.h>

enum {
  GECODCliOptHelp             = 'h',
  GECODCliOptVerbose          = 'v',
  GECODCliOptQuiet            = 'q',
  GECODCliOptEnable           = 'e',
  GECODCliOptDisable          = 'd',
  GECODCliOptDaemonize        = 'D',
  GECODCliOptPidFile          = 'p',
  GECODCliOptLogFile          = 'l',
  GECODCliOptStartupRetry     = 'r',
  GECODCliOptAllowUid         = 'U',
  GECODCliOptAllowGid         = 'G',
  GECODCliOptStateDir         = 'S',
  GECODCliOptCGroupMountPoint = 'm',
  GECODCliOptCGroupSubGroup   = 's'
};

const char *GECODCliOptString = "hvqe:d:Dp:l?r:U:G:S:m:s:";

const struct option GECODCliOpts[] = {
                  { "help",                 no_argument,          NULL,         GECODCliOptHelp },
                  { "verbose",              no_argument,          NULL,         GECODCliOptVerbose },
                  { "quiet",                no_argument,          NULL,         GECODCliOptQuiet },
                  { "disable",              required_argument,    NULL,         GECODCliOptDisable },
                  { "enable",               required_argument,    NULL,         GECODCliOptEnable },
                  { "daemon",               no_argument,          NULL,         GECODCliOptDaemonize },
                  { "pidfile",              required_argument,    NULL,         GECODCliOptPidFile },
                  { "logfile",              optional_argument,    NULL,         GECODCliOptLogFile },
                  { "startup-retry",        required_argument,    NULL,         GECODCliOptStartupRetry },
                  { "allow-uid",            required_argument,    NULL,         GECODCliOptAllowUid },
                  { "allow-gid",            required_argument,    NULL,         GECODCliOptAllowGid },
                  { "state-dir",            required_argument,    NULL,         GECODCliOptStateDir },
                  { "cgroup-mountpoint",    required_argument,    NULL,         GECODCliOptCGroupMountPoint },
                  { "cgroup-subgroup",      required_argument,    NULL,         GECODCliOptCGroupSubGroup },
                  { NULL,                   0,                    0,             0  }
                };

//

void
usage(
  const char    *exe
)
{
  GECOCGroupSubsystem   subsysId;
  bool                  needComma = false;
  
  printf(
      "usage:\n\n"
      "  %s {options} [task-id]\n\n"
      " options:\n\n"
      "  --help/-h                          show this information\n"
      "  --verbose/-v                       increase the verbosity level (may be used\n"
      "                                       multiple times)\n"
      "  --quiet/-q                         decrease the verbosity level (may be used\n"
      "                                       multiple times)\n"
      "  --enable/-e <subsystem>            enable checks against the given cgroup\n"
      "                                       subsystem\n"
      "  --disable/-d <subsystem>           disable checks against the given cgroup\n"
      "                                       subsystem\n"
      "  --daemon/-D                        run as a daemon\n"
      "  --pidfile/-p <path>                file in which our pid should be written\n"
      "  --logfile/-l {<path>}              all logging should be written to <path>; if\n"
      "                                       <path> is omitted stderr is used\n"
      "  --state-dir/-S <path>              directory to which gecod should write resource\n"
      "                                       cache files, traces, etc.  The <path> should\n"
      "                                       be on a network filesystem shared between all\n"
      "                                       nodes in the cluster\n"
      "                                       (default: %s)\n"
      "  --cgroup-mountpoint/-m <path>      directory in which cgroup subsystems are\n"
      "                                       mounted (default: %s)\n"
      "  --cgroup-subgroup/-s <name>        specify the path (relative to the cgroup subgroups'\n"
      "                                       mount points) in which GECO will create per-job\n"
      "                                       subgroups (default: %s)\n"
      "  --startup-retry/-r #               if cgroup setup fails, retry this many\n"
      "                                       times; specify -1 for unlimited retries\n"
      "                                       (default: %d %s)\n"
      "  --allow-uid/-U (#|#-#){,(#|#-#)..} declare what uid numbers should be ignored\n"
      "                                       when checking for \"rogue\" processes\n"
      "  --allow-gid/-G (#|#-#){,(#|#-#)..} declare what gid numbers should be ignored\n"
      "                                       when checking for \"rogue\" processes\n"
      "\n"
      "  <subsystem> should be one of:\n\n"
      "    ",
      exe,
      GECOGetStateDir(),
      GECOCGroupGetPrefix(),
      GECOCGroupGetSubGroup(),
      GECODDefaultStartupRetryCount, (GECODDefaultStartupRetryCount == 1) ? "retry" : "retries"
    );
  
  subsysId = GECOCGroupSubsystem_min;
  while ( subsysId < GECOCGroupSubsystem_max ) {
    printf("%s%s", ( subsysId > GECOCGroupSubsystem_min ? ", " : "" ), GECOCGroupSubsystemToCString(subsysId));
    subsysId++;
  }
  printf(
      "\n"
      "\n"
      "  Subsystems enabled by default are:\n"
      "\n"
      "    "
    );
  subsysId = GECOCGroupSubsystem_min;
  while ( subsysId < GECOCGroupSubsystem_max ) {
    if ( GECOCGroupGetSubsystemIsManaged(subsysId) ) {
      printf("%s%s", ( needComma ? ", " : "" ), GECOCGroupSubsystemToCString(subsysId));
      needComma = true;
    }
    subsysId++;
  }
  printf(
      "\n"
      "\n"
      " $Id$\n"
      "\n"
    );
}

//
////
//

void
GECODHandleSignal(
  int     signo
)
{
  switch ( signo ) {
    
    case SIGTERM:
    case SIGINT: {
      GECORunloopSetShouldExitRunloop(GECODRunloop, true);
      break;
    }
  
  }
}

//

int
main(
  int                 argc,
  char                **argv
)
{
  const char          *exe = argv[0];
  int                 optch, optidx;
  
  int                 rc = 0;
  bool                isDaemon = false;
  char                *cgroupDir = NULL;
  char                *cgroupSubGroup = NULL;
  char                *stateDir = NULL;
  char                *pidFile = NULL;
  char                *logFile = NULL;
  int                 startupRetryCount = GECODDefaultStartupRetryCount;
  
  GECODNetlinkSocket  nlSocket;
  
  if ( getuid() != 0 ) {
		fprintf(stderr, "ERROR:  %s must be run as root\n", exe);
		return EPERM;
	}
  
  // Create uid/gid allow sets:
  if ( ! (GECODAllowedUids = GECOIntegerSetCreate()) ) {
    fprintf(stderr, "ERROR:  unable to allocate allowed uid integer set\n");
    return ENOMEM;
  }
  if ( ! (GECODAllowedGids = GECOIntegerSetCreate()) ) {
    fprintf(stderr, "ERROR:  unable to allocate allowed gid integer set\n");
    return ENOMEM;
  }
  GECOIntegerSetAddIntegerRange(GECODAllowedUids, 0, 499);
  GECOIntegerSetAddIntegerRange(GECODAllowedGids, 0, 499);
  
  // Init libxml:
  xmlInitParser();
  LIBXML_TEST_VERSION
  
  // Check for arguments:
  while ( (optch = getopt_long(argc, argv, GECODCliOptString, GECODCliOpts, &optidx)) != -1 ) {
    GECOIntegerSetRef   *whichSet = NULL;
    
    switch ( optch ) {
      
      case GECODCliOptHelp: {
        usage(exe);
        exit(0);
      }
      
      case GECODCliOptVerbose: {
        GECOLogIncLevel(GECOLogDefault);
        break;
      }
      
      case GECODCliOptQuiet: {
        GECOLogDecLevel(GECOLogDefault);
        break;
      }
      
      case GECODCliOptEnable:
      case GECODCliOptDisable: {
        if ( optarg ) {
          GECOCGroupSubsystem   subsysId = GECOCGroupCStringToSubsystem(optarg);
          
          if ( subsysId != GECOCGroupSubsystem_invalid ) {
            GECOCGroupSetSubsystemIsManaged(subsysId, (optch == GECODCliOptEnable) ? true : false);
          } else {
            fprintf(stderr, "ERROR:  invalid cgroup subsystem specified: %s\n", optarg);
            exit(EINVAL);
          }
        }
        break;
      }
      
      case GECODCliOptDaemonize: {
        isDaemon = true;
        break;
      }
        
      case GECODCliOptPidFile: {
        if ( optarg && *optarg ) {
          pidFile = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --pidfile/-p option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptLogFile: {
        if ( optarg && *optarg ) logFile = optarg;
        break;
      }
      
      case GECODCliOptStateDir: {
        if ( optarg && *optarg ) {
          stateDir = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --state-dir/-S option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptCGroupMountPoint: {
        if ( optarg && *optarg ) {
          cgroupDir = optarg;
        } else {
          fprintf(stderr, "ERROR:  no path provided with --cgroup-mountpoint/-c option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptCGroupSubGroup: {
        if ( optarg && *optarg ) {
          cgroupSubGroup = optarg;
        } else {
          fprintf(stderr, "ERROR:  no name provided with --cgroup-subgroup/-s option\n");
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptStartupRetry: {
        int     tmpInt;
        
        if ( optarg && *optarg && GECO_strtoi(optarg, &tmpInt, NULL) ) {
          startupRetryCount = (tmpInt < 0) ? -1 : tmpInt;
        } else {
          fprintf(stderr, "ERROR:  invalid value provided with --startup-retry/-S: %s\n", optarg);
          exit(EINVAL);
        }
        break;
      }
      
      case GECODCliOptAllowUid:
        whichSet = &GECODAllowedUids;
      case GECODCliOptAllowGid: {
        if ( optarg && *optarg ) {
          int           low, high;
          const char    *startPtr = optarg, *endPtr;
          
          if ( ! whichSet ) whichSet = &GECODAllowedGids;
          
          while ( 1 ) {
            if ( ! startPtr || ! *startPtr ) break;
            
            if ( GECO_strtoi(startPtr, &low, &endPtr) ) {
              switch ( *endPtr ) {
              
                case ',':
                  endPtr++;
                case '\0':
                  high = low;
                  break;
                  
                case '-': {
                  if ( *(endPtr + 1) == '\0' ) {
                    high = INT_MAX;
                  }
                  else if ( GECO_strtoi(endPtr + 1, &high, &endPtr) ) {
                    if ( high < low ) {
                      fprintf(stderr, "ERROR:  invalid range specified with --allow-%s/-%c: %d-%d\n", (optch == GECODCliOptAllowGid) ? "gid" : "uid", optch, low, high);
                      exit(EINVAL);
                    }
                    if ( *endPtr == ',' ) endPtr++;
                  } else {
                    fprintf(stderr, "ERROR:  invalid range specified with --allow-%s/-%c: %s\n", (optch == GECODCliOptAllowGid) ? "gid" : "uid", optch, endPtr + 1);
                    exit(EINVAL);
                  }
                  break;
                }
                
                default:
                  fprintf(stderr, "ERROR:  invalid range specified with --allow-%s/-%c: %s\n", (optch == GECODCliOptAllowGid) ? "gid" : "uid", optch, endPtr + 1);
                  exit(EINVAL);
              }
              GECOIntegerSetAddIntegerRange(*whichSet, low, high);
            } else {
              fprintf(stderr, "ERROR:  invalid value specified with --allow-%s/-%c: %s\n", (optch == GECODCliOptAllowGid) ? "gid" : "uid", optch, startPtr);
              exit(EINVAL);
            }
            startPtr = endPtr;
          }
        } else {
          fprintf(stderr, "ERROR:  a value must be specified with --allow-%s/-%c\n", (optch == GECODCliOptAllowGid) ? "gid" : "uid", optch);
          exit(EINVAL);
        }
        break;
      }

    }
  }
  
  if ( isDaemon ) {
    if ( daemon(0, 1) != 0 ) {
      fprintf(stderr, "ERROR:  unable to daemonize (errno = %d)", errno);
      exit(EINVAL);
    }
  }
  
  // Setup signal handling:
  signal(SIGHUP, SIG_IGN);
  signal(SIGTERM, GECODHandleSignal);
  signal(SIGINT, GECODHandleSignal);
  
  // Open the pid file if one was requested:
  if ( pidFile ) {
    FILE    *fptr = fopen(pidFile, "w");
    
    if ( fptr ) {
      fprintf(fptr, "%lld", (long long int)getpid());
      fclose(fptr);
    } else {
      fprintf(stderr, "ERROR:  unable to write to pidfile %s\n", pidFile);
    }
  }
  
  // Turn the UID/GID lists into constants, possibly saving lots of memory:
  GECOIntegerSetRef   tmp;
  
  tmp = GECOIntegerSetCreateConstantCopy(GECODAllowedUids);
  GECOIntegerSetDestroy(GECODAllowedUids); GECODAllowedUids = tmp;
  
  tmp = GECOIntegerSetCreateConstantCopy(GECODAllowedGids);
  GECOIntegerSetDestroy(GECODAllowedGids); GECODAllowedGids = tmp;
  
  // Get the log file opened if an alternate path was requested:
  if ( logFile ) {
    if ( ! freopen(optarg, "a", stderr) ) {
      fprintf(stderr, "ERROR:  unable to open logfile %s\n", optarg);
      rc = errno;
      goto gecod_earlyExit;
    }
  }
  
  // Send stdin and stdout to /dev/null like any well-behaved daemon:
  if ( isDaemon ) {
    freopen("/dev/null", "r", stdin);
    freopen("/dev/null", "w", stdout);
  }
  
  // Get the GECO shared state directory ready to roll:
  if ( ! GECOSetStateDir(stateDir) ) {
    GECO_ERROR("unable to setup state directory %s (errno = %d)\n", ( stateDir ? stateDir : GECOGetStateDir() ), errno);
    rc = errno;
    goto gecod_earlyExit;
  }
  
  // Make sure the cgroup mountpoint container is ready:
  if ( ! GECOCGroupSetPrefix(cgroupDir) ) {
    GECO_ERROR("unable to setup cgroup prefix directory %s (errno = %d)\n", ( cgroupDir ? cgroupDir : GECOCGroupGetPrefix() ), errno);
    rc = errno;
    goto gecod_earlyExit;
  }
  
  // Validate the cgroup subgroup name if one was explicitly provided on the CLI:
  if ( cgroupSubGroup && ! GECOCGroupSetSubGroup(cgroupSubGroup) ) {
    GECO_ERROR("invalid cgroup subgroup %s (errno = %d)\n", cgroupSubGroup, errno);
    rc = errno;
    goto gecod_earlyExit;
  }

retry_cgroup_init:

  // Try to get the GECO cgroup subgroups created:
  if ( ! GECOCGroupInitSubsystems() ) {
    if ( startupRetryCount != 0 ) {
      if ( startupRetryCount > 0 ) startupRetryCount--;
      GECO_WARN("failed to complete initial setup of GECO cgroup subgroups...retrying in 15 seconds");
      sleep(15);
      goto retry_cgroup_init;
    } else {
      GECO_ERROR("failed to complete initial setup of GECO cgroup subgroups.");
      rc = EPERM;
      goto gecod_earlyExit;
    }
  }
  
  // Setup the netlink socket:
  if ( (rc = GECODNetlinkSocketInit(&nlSocket)) == 0 ) {
    // Create the runloop:
    GECODRunloop = GECORunloopCreate();
    if ( GECODRunloop ) {
      GECO_DEBUG("created runloop");
      
      // Initialize the job management component:
      GECOJobInit();
      GECO_DEBUG("initialized job management");
      
      // Add the netlink socket to the runloop:
      GECORunloopAddPollingSource(GECODRunloop, &nlSocket, &GECODNetlinkSocketCallbacks, 0);
      GECO_DEBUG("netlink polling source added to runloop");
     
      // Run until something says we're done:
      GECO_DEBUG("entering runloop");
      rc = GECORunloopRun(GECODRunloop);
      
      // Deinitialize the job management component:
      GECOJobDeinit();
      GECO_DEBUG("shutting down job management");
      
      // All done with our runloop:
      GECO_DEBUG("destroying runloop");
      GECORunloopDestroy(GECODRunloop);
    }
    
    if ( nlSocket.fd >= 0 ) {
      // Drop our netlink socket:
      GECO_DEBUG("closing netlink socket %d", nlSocket);
      close(nlSocket.fd);
    }
  }
  
  // Shutdown all GECO cgroup subgroups:
  GECOCGroupShutdownSubsystems();

gecod_earlyExit:
  // Shutdown libxml:
  xmlCleanupParser();
  
  // Drop the pid file if we had one:
  if ( pidFile ) unlink(pidFile);
  
  return rc;
}

/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOExecWrapper.c
 *  
 *  Wrappers to the exec*() suite of functions to facilitate
 *  pre-execution setup of cgroups and quarantine 
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECO.h"
#include "GECOLog.h"
#include "GECOIntegerSet.h"
#include "GECOQuarantine.h"

#include "confuse.h"

#include <signal.h>
#include <pwd.h>
#include <grp.h>

#define __USE_GNU
#include <dlfcn.h>

//

void
GECOExecWrapperTmpDebug(
  const char    *format,
  ...
)
{
#ifdef GECOEXECWRAPPER_TMP_DEBUG
  va_list       varg;
  FILE          *fptr =fopen("/tmp/GECOExecWrapperDebug.log", "a");
  
  if ( fptr ) {
    va_start(varg, format);
    vfprintf(fptr, format, varg);
    va_end(varg);
    fclose(fptr);
  }
#endif
}

void
GECOExecWrapperTmpDebugIntegerSet(
  GECOIntegerSetRef   anIntSet
)
{
#ifdef GECOEXECWRAPPER_TMP_DEBUG
  FILE          *fptr =fopen("/tmp/GECOExecWrapperDebug.log", "a");
  
  if ( fptr ) {
    GECOIntegerSetDebug(anIntSet, fptr); fprintf(fptr, "\n");
    fclose(fptr);
  }
#endif
}

//

#ifndef HAVE_EXECVPE

int execvpe(const char *file, char *const argv[], char *const envp[]);
     
#endif

#ifndef GECO_STATIC_ARGV_SIZE
# define GECO_STATIC_ARGV_SIZE 1024
#endif

//

#ifndef GECO_LDPRELOAD_VALUE
#error GECO_LDPRELOAD_VALUE macro must be configured
#endif

const char                *GECOExecWrapperLDPreloadValue = "LD_PRELOAD="GECO_LDPRELOAD_VALUE;

#ifndef GECOD_QUARANTINE_SOCKET
#define GECOD_QUARANTINE_SOCKET     "path:/tmp/gecod_quarantine"
#endif

static const char         *GECODDefaultQuarantineSocket = GECOD_QUARANTINE_SOCKET;

//

static bool               GECOExecWrapperIsInited = false;

static cfg_bool_t         GECOExecWrapperShouldQuarantineSSH = cfg_true;

static GECOIntegerSetRef  GECOExecWrapperAllowedUids = NULL;
static GECOIntegerSetRef  GECOExecWrapperAllowedGids = NULL;

static uid_t              GECOExecWrapperExecdUser = -1;

static GECOLogLevel       GECOExecWrapperLogLevel = GECOLogLevelQuiet;
static int                GECOExecWrapperLogFileModeMask = 0644;
static const char         *GECOExecWrapperLogPathFormat = NULL;

static const char         *GECOExecWrapperQuarantineSocketAddr = NULL;
static unsigned int       GECOExecWrapperQuarantineSendTimeout;
static unsigned int       GECOExecWrapperQuarantineRecvTimeout;
static unsigned int       GECOExecWrapperQuarantineRetryCount;

//

const char*
GECOExecWrapperReadCommForPid(
  pid_t               aPid
)
{
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

GECOLogRef
GECOExecWrapperOpenLogFile(
  const char      *commName
)
{
  if ( GECOExecWrapperLogPathFormat ) {
    char          path[PATH_MAX];
    int           pathLen = 0;
    const char    *s = GECOExecWrapperLogPathFormat;
    
    while ( *s && (pathLen < sizeof(path)) ) {
      if ( *s == '$' ) {
        if ( strncmp(s, "${COMMAND}", strlen("${COMMAND}")) == 0 ) {
          s += strlen("${COMMAND}");
          pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%s", commName);
          continue;
        }
        else if ( strncmp(s, "${PARENT_COMMAND}", strlen("${PARENT_COMMAND}")) == 0 ) {
          s += strlen("${PARENT_COMMAND}");
          const char    *parentCommand = GECOExecWrapperReadCommForPid(getppid());
          
          if ( parentCommand ) {
            pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%s", parentCommand);
          }
          continue;
        }
        else if ( strncmp(s, "${TIME}", strlen("${TIME}")) == 0 ) {
          s += strlen("${TIME}");
          pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%.0lf", (double)time(NULL));
          continue;
        }
        else if ( strncmp(s, "${PID}", strlen("${PID}")) == 0 ) {
          s += strlen("${PID}");
          pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%d", getpid());
          continue;
        }
        else if ( strncmp(s, "${UID}", strlen("${UID}")) == 0 ) {
          s += strlen("${UID}");
          pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%d", getuid());
          continue;
        }
        else if ( strncmp(s, "${GID}", strlen("${GID}")) == 0 ) {
          s += strlen("${GID}");
          pathLen += snprintf(path + pathLen, sizeof(path) - pathLen, "%d", getgid());
          continue;
        }
      }
      path[pathLen++] = *s;
      s++;
    }
    if ( pathLen > 0 && pathLen < sizeof(path) - 1 ) {
      GECOLogRef      logFile = NULL;
      
      path[pathLen] = '\0';
      logFile = GECOLogCreateWithFilePath(GECOExecWrapperLogLevel, path);
      if ( logFile ) {
        chmod(path, GECOExecWrapperLogFileModeMask);
        GECOLogSetDefault(logFile);
        return logFile;
      }
    }
  }
  return NULL;
}

//

static inline bool
GECOExecWrapperIsUidWhitelisted(
  uid_t     theUid
)
{
  GECOExecWrapperTmpDebug("GECOExecWrapperIsUidWhitelisted\n");
  if ( GECOExecWrapperAllowedUids ) {
    GECOExecWrapperTmpDebug("GECOExecWrapperAllowedUids =\n");
    GECOExecWrapperTmpDebugIntegerSet(GECOExecWrapperAllowedUids);
    GECOExecWrapperTmpDebug("%d in set => %d\n", theUid, (int)GECOIntegerSetContains(GECOExecWrapperAllowedUids, theUid));
    GECOExecWrapperTmpDebug("=> => %d\n", ( GECOExecWrapperAllowedUids && GECOIntegerSetContains(GECOExecWrapperAllowedUids, theUid) ) ? true : false);
  } else {
    GECOExecWrapperTmpDebug("GECOExecWrapperAllowedUids is not defined?\n");
  }
  return ( GECOExecWrapperAllowedUids && GECOIntegerSetContains(GECOExecWrapperAllowedUids, theUid) ) ? true : false;
}

static inline bool
GECOExecWrapperIsGidWhitelisted(
  gid_t     theGid
)
{
  GECOExecWrapperTmpDebug("GECOExecWrapperIsGidWhitelisted\n");
  if ( GECOExecWrapperAllowedGids ) {
    GECOExecWrapperTmpDebug("GECOExecWrapperAllowedGids =\n");
    GECOExecWrapperTmpDebugIntegerSet(GECOExecWrapperAllowedGids);
  } else {
    GECOExecWrapperTmpDebug("GECOExecWrapperAllowedGids is not defined?\n");
  }
  return ( GECOExecWrapperAllowedGids && GECOIntegerSetContains(GECOExecWrapperAllowedGids, theGid) ) ? true : false;
}

//

int
__GECOExecWrapperCfgParseLogLevelCallback(
  cfg_t       *cfg,
  cfg_opt_t   *opt,
  const char  *value,
  void        *result
)
{
  long int    *outValue = (long int*)result;
  
  if ( (*value == '\0') || (strcasecmp(value, "NONE") == 0) )
    *outValue = GECOLogLevelQuiet;
  else if ( strcasecmp(value, "ERROR") == 0 )
    *outValue = GECOLogLevelError;
  else if ( strcasecmp(value, "WARN") == 0 )
    *outValue = GECOLogLevelWarn;
  else if ( strcasecmp(value, "INFO") == 0 )
    *outValue = GECOLogLevelInfo;
  else if ( strcasecmp(value, "DEBUG") == 0 )
    *outValue = GECOLogLevelDebug;
  else
    return -1;
    
  return 0;
}

bool
GECOExecWrapperInit(void)
{
  char                path[PATH_MAX];
  int                 pathLen;
  
  cfg_opt_t           cfg_logging_opts[] = {
                          CFG_STR("path", NULL, CFGF_NODEFAULT),
                          CFG_INT_CB("level", GECOLogLevelQuiet, CFGF_NONE, __GECOExecWrapperCfgParseLogLevelCallback),
                          CFG_INT("mode", 0644, CFGF_NONE),
                          CFG_END()
                        };
  cfg_opt_t           cfg_whitelist_opts[] = {
                          CFG_INT_LIST("uids", NULL, CFGF_NODEFAULT ),
                          CFG_INT_LIST("gids", NULL, CFGF_NODEFAULT ),
                          CFG_END()
                        };
  cfg_opt_t           cfg_quarantine_opts[] = {
                          CFG_STR("socket", (char*)GECODDefaultQuarantineSocket, CFGF_NONE ),
                          CFG_INT("send_timeout", 60, CFGF_NONE ),
                          CFG_INT("recv_timeout", 60, CFGF_NONE ),
                          CFG_INT("retry", 2, CFGF_NONE ),
                          CFG_END()
                        };
  cfg_opt_t           cfg_opts[] = {
                          CFG_SEC("whitelist", cfg_whitelist_opts, CFGF_NONE ),
                          CFG_SEC("quarantine", cfg_quarantine_opts, CFGF_NONE ),
                          CFG_INT("sge_execd_uid", -1, CFGF_NODEFAULT),
                          CFG_SEC("logging", cfg_logging_opts, CFGF_NONE),
                        	CFG_SIMPLE_BOOL("enable_sshd_quarantine", &GECOExecWrapperShouldQuarantineSSH),
                          CFG_END()
                        };
  GECOIntegerSetRef   uidSet = NULL, gidSet = NULL;
  
  GECOExecWrapperTmpDebug("%d:%d enter\n", getpid(), getppid());
  
  // Create uid/gid allow sets:
  if ( ! (uidSet = GECOIntegerSetCreate()) ) {
    errno = ENOMEM;
    return false;
  }
  GECOExecWrapperTmpDebug("%d:%d uidset created\n", getpid(), getppid());
  if ( ! (gidSet = GECOIntegerSetCreate()) ) {
    errno = ENOMEM;
    GECOIntegerSetDestroy(uidSet);
    return false;
  }

  GECOExecWrapperTmpDebug("%d:%d gidset created\n", getpid(), getppid());
  GECOIntegerSetAddIntegerRange(uidSet, 0, 499);
  GECOIntegerSetAddIntegerRange(gidSet, 0, 499);
  
  // Check for our configuration file:
  pathLen = snprintf(path, sizeof(path), "%s/geco-preload-lib.conf", GECODirectoryEtc);
  if ( pathLen > 0 && pathLen < sizeof(path) && GECOIsFile(path) ) {
    cfg_t               *cfg = cfg_init(cfg_opts, CFGF_NONE);
    
    if ( cfg ) {
      int               rc = cfg_parse(cfg, path);
      
      if ( rc == CFG_SUCCESS ) {
        unsigned int    i, iMax;
        long int        v;
        
        // Add any whitelisted uids/gids:
        cfg_t           *whitelistCfg = cfg_getsec(cfg, "whitelist");
        
        if ( whitelistCfg ) {
          i = 0; iMax = cfg_size(whitelistCfg, "uids");
          while ( i < iMax ) {
            GECOExecWrapperTmpDebug("%d added to uidset\n", cfg_getnint(whitelistCfg, "uids", i));
            GECOIntegerSetAddInteger(uidSet, cfg_getnint(whitelistCfg, "uids", i++));
          }
          
          // Add any whitelisted gids that were specified:
          i = 0; iMax = cfg_size(whitelistCfg, "gids");
          while ( i < iMax ) {
            GECOExecWrapperTmpDebug("%d added to gidset\n", cfg_getnint(whitelistCfg, "gids", i));
            GECOIntegerSetAddInteger(gidSet, cfg_getnint(whitelistCfg, "gids", i++));
          }
        }
        
        // Quarantine items::
        cfg_t           *quarantineCfg = cfg_getsec(cfg, "quarantine");
        
        if ( quarantineCfg ) {
          char          *socketAddr = cfg_getstr(quarantineCfg, "socket");
          
          if ( socketAddr ) GECOExecWrapperQuarantineSocketAddr = strdup(socketAddr);
          
          v = cfg_getint(quarantineCfg, "send_timeout");
          if ( v >= 0 && v < UINT_MAX ) GECOExecWrapperQuarantineSendTimeout = v;
          
          v = cfg_getint(quarantineCfg, "recv_timeout");
          if ( v >= 0 && v < UINT_MAX ) GECOExecWrapperQuarantineRecvTimeout = v;
          
          v = cfg_getint(quarantineCfg, "retry");
          if ( v >= 0 && v < UINT_MAX ) GECOExecWrapperQuarantineRetryCount = v;
        }
        
        // What uid is sge_execd expected to run as?
        if ( (v = cfg_getint(cfg, "sge_execd_uid")) >= 0 ) {
          if ( v < INT_MAX ) GECOExecWrapperExecdUser = v;
        }
        
        // Should we setup some logging?
        cfg_t           *loggingCfg = cfg_getsec(cfg, "logging");
        
        if ( loggingCfg ) {
          GECOLogLevel  logLevel = cfg_getint(loggingCfg, "level");
          
          if ( logLevel > GECOLogLevelQuiet ) {
            GECOExecWrapperLogLevel = logLevel;
            
            // Check for a file path format string:
            char        *pathFormat = cfg_getstr(loggingCfg, "path");
            
            if ( pathFormat && *pathFormat && (strcasecmp(pathFormat, ":stderr:") != 0) ) {
              GECOExecWrapperLogPathFormat = strdup(pathFormat);
              
              GECOExecWrapperLogFileModeMask = cfg_getint(loggingCfg, "mode");
            }
          }
        }
      }
      cfg_free(cfg);
    }
  }
  
  GECOExecWrapperAllowedUids = GECOIntegerSetCreateConstantCopy(uidSet); GECOIntegerSetDestroy(uidSet);
  GECOExecWrapperAllowedGids = GECOIntegerSetCreateConstantCopy(gidSet); GECOIntegerSetDestroy(gidSet);
  
  GECOExecWrapperIsInited = true;
  
  return true;
}

//
#if 0
#pragma mark -
#endif
//

bool
GECOExecWrapperEnvironmentHasLDPRELOAD(
  char *const   *inEnvP
)
{
  char          **p = (char**)inEnvP;
  
  while ( *p ) {
    if ( strstr(*p, "LD_PRELOAD=") == *p ) return true;
    p++;
  }
  return false;
}

//

bool
GECOExecWrapperEnvironmentAddLDPRELOAD(
  char *const   *inEnvP,
  char *const   **outEnvP
)
{
  char          **p, **drop = NULL;
  int           count = 0;
  
  // We want to remove the LD_PRELOAD variable if present:
  p = (char**)inEnvP;
  while ( *p ) {
    if ( strstr(*p, "LD_PRELOAD=") == *p ) {
      if ( strcmp(*p + strlen("LD_PRELOAD="), GECOExecWrapperLDPreloadValue) == 0 ) return true;
      drop = p;
    } else {
      count++;
    }
    p++;
  }
  
  char*         *newList = malloc((count + ( drop ? 1 : 2 )) * sizeof(char *const));
  
  if ( newList ) {
    *outEnvP = newList;
    p = (char**)inEnvP;
    while ( *p ) {
      if ( p != drop ) *newList++ = *p;
      p++;
    }
    *newList++ = (char*)GECOExecWrapperLDPreloadValue;
    *newList = NULL;
    return true;
  }
  errno = ENOMEM;
  return false;
}

//

bool
GECOExecWrapperEnvironmentRemoveLDPRELOAD(
  char *const   *inEnvP,
  char *const   **outEnvP
)
{
  char          **p, **drop = NULL;
  int           count = 0;
  
  // We want to remove the LD_PRELOAD variable if present:
  p = (char**)inEnvP;
  while ( *p ) {
    if ( strstr(*p, "LD_PRELOAD=") == *p ) {
      drop = p;
    } else {
      count++;
    }
    p++;
  }
  if ( count && drop ) {
    char*     *newList = malloc((count + 1) * sizeof(char *const));
    
    if ( newList ) {
      *outEnvP = newList;
      p = (char**)inEnvP;
      while ( *p ) {
        if ( p != drop ) *newList++ = *p;
        p++;
      }
      *newList = NULL;
      return true;
    } else {
      errno = ENOMEM;
      return false;
    }
  }
  *outEnvP = NULL;
  return true;
}

//

typedef enum {
  GECOExecWrapperProcessCommSGEExecd = 0,
  GECOExecWrapperProcessCommSGEShepherd,
  GECOExecWrapperProcessCommSSHD,
  GECOExecWrapperProcessCommUnhandled
} GECOExecWrapperProcessComm;

const char* GECOExecWrapperProcessCommString[] = {
                  "sge_execd",
                  "sge_shepherd",
                  "sshd"
                };

GECOExecWrapperProcessComm
GECOExecWrapperProcessCommForPid(
  pid_t               aPid
)
{
  GECOExecWrapperProcessComm      procComm = GECOExecWrapperProcessCommUnhandled;
  char                            comm[64];
  int                             commFd;
  
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
      
      procComm = GECOExecWrapperProcessCommSGEShepherd;
      while ( procComm < GECOExecWrapperProcessCommUnhandled ) {
        if ( (count == strlen(GECOExecWrapperProcessCommString[procComm])) && (strncmp(comm, GECOExecWrapperProcessCommString[procComm], count) == 0) ) break;
        procComm++;
      }
    }
  }
  return procComm;
}

//

bool
GECOExecWrapperQuarantine(
  const char          *nextExec,
  char *const         *inEnvP,
  char *const         **outEnvP
)
{
  if ( ! GECOExecWrapperIsInited ) exit(EINVAL);
  
  // Isolate the basename of the command we wish to exec:
  GECOExecWrapperProcessComm    parentCommand = GECOExecWrapperProcessCommForPid(getppid());
  long int                      jobId = GECOUnknownJobId, taskId = GECOUnknownJobId;
  const char                    *nextCommand = nextExec + strlen(nextExec);
  
  while ( nextCommand > nextExec ) {
    nextCommand--;
    if ( *nextCommand == '/' ) {
      nextCommand++;
      break;
    }
  }
  
  // Logging, please:
  GECOLogRef                    defaultLog = NULL;
  bool                          rc = false;
  
  // What kind of process was my parent?
  switch ( parentCommand ) {
    
    case GECOExecWrapperProcessCommSGEExecd: {
      defaultLog = GECOExecWrapperOpenLogFile(nextCommand);
      GECO_INFO("exec daemon is launching a child, adding-back LD_PRELOAD");
      rc = GECOExecWrapperEnvironmentAddLDPRELOAD(inEnvP, outEnvP);
      break;
    }
  
    case GECOExecWrapperProcessCommSGEShepherd: {
      defaultLog = GECOExecWrapperOpenLogFile(nextCommand);
      rc = true;
      
      // Anything with uid 0 (root) should be given an automatic pass:
      if ( getuid() == 0 ) {
        GECO_INFO("no quarantine for child process '%s' (%ld) of shepherd(%ld) running as root", nextCommand, getpid(), getppid());
        rc = true;
        goto early_exit;
      }
      
      // If an sgeadm uid was provided, be sure the parent matches that:
      if ( GECOExecWrapperExecdUser >= 0 ) {
        uid_t       parentUid;
        
        if ( GECOGetPidInfo(getppid(), NULL, &parentUid, NULL, NULL) ) {
          if ( parentUid != GECOExecWrapperExecdUser ) {
            GECO_ERROR("uid(%d) of parent process pid(%ld) does not match expected uid(%d)",
                parentUid, getppid(),
                GECOExecWrapperExecdUser
              );
            rc = false;
          }
        } else {
          GECO_ERROR("unable to get process info for parent pid(%ld)", getppid());
          rc = false;
        }
      }
      break;
    }
    
    case GECOExecWrapperProcessCommSSHD: {
      defaultLog = GECOExecWrapperOpenLogFile(nextCommand);
      
      // If what we want to exec is also an sshd, then ensure the environment
      // will have our LDPRELOAD and allow it to happen:
      if ( strcmp(nextCommand, "sshd") == 0 ) {
        GECO_INFO("executing child sshd(%d) of sshd(%d)", getpid(), getppid());
        rc = GECOExecWrapperEnvironmentAddLDPRELOAD(inEnvP, outEnvP);
        goto early_exit;
      }
      
      rc = true;
      
      // Are we doing quarantine?
      if ( GECOExecWrapperShouldQuarantineSSH != cfg_true ) {
        GECO_DEBUG("sshd quarantine is disabled");
        goto early_exit;
      }
        
      // Is it a user who's whitelisted?
      if ( GECOExecWrapperIsUidWhitelisted(getuid()) ) {
        GECO_INFO("sshd running as uid(%d) is whitelisted", getuid());
        goto early_exit;
      }
      
      // Is it a group that's directly whitelisted?
      if ( GECOExecWrapperIsGidWhitelisted(getgid()) ) {
        GECO_INFO("sshd running as gid(%d) is whitelisted", getgid());
        goto early_exit;
      }
      
      // Check all groups of which the user is a member:
      struct passwd     *passwdRec = getpwuid(getuid());
      
      if ( passwdRec ) {
        int             ngroups = 0;
        
        if ( (getgrouplist(passwdRec->pw_name, passwdRec->pw_gid, NULL, &ngroups) == -1) && (ngroups > 1) ) {
          gid_t         groupList[ngroups];
          
          if ( getgrouplist(passwdRec->pw_name, passwdRec->pw_gid, groupList, &ngroups) != -1 ) {
            while ( ngroups-- > 0 ) {
              if ( GECOExecWrapperIsGidWhitelisted(groupList[ngroups]) ) {
                GECO_INFO("sshd running as uid(%d) is member of whitelisted gid(%d)", getuid(), groupList[ngroups]);
                goto early_exit;
              }
            }
          }
        }
      }
      break;
    }
    
    default: {
      GECO_INFO("whitelisting pid(%ld) with command %s", (long int)getpid(), nextCommand);
      rc = true;
      goto early_exit;
    }
  
  }

  if ( rc ) {
    // Check the target environment for GE job identifiers:
    if ( inEnvP ) {
      char *const     *p = inEnvP;
      
      while ( *p ) {
        if ( strstr(*p, "JOB_ID=") == *p ) {
          GECO_strtol(*p + 7, &jobId, NULL);
        }
        else if ( strstr(*p, "SGE_TASK_ID=") == *p ) {
          GECO_strtol(*p + 12, &taskId, NULL);
        }
        p++;
      }
    }
    if ( jobId == GECOUnknownJobId ) {
      GECO_WARN("Command '%s' with pid %ld does not contain GE job id in environment.  Execution forbidden.", nextCommand, (long int)getpid());
      errno = EPERM;
      rc = false;
      goto early_exit;
    }
    if ( taskId == GECOUnknownJobId ) {
      GECO_INFO("Implicit task id 1 used for command '%s' with pid %ld and job id %ld.", nextCommand, (long int)getpid(), jobId);
      taskId = 1;
    }
    
    // Now we've got the job id.  Let's try to inform gecod:
    GECOQuarantineSocket      theSocket;
    
    rc = GECOQuarantineSocketOpenClient(
                GECOQuarantineSocketTypeInferred,
                ( GECOExecWrapperQuarantineSocketAddr ? GECOExecWrapperQuarantineSocketAddr : GECODDefaultQuarantineSocket ),
                GECOExecWrapperQuarantineRetryCount,
                GECOExecWrapperQuarantineRecvTimeout,
                GECOExecWrapperQuarantineSendTimeout,
                &theSocket
              );
    if ( rc ) {
      GECOQuarantineCommandRef  quarantineCommand = GECOQuarantineCommandJobStartedCreate(jobId, taskId, getpid());
      
      if ( quarantineCommand ) {
        rc = GECOQuarantineSocketSendCommand(&theSocket, quarantineCommand);
        GECOQuarantineCommandDestroy(quarantineCommand);
        quarantineCommand = NULL;
        if ( rc ) {
          rc = GECOQuarantineSocketRecvCommand(&theSocket, &quarantineCommand);
          if ( rc ) {
            if ( GECOQuarantineCommandGetCommandId(quarantineCommand) == GECOQuarantineCommandIdAckJobStarted ) {
              long int      ackJobId = GECOQuarantineCommandAckJobStartedGetJobId(quarantineCommand);
              long int      ackTaskId = GECOQuarantineCommandAckJobStartedGetTaskId(quarantineCommand);
              
              if ( ackJobId == jobId && ackTaskId == taskId ) {
                rc = GECOQuarantineCommandAckJobStartedGetSuccess(quarantineCommand);
                GECO_INFO("Received acknowledgement from gecod:  job %ld.%ld (pid %ld) was%s quarantined",
                      jobId, taskId, (long int)getpid(),
                      ( rc ? "" : " not" )
                    );
              } else {
                GECO_ERROR("Expected job-started acknowledgement for %ld.%ld (pid %ld), got acknowledgement for %ld.%ld", jobId, taskId, (long int)getpid(), ackJobId, ackTaskId);
                rc = false;
              }
            } else {
              GECO_ERROR("Expected job-started acknowledgement for %ld.%ld (pid %ld), got wrong command (%u) from server", jobId, taskId, (long int)getpid(),
                  GECOQuarantineCommandGetCommandId(quarantineCommand)
                );
              rc = false;
            }
          } else {
            GECO_ERROR("Failed to receive job-started acknowledgement for %ld.%ld (pid %ld)", jobId, taskId, (long int)getpid());
          }
          GECOQuarantineCommandDestroy(quarantineCommand);
        } else {
          GECO_ERROR("Failed to send job-started quarantine command for %ld.%ld (pid %ld) (errno = %d)", jobId, taskId, (long int)getpid(), errno);
        }
      } else {
        GECO_ERROR("Could not create job-started quarantine command for %ld.%ld (pid %ld)", jobId, taskId, (long int)getpid());
        rc = false;
      }
    } else {
      GECO_ERROR("Could not open client socket '%s' to perform quarantine operations for %ld.%ld (pid %ld)",
          ( GECOExecWrapperQuarantineSocketAddr ? GECOExecWrapperQuarantineSocketAddr : GECODDefaultQuarantineSocket ),
          jobId, taskId, (long int)getpid()
        );
    }
  }
  
early_exit:
  if ( defaultLog ) {
    GECOLogSetDefault(NULL);
    GECOLogDestroy(defaultLog);
  }
  return rc;
}

//
#if 0
#pragma mark -
#endif
//

int
execl(
  const char    *path,
  const char    *arg,
  ...
)
{
  int           rc = 0;
  size_t        argv_max = GECO_STATIC_ARGV_SIZE;
  const char    *initial_argv[GECO_STATIC_ARGV_SIZE];
  const char    **argv = initial_argv;
  va_list       args;

  argv[0] = arg;
  
  GECOExecWrapperTmpDebug("%d:%d execl(%s,%s, ...)\n", getpid(), getppid(), path, (arg ? arg : "''"));
  
  va_start(args, arg);
  unsigned int i = 0;
  while (argv[i++] != NULL) {
    if (i == argv_max) {
      argv_max *= 2;
      const char **nptr = realloc(argv == initial_argv ? NULL : argv, argv_max * sizeof(const char *));
	  
      if (nptr == NULL) {
        if (argv != initial_argv) free(argv);
	      return -1;
	    }
      if (argv == initial_argv) {
        /* We have to copy the already filled-in data ourselves.  */
        memcpy(nptr, argv, i * sizeof(const char *));
      }
      argv = nptr;
    }
    argv[i] = va_arg(args, const char *);
  }
  va_end(args);

  rc = execve(path, (char *const *)argv, __environ);
  if (argv != initial_argv) free(argv);

  return rc;
}

//

int
execle(
  const char    *path,
  const char    *arg,
  ...
)
{
  int               rc = 0;
  size_t            argv_max = GECO_STATIC_ARGV_SIZE;
  const char        *initial_argv[GECO_STATIC_ARGV_SIZE];
  const char        **argv = initial_argv;
  const char *const *envp;
  va_list           args;

  argv[0] = arg;

  GECOExecWrapperTmpDebug("%d:%d execle(%s,%s, ...)\n", getpid(), getppid(), path, (arg ? arg : "''"));

  va_start(args, arg);
  unsigned int i = 0;
  while (argv[i++] != NULL) {
    if (i == argv_max) {
      argv_max *= 2;
      const char **nptr = realloc(argv == initial_argv ? NULL : argv, argv_max * sizeof(const char *));
	  
      if (nptr == NULL) {
        if (argv != initial_argv) free(argv);
	      return -1;
	    }
      if (argv == initial_argv) {
        /* We have to copy the already filled-in data ourselves.  */
        memcpy(nptr, argv, i * sizeof(const char *));
      }
      argv = nptr;
    }
    argv[i] = va_arg(args, const char *);
  }
  envp = va_arg(args, const char *const *);
  va_end(args);

  rc = execve(path, (char *const *)argv, (char *const *)envp);
  if (argv != initial_argv) free(argv);

  return rc;
}

//

int
execlp(
  const char    *path,
  const char    *arg,
  ...
)
{
  int           rc = 0;
  size_t        argv_max = GECO_STATIC_ARGV_SIZE;
  const char    *initial_argv[GECO_STATIC_ARGV_SIZE];
  const char    **argv = initial_argv;
  va_list       args;

  argv[0] = arg;
  
  GECOExecWrapperTmpDebug("%d:%d execlp(%s,%s, ...)\n", getpid(), getppid(), path, (arg ? arg : "''"));

  va_start(args, arg);
  unsigned int i = 0;
  while (argv[i++] != NULL) {
    if (i == argv_max) {
      argv_max *= 2;
      const char **nptr = realloc(argv == initial_argv ? NULL : argv, argv_max * sizeof(const char *));
	  
      if (nptr == NULL) {
        if (argv != initial_argv) free(argv);
	      return -1;
	    }
      if (argv == initial_argv) {
        /* We have to copy the already filled-in data ourselves.  */
        memcpy(nptr, argv, i * sizeof(const char *));
      }
      argv = nptr;
    }
    argv[i] = va_arg(args, const char *);
  }
  va_end(args);

  rc = execvp(path, (char *const *)argv);
  if (argv != initial_argv) free(argv);

  return rc;
}

//

int
execv(
  const char    *path,
  char *const   argv[]
)
{
  GECOExecWrapperTmpDebug("%d:%d execv(%s, ...)\n", getpid(), getppid(), path);
  return execve(path, argv, __environ);
}

//

int
execve(
  const char    *filename,
  char *const   argv[],
  char *const   envp[]
)
{
  static int (*GECOOriginalSymbol_execve)(const char *filename, char *const argv[], char *const envp[]) = NULL;
  
  if ( ! GECOOriginalSymbol_execve ) GECOOriginalSymbol_execve = dlsym(RTLD_NEXT, "execve");
  
  GECOExecWrapperTmpDebug("%d:%d execve(%s, ...)\n", getpid(), getppid(), filename);
  
  // Pare the command we wish to execute down to its basename:
  const char    *nextCommand = filename + strlen(filename);
  
  while ( nextCommand-- > filename ) {
    if ( *nextCommand == '/' ) {
      nextCommand++;
      break;
    }
  }
  
  char *const   *cleanEnvP = NULL;
  
  if ( GECOExecWrapperQuarantine(nextCommand, envp, &cleanEnvP) ) {
    if ( GECOOriginalSymbol_execve ) {
      return GECOOriginalSymbol_execve(filename, argv, (cleanEnvP ? cleanEnvP : envp ));
    } else {
      errno = ENOSYS;
    }
  }
  if ( cleanEnvP ) free((void*)cleanEnvP);
  
  return -1;
}

//

int
execvp(
  const char    *file,
  char *const   argv[]
)
{
  GECOExecWrapperTmpDebug("%d:%d execvp(%s, ...)\n", getpid(), getppid(), file);
  return execvpe(file, argv, __environ);
}

//

int
execvpe(
  const char    *file,
  char *const   argv[],
  char *const   envp[]
)
{
  static int (*GECOOriginalSymbol_execvpe)(const char *file, char *const argv[], char *const envp[]) = NULL;
  
  if ( ! GECOOriginalSymbol_execvpe ) GECOOriginalSymbol_execvpe = dlsym(RTLD_NEXT, "execvpe");
  
  char *const   *cleanEnvP = NULL;
  
  GECOExecWrapperTmpDebug("%d:%d execvpe(%s, ...)\n", getpid(), getppid(), file);
  
  if ( GECOExecWrapperQuarantine(file, envp, &cleanEnvP) ) {
    if ( GECOOriginalSymbol_execvpe ) {
      return GECOOriginalSymbol_execvpe(file, argv, (cleanEnvP ? cleanEnvP : envp ));
    } else {
      errno = ENOSYS;
    }
  }
  if ( cleanEnvP ) free((void*)cleanEnvP);
  
  return -1;
}

//

int
fexecve(
  int           fd,
  char *const   argv[],
  char *const   envp[]
)
{
  static int (*GECOOriginalSymbol_fexecve)(int fd, char *const argv[], char *const envp[]) = NULL;
  
  if ( ! GECOOriginalSymbol_fexecve ) GECOOriginalSymbol_fexecve = dlsym(RTLD_NEXT, "fexecve");
  
  char *const   *cleanEnvP = NULL;
  
  GECOExecWrapperTmpDebug("%d:%d fexecve(%d, ...)\n", getpid(), getppid(), fd);
  
  if ( GECOExecWrapperQuarantine("unknown", envp, &cleanEnvP) ) {
    if ( GECOOriginalSymbol_fexecve ) {
      return GECOOriginalSymbol_fexecve(fd, argv, (cleanEnvP ? cleanEnvP : envp ));
    } else {
      errno = ENOSYS;
    }
  }
  if ( cleanEnvP ) free((void*)cleanEnvP);
  
  return -1;
}

//
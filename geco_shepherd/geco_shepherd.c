

#include "GECO.h"
#include "GECOLog.h"
#include "GECOResource.h"

#include <sys/types.h>
#include <sys/wait.h>

//

#ifndef GECO_LDPRELOAD_VALUE
#error GECO_LDPRELOAD_VALUE macro must be configured
#endif

const char                *GECOShepherdLDPreloadValue = "LD_PRELOAD="GECO_LDPRELOAD_VALUE;

//

bool
GECOShepherdGetDataFromEnvironmentFile(
  long int    *jobId,
  long int    *taskId,
  char        *shepherdPath,
  size_t      shepherdPathLen
)
{
  bool        rc = false;
  int         try = 0;
  
  //
  // Inside the shepherd's working directory is a file named "environment"
  // that contains the environment that should be reconstituted for the
  // job's runtime.  Therein will be found the JOB_ID and SGE_TASK_ID.
  //
  while ( try < 5 ) {
    if ( GECOIsFile("environment") ) break;
    try++;
    GECO_ERROR("waiting for environment file, try %d", try);
    sleep(try * 2);
  }
  if ( try < 5 ) {
    long int  foundJobId = GECOUnknownJobId;
    long int  foundTaskId = GECOUnknownTaskId;
    char      *sge_root = NULL, *sge_arch = NULL;
    FILE      *envFPtr = fopen("environment", "r");
    
    if ( envFPtr ) {
      size_t  lineBufferLen = 1024;
      char    *lineBuffer = malloc(lineBufferLen);
      
      while ( ! feof(envFPtr) ) {
        char  *p = lineBuffer, *e = lineBuffer + lineBufferLen;
        bool  fullLine = true, newlineFound = false;
        
        while ( ! newlineFound && ! feof(envFPtr) ) {
          *p = fgetc(envFPtr);
          switch ( *p ) {
            
            case '=': {
              if ( strncmp("JOB_ID=", lineBuffer, p - lineBuffer - 1) 
                   && strncmp("SGE_TASK_ID=", lineBuffer, p - lineBuffer - 1)
                   && strncmp("SGE_ROOT=", lineBuffer, p - lineBuffer - 1)
                   && strncmp("SGE_ARCH=", lineBuffer, p - lineBuffer - 1)
              ) {
                // We don't care about this variable, drop the NAME= and reuse the buffer (perhaps preventing a realloc):
                p = lineBuffer;
                fullLine = false;
              } else {
                p++;
              }
              break;
            }
            
            case '\n': {
              *p = '\0';
              newlineFound = true;
            }
            default: {
              p++;
              break;
            }
            
          }
          if ( (p == e) && ! newlineFound ) {
            // Buffer was too small:
            char    *newLineBuffer = realloc(lineBuffer, lineBufferLen + 1024);
            
            if ( ! newLineBuffer ) {
              free(lineBuffer);
              errno = ENOMEM;
              goto early_exit;
            }
            p = newLineBuffer + lineBufferLen;
            lineBufferLen += 1024;
            lineBuffer = newLineBuffer;
            e = lineBuffer + lineBufferLen;
          }
        }
        if ( fullLine ) {
          // Lines are NL-terminated and consist of NAME=VALUE:
          if ( strncmp("JOB_ID=", lineBuffer, 7) == 0 ) {
            if ( ! GECO_strtol(lineBuffer + 7, &foundJobId, NULL) ) {
              errno = EINVAL;
              goto early_exit;
            }
          }
          else if ( strncmp("SGE_TASK_ID=", lineBuffer, 12) == 0 ) {
            if ( strcmp("undefined", lineBuffer + 12) == 0 ) {
              foundTaskId = 0;
            }
            else if ( ! GECO_strtol(lineBuffer + 12, &foundTaskId, NULL) ) {
              errno = EINVAL;
              goto early_exit;
            }
          }
          else if ( strncmp("SGE_ROOT=", lineBuffer, 9) == 0 ) {
            sge_root = strdup(lineBuffer + 9);
          }
          else if ( strncmp("SGE_ARCH=", lineBuffer, 9) == 0 ) {
            sge_arch = strdup(lineBuffer + 9);
          }
          if ( (foundJobId != GECOUnknownJobId) && (foundTaskId != GECOUnknownTaskId) && sge_root && sge_arch ) {
            lineBufferLen = snprintf(shepherdPath, shepherdPathLen, "%s/bin/%s/sge_shepherd", sge_root, sge_arch);
            if ( lineBufferLen >= shepherdPathLen ) {
              errno = ENAMETOOLONG;
            } else {
              *jobId = foundJobId;
              *taskId = foundTaskId;
              rc = true;
            }
            break;
          }
        }
      }
early_exit:
      fclose(envFPtr);
      free(lineBuffer);
      if ( sge_arch ) free((void*)sge_arch);
      if ( sge_root ) free((void*)sge_root);
    }
  }
  return rc;
}

//

bool
GECOShepherdEnvironmentAddLDPRELOAD(
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
      if ( strcmp(*p + strlen("LD_PRELOAD="), GECOShepherdLDPreloadValue) == 0 ) return true;
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
    *newList++ = (char*)GECOShepherdLDPreloadValue;
    *newList = NULL;
    return true;
  }
  errno = ENOMEM;
  return false;
}

//

int
main(
  int             argc,
  const char*     argv[],
  char * const*   envp
)
{
  int               rc = 0;
  long int          jobId, taskId;
  char              sgeShepherd[PATH_MAX];
  GECOLogRef        logFile = GECOLogCreateWithFilePath(GECOLogLevelDebug, "/tmp/geco_shepherd.log");
  
  if ( logFile ) GECOLogSetDefault(logFile);
  
  if ( GECOShepherdGetDataFromEnvironmentFile(&jobId, &taskId, sgeShepherd, sizeof(sgeShepherd)) ) {
    char            path[PATH_MAX];
    int             pathLen;

    if ( taskId <= 0 ) taskId = 1;

    pathLen = snprintf(path, sizeof(path), "%s/resources/%ld.%ld", GECOGetStateDir(), jobId, taskId);
    if ( pathLen >= sizeof(path) ) {
      GECO_ERROR("resource cache path exceeds PATH_MAX (%d >= %d)", pathLen, (int)sizeof(path));
      rc = ENAMETOOLONG;
    } else {
      //
      // Is the data already cached?  We only want to do one qstat, so let's try to pre-cache the
      // resource info right now:
      //
      if ( ! GECOIsFile(path) ) {
        GECOResourceSetCreateFailure    failureReason;
        GECOResourceSetRef              jobResources = GECOResourceSetCreate(jobId, taskId, 5, &failureReason);;
        
        if ( ! jobResources ) {
          switch ( failureReason ) {
      
            case GECOResourceSetCreateFailureCheckErrno: {
              GECO_ERROR("failed to find resource information for job %ld.%ld (errno = %d)", jobId, taskId, errno);
              rc = 100;
              break;
            }
      
            case GECOResourceSetCreateFailureQstatFailure: {
              GECO_ERROR("failed to find resource information for job %ld.%ld, general qstat failure", jobId, taskId);
              rc = 100;
              break;
            }
      
            case GECOResourceSetCreateFailureMalformedQstatXML: {
              GECO_ERROR("failed to find resource information for job %ld.%ld, qstat output is malformed", jobId, taskId);
              rc = EINVAL;
              break;
            }
      
            case GECOResourceSetCreateFailureJobDoesNotExist: {
              GECO_ERROR("job %ld.%ld is not known to the qmaster", jobId, taskId);
              rc = EINVAL;
              break;
            }
            
            default: {
              GECO_ERROR("unknown error while looking for job %ld.%ld (reason = %d)", jobId, taskId, failureReason);
              rc = ENOENT;
              break;
            }
            
          }
        } else {
          GECO_INFO("loaded resource information for job %ld.%ld via qstat", jobId, taskId);
          rc = 0;
          //
          // Write to the cache file:
          //
          if ( GECOResourceSetSerialize(jobResources, path) ) {
            GECO_INFO("serialized resource information for job %ld.%ld to %s", jobId, taskId, path);
          } else {
            GECO_ERROR("unable to serialize resource profile for job %ld.%ld to %s (errno = %d)",
                jobId, taskId, path, errno
              );
            rc = 100;
          }
          GECOResourceSetDestroy(jobResources);
        }
      }
      
      //
      // Continue if we didn't fail to setup the resource cache:
      //
      if ( rc == 0 ) {
        char *const   *sgeShepherdEnv = NULL;
        
        if ( GECOShepherdEnvironmentAddLDPRELOAD(envp, &sgeShepherdEnv) ) {
          GECO_INFO("LD_PRELOAD added to environment for shepherd for job %ld.%ld", jobId, taskId);

#ifdef GECO_SHEPHERD_SHOULDFORK

          pid_t   childProcess = fork();
          
          if ( childProcess == 0 ) {
            execve(
                sgeShepherd,
                (char * const*)argv,
                sgeShepherdEnv
              );
            GECO_ERROR("failed to execute sge_shepherd %s for job %ld.%ld (errno = %d)", sgeShepherd, jobId, taskId, errno);
            rc = errno;
          }
          else if ( childProcess < 0 ) {
            GECO_ERROR("unable to fork to execute sge_shepherd %s for job %ld.%ld (errno = %d)", sgeShepherd, jobId, taskId, errno);
            rc = errno;
          }
          else {
            int     status;
            pid_t   exitedPid;
            
            while ( (exitedPid = wait(&status)) != childProcess ) {
              if ( exitedPid == -1 ) break;
            }
            rc = WEXITSTATUS(status);
            if ( rc != 0 ) {
              GECO_ERROR("%s for job %ld.%ld exited in error (errno = %d)", sgeShepherd, jobId, taskId, errno);
            } else {
              GECO_INFO("%s for job %ld.%ld exited cleanly", sgeShepherd, jobId, taskId);
            }
            
          }
#else

          GECO_INFO("executing sge_shepherd %s for job %ld.%ld", sgeShepherd, jobId, taskId);
          //
          // Replace this process with the actual shepherd, executing with our
          // preload library so that exec*() functions are patched:
          //
          execve(
              sgeShepherd,
              (char * const*)argv,
              sgeShepherdEnv
            );
          GECO_ERROR("failed to execute sge_shepherd %s for job %ld.%ld (errno = %d)", sgeShepherd, jobId, taskId, errno);
          rc = errno;
            
#endif
        } else {
          GECO_ERROR("unable to add LD_PRELOAD to environment for sge_shepherd for job %ld.%ld (errno = %d)", jobId, taskId, errno);
          rc = errno;
        }
      }
    
    }
  } else {
    char         *curWorkDir = get_current_dir_name();

    GECO_ERROR("unable to find a job/task identifier in %s/environment (errno = %d)", (curWorkDir ? curWorkDir : "<unknown>"), errno);
    if ( curWorkDir ) free((void*)curWorkDir);
    rc = 100;
  }
  return rc;
}

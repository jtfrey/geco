/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECODQuarantineSocket.c
 *  
 *  Polling source that watches for quarantine requests.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */



int
GECODQuarantineSocketFileDescriptorForPolling(
  GECOPollingSource   theSource
)
{
  GECOQuarantineSocket  *src = (GECOQuarantineSocket*)theSource;
  
  return src->socketFd;
}

//

void
GECODQuarantineSocketDidReceiveDataAvailable(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECOQuarantineSocket        *src = (GECOQuarantineSocket*)theSource;
  int                         connFd = accept(src->socketFd, NULL, NULL);
  
  if ( connFd >= 0 ) {
    GECOQuarantineSocket      theSocket;
    GECOQuarantineCommandRef  theCommand = NULL;
    
    GECO_INFO("GECODQuarantineSocketDidReceiveDataAvailable: connection accepted on fd %d", connFd);
    GECOQuarantineSocketInitWithFd(connFd, &theSocket);
    if ( GECOQuarantineSocketRecvCommand(&theSocket, &theCommand) ) {
    
      switch ( GECOQuarantineCommandGetCommandId(theCommand) ) {
      
        case GECOQuarantineCommandIdJobStarted: {
          //
          // See if we can reconstitute some job information:
          //
          bool                  ok = false;
          long int              jobId = GECOQuarantineCommandJobStartedGetJobId(theCommand),
                                taskId = GECOQuarantineCommandJobStartedGetTaskId(theCommand);
          pid_t                 jobPid = GECOQuarantineCommandJobStartedGetJobPid(theCommand);
          GECOJobRef            theJob = GECODJobCreationFunction(jobId, taskId);
          
          if ( theJob ) {
            if ( GECOJobCGroupInit(theJob, GECODRunloop) ) {
              if ( GECOJobCGroupAddPid(theJob, jobPid) ) {
                ok = true;
                GECOPidToJobIdMapAddPid(GECODPidMappings, jobPid, jobId, taskId);
              } else {
                GECO_ERROR("GECODQuarantineSocketDidReceiveDataAvailable: failed to add pid %ld to cgroups for %ld.%ld", (long int)jobPid, jobId, taskId);
                GECOJobRelease(theJob);
              }
            } else {
              GECO_ERROR("GECODQuarantineSocketDidReceiveDataAvailable: failed to init cgroups for %ld.%ld (pid %ld)", jobId, taskId, (long int)jobPid);
              GECOJobRelease(theJob);
            }
          } else {
            GECO_ERROR("GECODQuarantineSocketDidReceiveDataAvailable: no job information available for %ld.%ld (pid %ld)", jobId, taskId, (long int)jobPid);
          }
          
          GECOQuarantineCommandRef    ackCommand = GECOQuarantineCommandAckJobStartedCreate(jobId, taskId, ok);
          
          if ( ackCommand ) {
            if ( GECOQuarantineSocketSendCommand(&theSocket, ackCommand) ) {
              GECO_INFO("Job-started acknowledgement (%s) sent for %ld.%ld", (ok ? "success" : "failure"), jobId, taskId);
            } else {
              GECO_ERROR("Failed to send job-started acknowledgement (%s) for %ld.%ld", (ok ? "success" : "failure"), jobId, taskId);
            }
            GECOQuarantineCommandDestroy(ackCommand);
          } else {
            GECO_ERROR("Unable to create job-started acknowledgement command (%s) for %ld.%ld", (ok ? "success" : "failure"), jobId, taskId);
          }
          break;
        }
      
      }
      
      GECOQuarantineCommandDestroy(theCommand);
    }
 
    close(connFd);
    GECO_INFO("completed, fd %d closed", connFd);
  } else {
    GECO_ERROR("GECODQuarantineSocketDidReceiveDataAvailable: failed to accept connection (errno = %d)", errno);
  }
}

//

void
GECODQuarantineSocketDidReceiveClose(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECOQuarantineSocket  *src = (GECOQuarantineSocket*)theSource;
  
  GECOQuarantineSocketClose(src);
}

//

GECOPollingSourceCallbacks    GECODQuarantineSocketCallbacks = {
                                            .destroySource = NULL,
                                            .fileDescriptorForPolling = GECODQuarantineSocketFileDescriptorForPolling,
                                            .shouldSourceClose = NULL,
                                            .willRemoveAsSource = NULL,
                                            .didAddAsSource = NULL,
                                            .didBeginPolling = NULL,
                                            .didReceiveDataAvailable = GECODQuarantineSocketDidReceiveDataAvailable,
                                            .didEndPolling = NULL,
                                            .didReceiveClose = GECODQuarantineSocketDidReceiveClose,
                                            .didRemoveAsSource = NULL
                                          };


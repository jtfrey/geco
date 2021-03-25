/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOQuarantine.h
 *
 *  Quarantine messaging helpers.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOQUARANTINE_H__
#define __GECOQUARANTINE_H__

#include "GECO.h"

typedef struct __GECOQuarantineCommand * GECOQuarantineCommandRef;

//

typedef enum {
  GECOQuarantineSocketTypeInferred  = 0,
  GECOQuarantineSocketTypeFilePath  = 1,
  GECOQuarantineSocketTypeLoopback  = 2,
  //
  GECOQuarantineSocketTypeUnknown   = 7
} GECOQuarantineSocketType;

typedef struct {
  GECOQuarantineSocketType      socketType;
  const char                    *socketAddrInfo;
  int                           socketFd;
} GECOQuarantineSocket;

void GECOQuarantineSocketInitWithFd(int socketFd, GECOQuarantineSocket *outSocket);
bool GECOQuarantineSocketOpenServer(GECOQuarantineSocketType socketType, const char *socketAddrInfo, unsigned int retryCount, unsigned int timeoutForRecv, unsigned int timeoutForSend, GECOQuarantineSocket *outSocket);
bool GECOQuarantineSocketOpenClient(GECOQuarantineSocketType socketType, const char *socketAddrInfo, unsigned int retryCount, unsigned int timeoutForRecv, unsigned int timeoutForSend, GECOQuarantineSocket *outSocket);
bool GECOQuarantineSocketClose(GECOQuarantineSocket *theSocket);

bool GECOQuarantineSocketSendCommand(GECOQuarantineSocket *theSocket, GECOQuarantineCommandRef aCommand);
bool GECOQuarantineSocketRecvCommand(GECOQuarantineSocket *theSocket, GECOQuarantineCommandRef *aCommand);

//

enum {
  GECOQuarantineCommandIdNoOp             = 0,
  GECOQuarantineCommandIdJobStarted       = 1,
  GECOQuarantineCommandIdAckJobStarted    = 2
};
typedef uint32_t GECOQuarantineCommandId;

void GECOQuarantineCommandDestroy(GECOQuarantineCommandRef aCommand);

GECOQuarantineCommandId GECOQuarantineCommandGetCommandId(GECOQuarantineCommandRef aCommand);
size_t GECOQuarantineCommandGetPayloadBytes(GECOQuarantineCommandRef aCommand, void *buffer, size_t bufferLen);
size_t GECOQuarantineCommandGetPayloadSize(GECOQuarantineCommandRef aCommand);

//

GECOQuarantineCommandRef GECOQuarantineCommandJobStartedCreate(long int jobId, long int taskId, pid_t jobPid);
long int GECOQuarantineCommandJobStartedGetJobId(GECOQuarantineCommandRef aCommand);
long int GECOQuarantineCommandJobStartedGetTaskId(GECOQuarantineCommandRef aCommand);
pid_t GECOQuarantineCommandJobStartedGetJobPid(GECOQuarantineCommandRef aCommand);

//

GECOQuarantineCommandRef GECOQuarantineCommandAckJobStartedCreate(long int jobId, long int taskId, bool success);
long int GECOQuarantineCommandAckJobStartedGetJobId(GECOQuarantineCommandRef aCommand);
long int GECOQuarantineCommandAckJobStartedGetTaskId(GECOQuarantineCommandRef aCommand);
bool GECOQuarantineCommandAckJobStartedGetSuccess(GECOQuarantineCommandRef aCommand);

#endif /* __GECOQUARANTINE_H__ */

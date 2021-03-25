/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOResource.h
 *
 *  Functions/data types associated with job resource allocations
 *  granted by Grid Engine.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECORESOURCE_H__
#define __GECORESOURCE_H__

#include "GECOLog.h"

typedef struct _GECOResourcePerNodeData {
  long            slotCount;
  double          memoryLimit;
  double          virtualMemoryLimit;
  const char      *gpuList;
  const char      *phiList;
} GECOResourcePerNodeData;

typedef struct _GECOResourcePerNode *GECOResourcePerNodeRef;

const char* GECOResourcePerNodeGetNodeName(GECOResourcePerNodeRef aResource);
bool GECOResourcePerNodeGetIsSlave(GECOResourcePerNodeRef aResource);
void GECOResourcePerNodeGetNodeData(GECOResourcePerNodeRef aResource, GECOResourcePerNodeData *perNodeData);

//

typedef struct _GECOResourceSet   *GECOResourceSetRef;

//

typedef enum {
  GECOResourceSetCreateFailureNone = 0,
  GECOResourceSetCreateFailureCheckErrno,
  GECOResourceSetCreateFailureQstatFailure,
  GECOResourceSetCreateFailureMalformedQstatXML,
  GECOResourceSetCreateFailureJobDoesNotExist,
  GECOResourceSetCreateFailureNoStaticProperties,
  GECOResourceSetCreateFailureInvalidJobOwner,
  GECOResourceSetCreateFailureNoRequestedResources,
  GECOResourceSetCreateFailureNoGrantedResources
} GECOResourceSetCreateFailure;

//

bool GECOResourceSetIsJobRunningOnHost(long int jobId, long int taskId, int retryCount);

//

GECOResourceSetRef GECOResourceSetCreate(long int jobId, long int taskId, int retryCount, GECOResourceSetCreateFailure *failureReason);
GECOResourceSetRef GECOResourceSetCreateWithXMLAtPath(const char *xmlFile, long int jobId, long int taskId, GECOResourceSetCreateFailure *failureReason);
GECOResourceSetRef GECOResourceSetCreateWithFileDescriptor(int fd, long int jobId, long int taskId, GECOResourceSetCreateFailure *failureReason);
void GECOResourceSetDestroy(GECOResourceSetRef theResourceSet);

//

const char* GECOResourceSetGetJobName(GECOResourceSetRef theResourceSet);

const char* GECOResourceSetGetOwnerUserName(GECOResourceSetRef theResourceSet);
uid_t GECOResourceSetGetOwnerUserId(GECOResourceSetRef theResourceSet);
const char* GECOResourceSetGetOwnerGroupName(GECOResourceSetRef theResourceSet);
gid_t GECOResourceSetGetOwnerGroupId(GECOResourceSetRef theResourceSet);

bool GECOResourceSetExecuteAsOwner(GECOResourceSetRef theResourceSet);

const char* GECOResourceSetGetWorkingDirectory(GECOResourceSetRef theResourceSet);

GECOLogLevel GECOResourceSetGetTraceLevel(GECOResourceSetRef theResourceSet);
void GECOResourceSetSetTraceLevel(GECOResourceSetRef theResourceSet, GECOLogLevel traceLevel);

double GECOResourceSetGetPerSlotVirtualMemoryLimit(GECOResourceSetRef theResourceSet);
double GECOResourceSetGetRuntimeLimit(GECOResourceSetRef theResourceSet);
bool GECOResourceSetGetIsStandby(GECOResourceSetRef theResourceSet);
bool GECOResourceSetGetIsArrayJob(GECOResourceSetRef theResourceSet);
bool GECOResourceSetGetShouldConfigPhiForUser(GECOResourceSetRef theResourceSet);

//

unsigned int GECOResourceSetGetNodeCount(GECOResourceSetRef theResourceSet);
GECOResourcePerNodeRef GECOResourceSetGetPerNodeAtIndex(GECOResourceSetRef theResourceSet, unsigned int index);
GECOResourcePerNodeRef GECOResourceSetGetPerNodeWithNodeName(GECOResourceSetRef theResourceSet, const char *nodeName);
GECOResourcePerNodeRef GECOResourceSetGetPerNodeForHost(GECOResourceSetRef theResourceSet);

//

typedef enum {
  GECOResourceSetExportModeUserEnv,
  GECOResourceSetExportModeGEProlog,
  GECOResourceSetExportModeGEEpilog
} GECOResourceSetExportMode;

void GECOResourceSetExport(GECOResourceSetRef theResourceSet, GECOResourceSetExportMode exportMode);
void GECOResourceSetExportForNodeName(GECOResourceSetRef theResourceSet, GECOResourceSetExportMode exportMode, const char *nodeName);

bool GECOResourceSetSerialize(GECOResourceSetRef theResourceSet, const char *path);
GECOResourceSetRef GECOResourceSetDeserialize(const char *path);

#endif /* __GECORESOURCE_H__ */

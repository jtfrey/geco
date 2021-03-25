/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOJob.h
 *
 *  Wrapper for a Grid Engine job and state associated with it.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOJOB_H__
#define __GECOJOB_H__

#include "GECOLog.h"
#include "GECOResource.h"
#include "GECORunloop.h"

/*!
  @typedef GECOJobRef
  @discussion
    Type of a reference to information associated with a Grid Engine
    job.
*/
typedef struct _GECOJob * GECOJobRef;

/*!
  @function GECOJobInit
  @discussion
    Initializes this pseudo-class by pre-allocating a fixed number of
    (contiguous in memory) instances.
*/
void GECOJobInit(void);

/*!
  @function GECOJobDeinit
  @discussion
    Cleanup any GECOJob objects that have not yet been destroyed.
*/
void GECOJobDeinit(void);

GECOJobRef GECOJobCreateWithJobIdentifier(long int jobId, long int taskId);
GECOJobRef GECOJobCreateWithJobIdentifierFromResourceCache(long int jobId, long int taskId);

GECOJobRef GECOJobGetExistingObjectForJobIdentifier(long int jobId, long int taskId);

bool GECOJobIdentiferExistsInResourceCache(long int jobId, long int taskId);

unsigned int GECOJobGetReferenceCount(GECOJobRef theJob);
void GECOJobRetain(GECOJobRef theJob);
void GECOJobRelease(GECOJobRef theJob);

long int GECOJobGetJobId(GECOJobRef theJob);
long int GECOJobGetTaskId(GECOJobRef theJob);

bool GECOJobHasExited(GECOJobRef theJob);

bool GECOJobCGroupInit(GECOJobRef theJob, GECORunloopRef theRunloop);
bool GECOJobCGroupDeinit(GECOJobRef theJob);

bool GECOJobCGroupAddPid(GECOJobRef theJob, pid_t aPid);
bool GECOJobCGroupAddPidAndChildren(GECOJobRef theJob, pid_t aPid, bool shouldAddChildProcesses);

bool GECOJobScheduleOOMWatchInRunloop(GECOJobRef theJob, GECORunloopRef theRunloop);

#endif /* __GECOJOB_H__ */

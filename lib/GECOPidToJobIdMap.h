/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOPidToJobIdMap.h
 *
 *  Manage mappings of PID => (jobId, taskId)
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOPIDTOJOBIDMAP_H__
#define __GECOPIDTOJOBIDMAP_H__

#include "GECO.h"

/*!
  @typedef GECOPidToJobIdMapRef
  @discussion
    Type of a reference to a pid-to-job-id mapping object.
*/
typedef struct _GECOPidToJobIdMap * GECOPidToJobIdMapRef;

/*!
  @function GECOPidToJobIdMapCreate
  @discussion
    Create a new (intially empty) pid-to-job-id mapping table.  Internally, the
    pids will be hashed into a table of the given tableSize.  If tableSize is zero,
    the default size is used.
  @result
    Returns NULL if a new map could not be allocated.
*/
GECOPidToJobIdMapRef GECOPidToJobIdMapCreate(unsigned int tableSize);

/*!
  @function GECOPidToJobIdMapDestroy
  @discussion
    Invalidate aMap and deallocate any resources used by it.
  @result
    After calling this function, aMap is no longer a valid object.
*/
void GECOPidToJobIdMapDestroy(GECOPidToJobIdMapRef aMap);

/*!
  @function GECOPidToJobIdMapHasJobAndTaskId
  @discussion
    If aMap contains any mapping of a process id to (jobId, taskId) then return
    boolean true.
*/
bool GECOPidToJobIdMapHasJobAndTaskId(GECOPidToJobIdMapRef aMap, long int jobId, long int taskId);

/*!
  @function GECOPidToJobIdMapGetJobAndTaskIdForPid
  @discussion
    If the given process id, aPid, has a mapping in aMap then set *jobId and *taskId
    to the associated values. 
  @result
    Returns boolean true if aPid is present.
*/
bool GECOPidToJobIdMapGetJobAndTaskIdForPid(GECOPidToJobIdMapRef aMap, pid_t aPid, long int *jobId, long int *taskId);

/*!
  @function GECOPidToJobIdMapAddPid
  @discussion
    Associate process id aPid with the given (jobId, taskId) in aMap.
  @result
    Returns boolean true if the association was successfully added.
*/
bool GECOPidToJobIdMapAddPid(GECOPidToJobIdMapRef aMap, pid_t aPid, long int jobId, long int taskId);

/*!
  @function GECOPidToJobIdMapRemovePid
  @discussion
    If the given process id, aPid, has had an association registered
    then remove it.
*/
void GECOPidToJobIdMapRemovePid(GECOPidToJobIdMapRef aMap, pid_t aPid);

#endif /* __GECOPIDTOJOBIDMAP_H__ */

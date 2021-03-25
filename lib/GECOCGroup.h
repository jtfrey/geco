/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOCGroup.h
 *
 *  Interfaces/conveniences that work with the Linux cgroup
 *  facilities.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECOCGROUP_H__
#define __GECOCGROUP_H__

#include "GECO.h"

#include <hwloc.h>

/*!
  @function GECOCGroupGetPrefix
  @result
    Returns a C string constant containing the base path at which cgroups
    are expected to be mounted on the system.
    
    If the return value is NULL, the cgroup mechanism is non-functional.
*/
const char* GECOCGroupGetPrefix(void);

/*!
  @function GECOCGroupSetPrefix
  @discussion
    Attempts to set the library to use the given absolute path, cgroupPrefix,
    under which cgroups are expected to be mounted on the system.
    
    If cgroupPrefix is NULL, the built-in default path is selected.
    
    This function has no effect after GECOCGroupInitSubsystems() has been
    called.
  @result
    Returns boolean false if:
    
      - cgroupPrefix is a non-NULL, relative path
      - cgroupPrefix does not exist
      - cgroupPrefix is not a directory
*/
bool GECOCGroupSetPrefix(const char *cgroupPrefix);

/*!
  @function GECOCGroupGetSubGroup
  @result
    Returns a C string constant containing the path fragment used for
    all GECO-managed per-job cgroups.
*/
const char* GECOCGroupGetSubGroup(void);

/*!
  @function GECOCGroupSetSubGroup
  @discussion
    Attempts to set the library to use the given path fragment, subgroup,
    for all GECO-managed per-job cgroups.  The fragment can lead with '/'
    characters but they will be discarded.  The fragment cannot be "." or
    start with the parent directory ("..").  The fragment must be a single
    path component (contains no path separator character).
    
    This function has no effect after GECOCGroupInitSubsystems() has been
    called.
*/
bool GECOCGroupSetSubGroup(const char *subgroup);

/*!
  @typedef GECOCGroupSubsystem
  @discussion
    Enumeration of the available cgroup subsystems.
    
  @const GECOCGroupSubsystem_all
    Special value that equates to all available cgroup subsystems (e.g.
    for initialization functions).
*/
typedef enum {
  GECOCGroupSubsystem_min      = 0,
  //
  GECOCGroupSubsystem_blkio    = 0,
  GECOCGroupSubsystem_cpu      = 1,
  GECOCGroupSubsystem_cpuacct  = 2,
  GECOCGroupSubsystem_cpuset   = 3,
  GECOCGroupSubsystem_devices  = 4,
  GECOCGroupSubsystem_freezer  = 5,
  GECOCGroupSubsystem_memory   = 6,
  GECOCGroupSubsystem_net_cls  = 7,
  //
  GECOCGroupSubsystem_max      = 8,
  //
  GECOCGroupSubsystem_invalid  = -1,
  GECOCGroupSubsystem_all      = -2
} GECOCGroupSubsystem;

const char* GECOCGroupSubsystemToCString(GECOCGroupSubsystem theSubsystem);
GECOCGroupSubsystem GECOCGroupCStringToSubsystem(const char *subsystemStr);

/*!
  @function GECOCGroupGetSubsystemIsManaged
  @result
    Returns boolean true if the library is currently configured to manage
    the cgroup subsystem indicated by theCGroupSubsystem.
    
    Managed subsystems will automatically have per-job subgroups created
    by the GECOCGroupInitForJobIdentifier() and GECOCGroupInitSubsystems()
    functions.
*/
bool GECOCGroupGetSubsystemIsManaged(GECOCGroupSubsystem theCGroupSubsystem);

/*!
  @function GECOCGroupSetSubsystemIsManaged
  @result
    If isManaged is boolean true the library will manage the cgroup subsystem
    indicated by theCGroupSubsystem.  This function has no effect after
    GECOCGroupInitSubsystems() has been called.
    
    Managed subsystems will automatically have per-job subgroups created
    by the GECOCGroupInitForJobIdentifier() and GECOCGroupInitSubsystems()
    functions.
*/
void GECOCGroupSetSubsystemIsManaged(GECOCGroupSubsystem theCGroupSubsystem, bool isManaged);

/*!
  @function GECOCGroupInitSubsystems
  @discussion
    If any cgroup subsystems are configured to be managed by this library,
    then each is configured with:
    
      - a GECO subgroup
      - the "release_agent" set to the appropriate symlink of the GECO
        geco-cgroup-release executable
    
    For some subsystems, additional configuration details are applied
    (e.g. for cpusets, the cpusets.mem and cpusets.cpus entities are
    set to the parent's values).
  @result
    Returns boolean false if any subsystem failed to be setup.
*/
bool GECOCGroupInitSubsystems(void);

/*!
  @function GECOCGroupShutdownSubsystems
  @discussion
    If any cgroup subsystems are being managed by this library, then each
    is destroyed by:
    
      - rmdir() the GECO subgroup
      - the "release_agent" disabled (set to the empty string)
    
    For some subsystems, additional configuration details are applied
    (e.g. for cpusets, the cpusets.mem and cpusets.cpus entities are
    set to the parent's values).
  @result
    Returns boolean false if any subsystem failed to be removed.
*/
bool GECOCGroupShutdownSubsystems(void);

/*!
  @function GECOCGroupSnprintf
  @discussion
    Fill the given buffer with a filesystem path composed of, in sequence:
    
      - the cgroup prefix
      - the given cgroup subsystem
      - the GECO subgroup
      - (optional)the per-job directory, named according to "jobId.taskId"
      - (optional)an additional path fragment
    
    The jobId and taskId should be negative if unused, and the leafName should
    be NULL if unused.
    
    For example, the call
    
      GECOCGroupSnprintf(b, blen, GECOCGroupSubsystem_cpuset, GECOUnknownJobId, GECOUnknownTaskId, NULL)
      
    might produce the path
    
          /cgroup/cpuset/GECO
      
    Adding a leafName of "cpuset.cpus" would yield
    
          /cgroup/cpuset/GECO/cpuset.cpus
    
    while utilizing the job identifiers (all arguments used):
    
      GECOCGroupSnprintf(b, blen, GECOCGroupSubsystem_cpuset, 101, 15, "cpuset.cpus")
    
    would yield
    
          /cgroup/cpuset/GECO/101.15/cpuset.cpus
  
    This function is essentially a wrapper for snprintf, so its return values follow
    the conventions of that library function.
  @result
    Returns the number of characters written to buffer.
*/
int GECOCGroupSnprintf(char *buffer, size_t bufferSize, GECOCGroupSubsystem subsystem, long int jobId, long int taskId, const char* leafName);

/*!
  @typedef GECOCGroupInitCallback
  @discussion
    Type of a function that is called by GECOCGroupInitForJobIdentifier() for each subsystem
    it initializes.  The callback can do any application-specific initialization of the
    subgroup (at the given cgroupPath) that may be necessary.
  
    The function should return a boolean indicating whether or not it was successful in its
    own initialization work.
*/
typedef bool (*GECOCGroupInitCallback)(long int jobId, long int taskId, const void *context, GECOCGroupSubsystem theSubsystem, const char *cgroupPath, bool isNewSubgroup);

/*!
  @typedef GECOCGroupDeinitCallback
  @discussion
    Type of a function that is called by GECOCGroupDeinitForJobIdentifier() for each subsystem
    it tears-down.  The callback can do any application-specific tear-down of the subgroup
    (at the given cgroupPath) that may be necessary before GECOCGroupDeinitForJobIdentifier()
    attempts to remove cgroupPath.
  
    The function should return a boolean indicating whether or not it was successful in its
    own work.
*/
typedef bool (*GECOCGroupDeinitCallback)(long int jobId, long int taskId, const void *context, GECOCGroupSubsystem theSubsystem, const char *cgroupPath);

/*!
  @function GECOCGroupInitForJobIdentifier
  @discussion
    Create all per-job cgroup subgroups for the given job identifier, based on
    what cgroups are configured to be managed by this library.
    
    The notify_on_release option is enabled on each so that the GECO release agent
    will be called to remove the per-job cgroup when its final process exits.
  @result
    Returns boolean false in case of any error.
*/
bool GECOCGroupInitForJobIdentifier(long int jobId, long int taskId, GECOCGroupInitCallback initCallback, const void *initCallbackContext);

/*!
  @function GECOCGroupDeinitForJobIdentifier
  @discussion
    Destroy all per-job cgroup subgroups for the given job identifier, based on
    what cgroups are configured to be managed by this library.
  @result
    Returns boolean false in case of any error.
*/
bool GECOCGroupDeinitForJobIdentifier(long int jobId, long int taskId, GECOCGroupDeinitCallback deinitCallback, const void *deinitCallbackContext);

/*!
  @function GECOCGroupAddTask
  @discussion
    Attempt to add aPid to a per-job cgroup subsystem.
  @result
    Returns boolean true on success, false otherwise.
*/
bool GECOCGroupAddTask(GECOCGroupSubsystem theCGroupSubsystem, long int jobId, long int taskId, pid_t aPid);

/*!
  @function GECOCGroupAddTaskAndChildren
  @discussion
    Attempt to add aPid (and all of its child processes) to a per-job cgroup subsystem.
    
    In the realm of gecod, it's necessary to use this function with addChildPids of true
    because notification is done asynchronously, and while we're processing exec's and
    fork's those processes themselves may have already started child processes.
  @result
    Returns boolean true on success, false otherwise.
*/
bool GECOCGroupAddTaskAndChildren(GECOCGroupSubsystem theCGroupSubsystem, long int jobId, long int taskId, pid_t aPid, bool addChildPids);

/*!
  @function GECOCGroupRemoveTasks
  @discussion
    Attempt to move tasks associated with a per-job cgroup subsystem back to
    the root of the subsystem.
  @result
    Returns boolean true on success, false otherwise.
*/
bool GECOCGroupRemoveTasks(GECOCGroupSubsystem theCGroupSubsystem, long int jobId, long int taskId);

/*!
  @function GECOCGroupSignalTasks
  @discussion
    Attempt to send signum to all tasks associated with a per-job cgroup subsystem.
  @result
    Returns boolean true on success, false otherwise.
*/
bool GECOCGroupSignalTasks(GECOCGroupSubsystem theCGroupSubsystem, long int jobId, long int taskId, int signum);

/*!
  @function GECOCGroupReadLeaf
  @discussion
    Read data from an arbitrary path fragment within the given per-job or GECO
    subgroup of a cgroup subsystem.
    
    Passing jobId GECOUnknownJobId and taskId GECOUnknownTaskId constructs leafName
    relative to the GECO subgroup.
    
    At most *bufferLen bytes will be read from the file (if it exists).  If data was
    read, *bufferLen is set to the number of bytes read from the file.
  @result
    Returns boolean true if the file exists and data was read from it.
*/
bool GECOCGroupReadLeaf(GECOCGroupSubsystem subsystem, long int jobId, long int taskId, const char* leafName, void *buffer, size_t *bufferLen);

/*!
  @function GECOCGroupWriteLeaf
  @discussion
    Write data to an arbitrary path fragment within the given per-job or GECO
    subgroup of a cgroup subsystem.
    
    Passing jobId GECOUnknownJobId and taskId GECOUnknownTaskId constructs leafName
    relative to the GECO subgroup.
    
    At most bufferLen bytes will be written to the file (if it exists).
  @result
    Returns boolean true if the file exists and bufferLen bytes of data were
    successfully written to it.
*/
bool GECOCGroupWriteLeaf(GECOCGroupSubsystem subsystem, long int jobId, long int taskId, const char* leafName, const void *buffer, size_t bufferLen);

#if __STDC_VERSION__ >= 199901L
/*!
  @function GECOCGroupWriteEventControl
  @discussion
    Helper function that calls GECOCGroupWriteLeaf() with the cgroup.event_control
    leaf name.
  @result
    See documentation for GECOCGroupWriteLeaf()
*/
static inline bool
GECOCGroupWriteEventControl(
  GECOCGroupSubsystem   subsystem,
  long int              jobId,
  long int              taskId,
  const void            *buffer,
  size_t                bufferLen
)
{
  return GECOCGroupWriteLeaf(subsystem, jobId, taskId,  "cgroup.event_control", buffer, bufferLen);
}
#else
#error This source code should be compiled with C99 support enabled.        
#endif

/*!
  @function GECOCGroupScanActiveCpusetBindings
  @discussion
    Attempt to regain a level of sanity w.r.t. what CPU cores are still
    available to us.  Any per-job directories present under the GECO
    cpuset subgroup are checked, and their cpuset.cpus merged into a
    hwloc cpuset bitmap.  When all per-job directories have been scanned,
    this bitmap corresponds to the in-use cores.  These are removed from
    the bitmap of all cores available to the GECO cpuset subgroup itself
    to yield the unallocated set of cores.
    
    This function is called by GECOJob when GECOCGroupAllocateCores() fails
    to allocate the requested number of cores.
  @result
    As long as the GECO subgroup of the cpuset subsystem is present and
    navigable, this function returns boolean true.
*/
bool GECOCGroupScanActiveCpusetBindings(void);

/*!
  @function GECOCGroupAllocateCores
  @discussion
    Attempt to select nCores processing unit(s) on this system.  The hwloc
    library is used, so the selection is made as optimally as possible given
    knowledge of the underlying processor and memory architecture and what
    cores this library believes to be unassigned at the moment.
    
    If outCpuset is not NULL, then the cores chosen are marked in-use (internal
    to this library) and *outCpuset is set to the cpuset bitmap.  The caller is
    responsible for later passing this cpuset bitmap to the
    GECOCGroupDeallocateCores() function to mark them as being available, at
    which point the bitmap is also destroyed.
    
    If outCpuset is NULL, then the chosen cores are not marked as in-use and
    the function merely indicates via return value whether or not nCores
    processing units are available.
  @result
    Returns boolean true if nCores processing units are available.
*/
bool GECOCGroupAllocateCores(unsigned int nCores, hwloc_bitmap_t *outCpuset);

/*!
  @function GECOCGroupDeallocateCores
  @discussion
    Return the processing units marked in theCpuset to being available.
    This function should be called to balance a call to GECOCGroupAllocateCores()
    that successfully allocates processing units.
    
    The bitmap will be destroyed by this function.
*/
void GECOCGroupDeallocateCores(hwloc_bitmap_t theCpuset);

/*!
  @function GECOCGroupGetMemoryLimit
  @discussion
    If a per-job GECO subgroup for the memory subsystem exists, attempt to read
    the current real memory limit.  If successful, *m_mem_free is set to the
    maximum number of bytes the cgroup may consume.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupGetMemoryLimit(long int jobId, long int taskId, size_t *m_mem_free);

/*!
  @function GECOCGroupSetMemoryLimit
  @discussion
    If a per-job GECO subgroup for the memory subsystem exists, attempt to set
    its real memory limit to m_mem_free bytes.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupSetMemoryLimit(long int jobId, long int taskId, size_t m_mem_free);

/*!
  @function GECOCGroupGetVirtualMemoryLimit
  @discussion
    If a per-job GECO subgroup for the memory subsystem exists, attempt to read
    the current virtual memory limit.  If successful, *h_vmem is set to the
    maximum number of bytes the cgroup may consume.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupGetVirtualMemoryLimit(long int jobId, long int taskId, size_t *h_vmem);

/*!
  @function GECOCGroupSetVirtualMemoryLimit
  @discussion
    If a per-job GECO subgroup for the memory subsystem exists, attempt to set
    its virtual memory limit to h_vmem bytes.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupSetVirtualMemoryLimit(long int jobId, long int taskId, size_t h_vmem);

bool GECOCGroupGetIsUnderOOM(long int jobId, long int taskId, bool *isUnderOOM);

/*!
  @function GECOCGroupGetCpusetCpus
  @discussion
    If a per-job GECO subgroup for the cpuset subsystem exists, attempt to read
    its list of assigned processing units.  If *cpulist is NULL, then a bitmap is
    allocated and the caller is responsible for destroying it.  If *cpulist is
    not NULL, *cpulist is reset with the subgroup's cpuset data.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupGetCpusetCpus(long int jobId, long int taskId, hwloc_bitmap_t *cpulist);

/*!
  @function GECOCGroupSetCpusetCpus
  @discussion
    If a per-job GECO subgroup for the cpuset subsystem exists, attempt to set
    its list of assigned processing units.  This function also copies the GECO
    subgroup's cpuset.mems and enables cpuset.cpu_exclusive.
  @result
    Returns boolean true if successful.
*/
bool GECOCGroupSetCpusetCpus(long int jobId, long int taskId, hwloc_bitmap_t cpulist);

#endif /* __GECOCGROUP_H__ */

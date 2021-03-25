/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECORunloop.c
 *
 *  Pseudo-object that implements the polling runloop that watches
 *  an arbitrary set of file descriptors for data/state events.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#ifndef __GECORUNLOOP_H__
#define __GECORUNLOOP_H__

#include "GECO.h"

/*!
  @enum owd_poller_state
  @discussion
    Each poller object is essentially a state machine, moving
    between the idle and polling state until it is destroyed.
    During destruction the state transitions to "exiting".
    
    The unknown state indicates something truly strange is
    going on.
*/
typedef enum {
  GECORunloopStateUnknown      = 0,
  GECORunloopStateIdle,
  GECORunloopStatePolling,
  GECORunloopStateExiting
} GECORunloopState;

typedef enum {
  GECORunloopSourcePriorityLow = 0,
  GECORunloopSourcePriorityMedium,
  GECORunloopSourcePriorityHigh
} GECORunloopSourcePriority;

typedef struct _GECORunloop * GECORunloopRef;

typedef const void * GECOPollingSource;

typedef void (*GECOPollingSourceDestroyCallback)(GECOPollingSource theSource);
typedef int  (*GECOPollingSourceFileDescriptorCallback)(GECOPollingSource theSource);
typedef bool (*GECOPollingSourceStateCheckCallback)(GECOPollingSource theSource, GECORunloopRef theRunloop);
typedef void (*GECOPollingSourceNotificationCallback)(GECOPollingSource theSource, GECORunloopRef theRunloop);

typedef struct _GECOPollingSourceCallbacks {
  GECOPollingSourceDestroyCallback          destroySource;
  GECOPollingSourceFileDescriptorCallback   fileDescriptorForPolling;
  
  GECOPollingSourceStateCheckCallback       shouldSourceClose;
  GECOPollingSourceStateCheckCallback       willRemoveAsSource;
  
  GECOPollingSourceNotificationCallback     didAddAsSource;
  GECOPollingSourceNotificationCallback     didBeginPolling;
  GECOPollingSourceNotificationCallback     didReceiveDataAvailable;
  GECOPollingSourceNotificationCallback     didEndPolling;
  GECOPollingSourceNotificationCallback     didReceiveClose;
  GECOPollingSourceNotificationCallback     didRemoveAsSource;
} GECOPollingSourceCallbacks;

enum {
  GECOPollingSourceFlagStaticFileDescriptor   = 1 << 0,
  GECOPollingSourceFlagRemoveOnClose          = 1 << 1,
  //
  GECOPollingSourceFlagLowPriority            = 0 << 16,
  GECOPollingSourceFlagMediumPriority         = 1 << 16,
  GECOPollingSourceFlagHighPriority           = 2 << 16,
  GECOPollingSourceFlagPriority               = 3 << 16
};
typedef unsigned int GECOPollingSourceFlags;

//

typedef enum {
  GECORunloopActivityEntry          = 1 << 0,
  GECORunloopActivityBeforeWait     = 1 << 1,
  GECORunloopActivityAfterWait      = 1 << 2,
  GECORunloopActivityBeforeSources  = 1 << 3,
  GECORunloopActivityAfterSources   = 1 << 4,
  GECORunloopActivityExit           = 1 << 5,
  //
  GECORunloopActivityAll            = 0x3F
} GECORunloopActivity;

typedef const void * GECORunloopObserver;

typedef void (*GECORunloopObserverCallback)(GECORunloopObserver theObserver, GECORunloopRef theRunloop, GECORunloopActivity theActivity);

//

GECORunloopRef GECORunloopCreate(void);
void GECORunloopDestroy(GECORunloopRef theRunloop);

GECORunloopState GECORunloopGetState(GECORunloopRef theRunloop);

unsigned int GECORunloopGetGranularity(GECORunloopRef theRunloop);
void GECORunloopSetGranularity(GECORunloopRef theRunloop, unsigned int milliseconds);

bool GECORunloopGetShouldExitRunloop(GECORunloopRef theRunloop);
void GECORunloopSetShouldExitRunloop(GECORunloopRef theRunloop, bool shouldExitRunloop);

unsigned int GECORunloopGetPollingSourceCount(GECORunloopRef theRunloop);

bool GECORunloopGetPollingSourceAtIndex(GECORunloopRef theRunloop, unsigned int index, GECOPollingSource *theSource, GECOPollingSourceCallbacks *callbacks);
bool GECORunloopRemovePollingSourceAtIndex(GECORunloopRef theRunloop, unsigned int index);

bool GECORunloopAddPollingSource(GECORunloopRef theRunloop, GECOPollingSource theSource, GECOPollingSourceCallbacks *callbacks, GECOPollingSourceFlags flags);
bool GECORunloopRemovePollingSource(GECORunloopRef theRunloop, GECOPollingSource theSource);
void GECORunloopRemoveAllPollingSources(GECORunloopRef theRunloop);

void GECORunloopAddObserver(GECORunloopRef theRunloop, GECORunloopObserver theObserver, GECORunloopActivity theActivities, GECORunloopObserverCallback callback, \
                              int rank, bool shouldRepeat);
void GECORunloopRemoveObserver(GECORunloopRef theRunloop, GECORunloopObserver theObserver, GECORunloopActivity theActivities);
void GECORunloopRemoveObservers(GECORunloopRef theRunloop, GECORunloopActivity theActivities);
void GECORunloopRemoveAllObservers(GECORunloopRef theRunloop);

int GECORunloopRun(GECORunloopRef theRunloop);
int GECORunloopRunUntil(GECORunloopRef theRunloop, time_t endTime);



#endif /* __GECORUNLOOP_H__ */

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

#include "GECORunloop.h"
#include "GECOLog.h"

#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/wait.h>

//

typedef struct _GECOPollingSourceRec {
  GECOPollingSource                 theSource;
  GECOFlags                         flags;
  GECOPollingSourceCallbacks        callbacks;
  struct _GECOPollingSourceRec      *link;
} GECOPollingSourceRec;

static GECOPollingSourceRec                *GECOPollingSourceRecPool = NULL;

GECOPollingSourceRec*
__GECOPollingSourceRecAlloc(void)
{
  GECOPollingSourceRec    *newRec = NULL;
  
  if ( GECOPollingSourceRecPool ) {
    newRec = GECOPollingSourceRecPool;
    GECOPollingSourceRecPool = newRec->link;
  } else {
    newRec = malloc(sizeof(*newRec));
  }
  if ( newRec ) memset(newRec, 0, sizeof(*newRec));
  return newRec;
}

//

void
__GECOPollingSourceRecDealloc(
  GECOPollingSourceRec    *theRec
)
{
  theRec->link = GECOPollingSourceRecPool;
  GECOPollingSourceRecPool = theRec;
}

//

#define GECORUNLOOP_NOTIFY_POLLINGSOURCE(R, S, X) { if ( (S)->callbacks.X ) { (S)->callbacks.X(S->theSource, R); } }
#define GECORUNLOOP_NOTIFY_POLLINGSOURCES(R, X) { GECOPollingSourceRec *s = (R)->sources; while (s) { if ( s->callbacks.X ) s->callbacks.X(s->theSource, R); s = s->link; } }

//
#if 0
#pragma mark -
#endif
//

#define GECORunloopObserverActivityCount 6

int __GECORunloopObserverLinkTable[1 << GECORunloopObserverActivityCount] = { -1, 0, 1, -1, 2, -1, -1, -1, 3, -1, -1, -1, -1, -1, -1, -1,
                                                                              4, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                                                              5, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                                                              -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
                                                                            };

enum {
  GECORunloopObserverFlagShouldRepeat     = 1 << 0,
  GECORunloopObserverFlagIsInvalidated    = 1 << 15
};

typedef struct __GECORunloopObserverRec {
  int                               rank;
  GECOFlags                         flags;
  GECORunloopObserver               observer;
  GECORunloopObserverCallback       callback;
  struct __GECORunloopObserverRec   *link;
} GECORunloopObserverRec;

static GECORunloopObserverRec       *GECORunloopObserverRecPool = NULL;

GECORunloopObserverRec*
__GECORunloopObserverRecAlloc(void)
{
  GECORunloopObserverRec            *newRec = NULL;
  
  if ( GECORunloopObserverRecPool ) {
    newRec = GECORunloopObserverRecPool;
    GECORunloopObserverRecPool = newRec->link;
  } else {
    newRec = malloc(sizeof(*newRec));
  }
  if ( newRec ) memset(newRec, 0, sizeof(*newRec));
  return newRec;
}

//

void
__GECORunloopObserverRecDealloc(
  GECORunloopObserverRec      *theRec
)
{
  theRec->link = GECORunloopObserverRecPool;
  GECORunloopObserverRecPool = theRec;
}

//

#define GECORUNLOOP_INVOKE_OBSERVERS(R, A) { GECORunloopObserverRec *o = (R)->observers[__GECORunloopObserverLinkTable[(A)]]; \
                                             while (o) { \
                                               if ( ! GECOFLAGS_ISSET(o->flags, GECORunloopObserverFlagIsInvalidated) ) {\
                                                 o->callback(o->observer, (R), (A)); \
                                                 if ( ! GECOFLAGS_ISSET(o->flags, GECORunloopObserverFlagShouldRepeat) ) GECOFLAGS_SET(o->flags, GECORunloopObserverFlagIsInvalidated); \
                                               } \
                                               o = o->link; \
                                             } \
                                           }

//
#if 0
#pragma mark -
#endif
//

#ifndef GECORUNLOOP_MAX_EPOLL_TIMEOUT
#define GECORUNLOOP_MAX_EPOLL_TIMEOUT      ((INT_MAX / 1000))
#endif

//

enum {
  GECORunloopFlagExitRunloop          = 1 << 0,
  GECORunloopFlagHasDynamicSources    = 1 << 1,
  GECORunloopFlagResetStaticDispatch  = 1 << 2
};

typedef struct _GECORunloop {
  GECOFlags                         flags;
  GECORunloopState                  state;
  int                               epoll_fd;
  unsigned int                      period_in_ms;
  unsigned int                      sourceCount;
  GECOPollingSourceRec              *sources;
  GECORunloopObserverRec            *observers[GECORunloopObserverActivityCount];
} GECORunloop;

//

GECORunloop*
__GECORunloopAlloc(void)
{
  GECORunloop       *newRunloop = NULL;
  
  newRunloop = malloc(sizeof(*newRunloop));
  if ( newRunloop ) {
    memset(newRunloop, 0, sizeof(*newRunloop));
    newRunloop->epoll_fd = -1;
    newRunloop->state = GECORunloopStateIdle;
    newRunloop->period_in_ms = 60000;
  }
  return newRunloop;
}

//

GECORunloopRef
GECORunloopCreate(void)
{
  GECORunloop   *newRunloop = __GECORunloopAlloc();
  
  if ( newRunloop ) {
    newRunloop->epoll_fd = epoll_create(8);
    if ( newRunloop->epoll_fd < 0 ) {
      GECO_WARN("GECORunloopCreate:  failed in epoll_create() (errno = %d)", errno);
      GECORunloopDestroy(newRunloop);
      return NULL;
    }
    GECO_DEBUG("created runloop %p with epoll_fd = %d", newRunloop, newRunloop->epoll_fd);
  }
  return newRunloop;
}

//

void
GECORunloopDestroy(
  GECORunloopRef  theRunloop
)
{
  theRunloop->state = GECORunloopStateExiting;
  
  if ( theRunloop->sourceCount ) GECORunloopRemoveAllPollingSources(theRunloop);
  GECO_DEBUG("removed all polling sources from runloop %p", theRunloop);
  
  int             obIdx = 0;
  
  while ( obIdx < GECORunloopObserverActivityCount ) {
    GECORunloopObserverRec  *node = theRunloop->observers[obIdx], *next = NULL;
    
    while ( node ) {
      next = node->link;
      __GECORunloopObserverRecDealloc(node);
      node = next;
    }
    theRunloop->observers[obIdx] = NULL;
    obIdx++;
  }
  GECO_DEBUG("removed all observers from runloop %p", theRunloop);
  
  if ( theRunloop->epoll_fd >= 0 ) close(theRunloop->epoll_fd);
  GECO_DEBUG("closed polling fd %d for runloop %p", theRunloop->epoll_fd, theRunloop);
  
  free((void*)theRunloop);
  GECO_DEBUG("destroyed runloop %p", theRunloop->epoll_fd, theRunloop);
}

//

GECORunloopState
GECORunloopGetState(
  GECORunloopRef  theRunloop
)
{
  return theRunloop->state;
}

//

unsigned int
GECORunloopGetGranularity(
  GECORunloopRef  theRunloop
)
{
  return theRunloop->period_in_ms;
}
void
GECORunloopSetGranularity(
  GECORunloopRef  theRunloop,
  unsigned int    milliseconds
)
{
  if ( milliseconds <= GECORUNLOOP_MAX_EPOLL_TIMEOUT ) {
    GECO_DEBUG("runloop %p granularity changed, %u => %u", theRunloop, theRunloop->period_in_ms, milliseconds);
    theRunloop->period_in_ms = milliseconds;
  }
}

//

bool
GECORunloopGetShouldExitRunloop(
  GECORunloopRef  theRunloop
)
{
  return GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagExitRunloop) ? true : false;
}
void
GECORunloopSetShouldExitRunloop(
  GECORunloopRef  theRunloop,
  bool            shouldExitRunloop
)
{
  if ( shouldExitRunloop ) {
    GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagExitRunloop);
  } else {
    GECOFLAGS_UNSET(theRunloop->flags, GECORunloopFlagExitRunloop);
  }
}

//

unsigned int
GECORunloopGetPollingSourceCount(
  GECORunloopRef  theRunloop
)
{
  return theRunloop->sourceCount;
}

//

bool
GECORunloopGetPollingSourceAtIndex(
  GECORunloopRef              theRunloop,
  unsigned int                index,
  GECOPollingSource           *theSource,
  GECOPollingSourceCallbacks  *callbacks
)
{
  GECOPollingSourceRec        *node = theRunloop->sources;
  
  while ( node && index-- ) node = node->link;
  if ( node ) {
    if ( theSource ) *theSource = node->theSource;
    if ( callbacks ) *callbacks = node->callbacks;
    return true;
  }
  return false;
}

//

bool
GECORunloopRemovePollingSourceAtIndex(
  GECORunloopRef  theRunloop,
  unsigned int    index
)
{
  GECOPollingSourceRec        *prev = NULL, *node = theRunloop->sources;
  bool                        allStatic = true, exitVal = false;
  
  while ( node && index-- ) {
    if ( ! GECOFLAGS_ISSET(node->flags, GECOPollingSourceFlagStaticFileDescriptor) ) allStatic = false;
    prev = node;
    node = node->link;
  }
  if ( node ) {
    GECOPollingSourceRec      *next = node->link;
    
    GECO_DEBUG("runloop %p removing source %p", theRunloop, node->theSource);
    if ( prev ) {
      prev->link = node->link;
    } else {
      theRunloop->sources = node->link;
    }
    GECO_DEBUG("  notifying source %p -- didRemoveAsSource", node->theSource);
    GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, node, didRemoveAsSource);
    if ( node->callbacks.destroySource ) {
      GECO_DEBUG("  source %p.destroySource()", node->theSource);
      node->callbacks.destroySource(node->theSource);
    }
    __GECOPollingSourceRecDealloc(node);
    GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagResetStaticDispatch);
    exitVal = true;
    theRunloop->sourceCount--;
    node = next;
  }
  while ( node ) {
    if ( ! GECOFLAGS_ISSET(node->flags, GECOPollingSourceFlagStaticFileDescriptor) ) allStatic = false;
    node = node->link;
  }
  if ( exitVal && (GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) != (!allStatic)) ) {
    GECO_DEBUG("runloop %p has changed to %s", theRunloop, ( allStatic ? "static" : "dynamic" ));
    if ( allStatic ) {
      GECOFLAGS_UNSET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
    } else {
      GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
    }
  }
  return exitVal;
}

//

bool
GECORunloopAddPollingSource(
  GECORunloopRef              theRunloop,
  GECOPollingSource           theSource,
  GECOPollingSourceCallbacks  *callbacks,
  GECOPollingSourceFlags      flags
)
{
  GECOPollingSourceRec        *newRec = NULL;
  int                         fd = -1;
  
  if ( ! theSource || ! callbacks->fileDescriptorForPolling ) return false;
  
  newRec = __GECOPollingSourceRecAlloc();
  if ( newRec ) {
    GECOPollingSourceRec      *node = theRunloop->sources, *prev = NULL;
    int                       myPriority = newRec->flags & GECOPollingSourceFlagPriority;
    
    newRec->theSource   = theSource;
    newRec->callbacks   = *callbacks;
    newRec->flags       = flags;
        
    while ( node && ((node->flags & GECOPollingSourceFlagPriority) > myPriority) ) {
      prev = node;
      node = node->link;
    }
    if ( prev ) {
      newRec->link = prev->link;
      prev->link = newRec;
    } else {
      newRec->link = theRunloop->sources;
      theRunloop->sources = newRec;
    }
    
    theRunloop->sourceCount++;
    
    if ( ! GECOFLAGS_ISSET(flags, GECOPollingSourceFlagStaticFileDescriptor) && ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) ) {
      GECO_DEBUG("runloop %p has changed to dynamic", theRunloop);
      GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
    }
    GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagResetStaticDispatch);
    if ( callbacks->didAddAsSource ) callbacks->didAddAsSource(theSource, theRunloop);
    return true;
  }
  return false;
}

//

bool
GECORunloopRemovePollingSource(
  GECORunloopRef    theRunloop,
  GECOPollingSource theSource
)
{
  GECOPollingSourceRec        *prev = NULL, *node = theRunloop->sources;
  bool                        allStatic = true, exitVal = false;
  
  while ( node ) {
    if ( node->theSource == theSource ) {
      GECO_DEBUG("runloop %p removing source %p", theRunloop, node->theSource);
      if ( prev ) {
        prev->link = node->link;
      } else {
        theRunloop->sources = node->link;
      }
      GECO_DEBUG("  notifying source %p -- didRemoveAsSource", node->theSource);
      GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, node, didRemoveAsSource);
      if ( node->callbacks.destroySource ) {
        GECO_DEBUG("  source %p.destroySource()", node->theSource);
        node->callbacks.destroySource(node->theSource);
      }
      __GECOPollingSourceRecDealloc(node);
      GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagResetStaticDispatch);
      theRunloop->sourceCount--;
      exitVal = true;
    } else if ( ! GECOFLAGS_ISSET(node->flags, GECOPollingSourceFlagStaticFileDescriptor) ) {
      allStatic = false;
    }
    node = node->link;
  }
  if ( exitVal && (GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) != (!allStatic)) ) {
    GECO_DEBUG("runloop %p has changed to %s", theRunloop, ( allStatic ? "static" : "dynamic" ));
    if ( allStatic ) {
      GECOFLAGS_UNSET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
    } else {
      GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
    }
  }
  return exitVal;
}

//

void
GECORunloopRemoveAllPollingSources(
  GECORunloopRef  theRunloop
)
{
  GECOPollingSourceRec        *next = NULL, *node = theRunloop->sources;
  
  while ( node ) {
    next = node->link;
    GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, node, didRemoveAsSource);
    if ( node->callbacks.destroySource ) node->callbacks.destroySource(node->theSource);
    __GECOPollingSourceRecDealloc(node);
    node = next;
  }
  GECOFLAGS_SET(theRunloop->flags, GECORunloopFlagResetStaticDispatch);
  GECOFLAGS_UNSET(theRunloop->flags, GECORunloopFlagHasDynamicSources);
  GECO_DEBUG("all sources removed from runloop %p", theRunloop);
  theRunloop->sourceCount = 0;
}

//

void
GECORunloopAddObserver(
  GECORunloopRef              theRunloop,
  GECORunloopObserver         theObserver,
  GECORunloopActivity         theActivities,
  GECORunloopObserverCallback callback,
  int                         rank,
  bool                        shouldRepeat
)
{
  unsigned int                activityMask = 1;
  int                         obIdx = 0;
  
  while ( obIdx < GECORunloopObserverActivityCount ) {
    if ( (theActivities & activityMask) == activityMask ) {
      GECORunloopObserverRec         *newObserver = __GECORunloopObserverRecAlloc();
      
      if ( newObserver ) {
        newObserver->rank = rank;
        newObserver->flags = ( shouldRepeat ? GECORunloopObserverFlagShouldRepeat : 0 );
        newObserver->observer = theObserver;
        newObserver->callback = callback;
        
        if ( theRunloop->observers[obIdx] ) {
          GECORunloopObserverRec  *node = theRunloop->observers[obIdx], *prev = NULL;
          
          while ( node ) {
            if ( node->rank > rank ) {
              if ( prev ) {
                newObserver->link = node;
                prev->link = newObserver;
              } else {
                newObserver = node;
                theRunloop->observers[obIdx] = newObserver;
              }
              break;
            }
            prev = node;
            node = node->link;
          }
        } else {
          theRunloop->observers[obIdx] = newObserver;
        }
      }
    }
    activityMask <<= 1;
    obIdx++;
  }
}

//

void
GECORunloopRemoveObserver(
  GECORunloopRef        theRunloop,
  GECORunloopObserver   theObserver,
  GECORunloopActivity   theActivities
)
{
  unsigned int                activityMask = 1;
  int                         obIdx = 0;
  
  while ( obIdx < GECORunloopObserverActivityCount ) {
    if ( (theActivities & activityMask) == activityMask ) {
      GECORunloopObserverRec  *node = theRunloop->observers[obIdx], *prev = NULL;
      
      while ( node ) {
        if ( node->observer == theObserver ) {
          if ( prev ) {
            prev->link = node->link;
          } else {
            theRunloop->observers[obIdx] = node->link;
          }
          __GECORunloopObserverRecDealloc(node);
          break;
        }
        prev = node;
        node = node->link;
      }
    }
    activityMask <<= 1;
    obIdx++;
  }
}

//

void
GECORunloopRemoveObservers(
  GECORunloopRef        theRunloop,
  GECORunloopActivity   theActivities
)
{
  unsigned int                activityMask = 1;
  int                         obIdx = 0;
  
  while ( obIdx < GECORunloopObserverActivityCount ) {
    if ( (theActivities & activityMask) == activityMask ) {
      GECORunloopObserverRec  *node = theRunloop->observers[obIdx], *next = NULL;
      
      while ( node ) {
        next = node->link;
        __GECORunloopObserverRecDealloc(node);
        node = next;
      }
      theRunloop->observers[obIdx] = NULL;
    }
    activityMask <<= 1;
    obIdx++;
  }
}

//

void
GECORunloopRemoveAllObservers(
  GECORunloopRef        theRunloop
)
{
  int                         obIdx = 0;
  
  while ( obIdx < GECORunloopObserverActivityCount ) {
    GECORunloopObserverRec  *node = theRunloop->observers[obIdx], *next = NULL;
    
    while ( node ) {
      next = node->link;
      __GECORunloopObserverRecDealloc(node);
      node = next;
    }
    theRunloop->observers[obIdx] = NULL;
    obIdx++;
  }
}

//

int
GECORunloopRun(
  GECORunloopRef  theRunloop
)
{
  return GECORunloopRunUntil(theRunloop, (time_t)0);
}

//

typedef struct _GECORunloopDispatch {
  int                     fd;
  GECOPollingSourceRec    *sourceRec;
} GECORunloopDispatch;

int
__GECORunloopRunUntil_Dynamic(
  GECORunloopRef  theRunloop,
  time_t          endTime
)
{
  int                   rc = 0;
  bool                  running = true;
  
  GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityEntry);
  
  while ( running && ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagExitRunloop) && (theRunloop->state != GECORunloopStateExiting) ) {
    int             timeout;
    time_t          now = time(NULL);
    
    //
    // Try to reap any child processes that have exited:
    //
    {
      int   status;
      pid_t childPid;
      
      while ( (childPid = waitpid(-1, &status, WNOHANG)) > 0 ) {
        if ( WIFEXITED(status) ) {
          GECO_DEBUG("child process %ld exited with status %d", childPid, WEXITSTATUS(status));
        }
      }
    }
    
    //
    // Runtime exceeded?
    //
    if ( (endTime > 0) && (now >= endTime) ) {
      GECO_DEBUG("runtime limit exceeded -- %lld >= %lld", (long long)now, (long long)endTime);
      break;
    }
    
    //
    // Has the runloop changed to being static?
    //
    if ( ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) ) {
      GECO_DEBUG("runloop changed from dynamic to static");
      rc = INT_MAX;
      break;
    }
    
    //
    // Figure out how long we can sleep:
    //
    timeout = 1000 * (endTime - now);
    if ( theRunloop->period_in_ms > 0 ) {
      if ( timeout > theRunloop->period_in_ms ) timeout = theRunloop->period_in_ms;
    } else if ( endTime - now >= GECORUNLOOP_MAX_EPOLL_TIMEOUT ) {
      timeout = GECORUNLOOP_MAX_EPOLL_TIMEOUT;
    }
    GECO_DEBUG("runloop timeout calculated as %d", timeout);
      
    //
    // If there are no data sources then just go to sleep...
    //
    if ( theRunloop->sourceCount == 0 ) {
      GECO_INFO("no sources in runloop, going to sleep");
      GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
      GECOSleepForMicroseconds(timeout);
      GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
      continue;
    } else {
      unsigned int            dispatchCount = 0;
      GECORunloopDispatch     dispatchTable[theRunloop->sourceCount];
      GECOPollingSourceRec    *source;
      
      //
      // Ensure we have an epoll descriptor open:
      //
      if ( theRunloop->epoll_fd < 0 ) {
        theRunloop->epoll_fd = epoll_create(8);
        if ( theRunloop->epoll_fd < 0 ) {
          GECO_WARN("__GECORunloopRunUntil_Dynamic: failure in epoll_create (errno = %d)", errno);
          return errno;
        }
      }
  
      //
      // Fill-in the dispatch table, registering the descriptors with epoll
      // as we go:
      //
      source = theRunloop->sources;
      while ( source ) {
        int                   fdOfInterest = source->callbacks.fileDescriptorForPolling(source->theSource);
        
        if ( fdOfInterest >= 0 ) {
          struct epoll_event  ev = {
                                  .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
                                  .data.fd = fdOfInterest
                                };
          if ( epoll_ctl(theRunloop->epoll_fd, EPOLL_CTL_ADD, fdOfInterest, &ev) == 0 ) {
            dispatchTable[dispatchCount].fd = fdOfInterest;
            dispatchTable[dispatchCount].sourceRec = source;
            dispatchCount++;
            GECO_DEBUG("registered fd %d with epoll fd %d", fdOfInterest, theRunloop->epoll_fd);
          } else {
            GECO_WARN("__GECORunloopRunUntil_Dynamic: failed to register fd %d with epoll fd %d (errno = %d)", fdOfInterest, theRunloop->epoll_fd, errno);
          }
        }
        source = source->link;
      }
      GECO_DEBUG("dispatch table constructed with %u descriptors", dispatchCount);
  
      //
      // If we didn't actually register any sources, just sleep...
      //
      if ( dispatchCount == 0 ) {
        GECO_INFO("no sources registered with epoll, going to sleep");
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
        GECOSleepForMicroseconds(timeout);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
      } else {
        struct epoll_event      responseBuffer[dispatchCount];
        
        //
        // Enter polling state:
        //
        theRunloop->state = GECORunloopStatePolling;
        
        //
        // Let any interested sources know:
        //
        GECO_DEBUG("notifying all sources -- didBeginPolling");
        GECORUNLOOP_NOTIFY_POLLINGSOURCES(theRunloop, didBeginPolling);
        
        //
        // Go to sleep and await an event or two:
        //
        GECO_DEBUG("entering epoll_wait(%d, %p, %u, %d)...", theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
        int eventCount = epoll_wait(theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout);
        GECO_DEBUG("...exited epoll_wait(%d, %p, %u, %d) = %d", theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout, eventCount);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
        
        //
        // Back to idle state:
        //
        theRunloop->state = GECORunloopStateIdle;
        
        //
        // Let any interested sources know we've exited epoll:
        //
        GECO_DEBUG("notifying all sources -- didEndPolling");
        GECORUNLOOP_NOTIFY_POLLINGSOURCES(theRunloop, didEndPolling);
        
        if ( eventCount < 0 ) {
          //
          // If we were interrupted by a signal we can keep going, otherwise
          // it's time to get outta here:
          //
          if ( errno != EINTR ) {
            GECO_WARN("__GECORunloopRunUntil_Dynamic: error during runloop causing early exit (errno = %d)", errno);
            rc = errno;
            running = false;
          } else {
            GECO_DEBUG("polling loopus interruptus (errno = %d)", errno);
          }
        } else if ( (eventCount > 0) && ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagExitRunloop) ) {
          int           dispatchIdx, eventIdx;

#ifndef GECO_DEBUG_DISABLE
          //
          // Debugging -- show all event fds:
          //
          eventIdx = 0;
          while ( eventIdx < eventCount ) {
            GECO_DEBUG("  event %08X on fd %d", responseBuffer[eventIdx].events, responseBuffer[eventIdx].data.fd);
            eventIdx++;
          }
#endif
          
          //
          // Walk the dispatch table:
          //
          dispatchIdx = 0;
          GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeSources);
          while ( dispatchIdx < dispatchCount ) {
            int                     fdToMatch = dispatchTable[dispatchIdx].fd;
            GECOPollingSourceRec    *source = dispatchTable[dispatchIdx].sourceRec;
            
            eventIdx = 0;
            while ( eventIdx < eventCount ) {
              if ( fdToMatch == responseBuffer[eventIdx].data.fd ) {
                if ( responseBuffer[eventIdx].events & EPOLLIN ) {
                  //
                  // Data present for read on file descriptor:
                  //
                  GECO_DEBUG("notifying source for fd %d -- didReceiveDataAvailable", fdToMatch);
                  GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, source, didReceiveDataAvailable);
                }
                if ( (responseBuffer[eventIdx].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) || (source->callbacks.shouldSourceClose && source->callbacks.shouldSourceClose(source->theSource, theRunloop)) ) {
                  //
                  // Cleanup after a closed descriptor:
                  //
                  GECO_DEBUG("notifying source for closed fd %d", fdToMatch);
                  GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, source, didReceiveClose);
                  if ( GECOFLAGS_ISSET(source->flags, GECOPollingSourceFlagRemoveOnClose) ) {
                    GECO_DEBUG("removing source for closed fd %d", fdToMatch);
                    GECORunloopRemovePollingSource(theRunloop, source->theSource);
                  }
                }
                break;
              }
              eventIdx++;
            }
            dispatchIdx++;
          }
          GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterSources);
        }
        
        //
        // Unregister all the polling fds:
        //
        GECO_DEBUG("clearing %d sources from the dispatch table", dispatchCount);
        while ( dispatchCount-- ) {
          struct epoll_event  ev = {
                                  .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
                                  .data.fd = dispatchTable[dispatchCount].fd
                                };
          epoll_ctl(theRunloop->epoll_fd, EPOLL_CTL_DEL, dispatchTable[dispatchCount].fd, &ev);
        }
      }
    }
  }
  
  GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityExit);
  
  return rc;
}

int
__GECORunloopRunUntil_Static(
  GECORunloopRef  theRunloop,
  time_t          endTime
)
{
  int                   rc = 0;
  bool                  running = true;
  unsigned int          dispatchCount = 0, dispatchCapacity = 0;
  GECORunloopDispatch   *dispatchTable = NULL;
  
  GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityEntry);
  
  while ( running && ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagExitRunloop) && (theRunloop->state != GECORunloopStateExiting) ) {
    int             timeout;
    time_t          now = time(NULL);
    
    //
    // Try to reap any child processes that have exited:
    //
    {
      int   status;
      pid_t childPid;
      
      while ( (childPid = waitpid(-1, &status, WNOHANG)) > 0 ) {
        if ( WIFEXITED(status) ) {
          GECO_DEBUG("child process %ld exited with status %d", childPid, WEXITSTATUS(status));
        }
      }
    }
    
    //
    // Runtime exceeded?
    //
    if ( (endTime > 0) && (now >= endTime) ) {
      GECO_DEBUG("runtime limit exceeded -- %lld >= %lld", (long long)now, (long long)endTime);
      break;
    }
    
    //
    // Has the runloop changed to being dynamic?
    //
    if ( GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) ) {
      GECO_DEBUG("runloop changed from static to dynamic");
      rc = INT_MAX;
      break;
    }
    
    //
    // Figure out how long we can sleep:
    //
    timeout = 1000 * (endTime - now);
    if ( theRunloop->period_in_ms > 0 ) {
      if ( timeout > theRunloop->period_in_ms ) timeout = theRunloop->period_in_ms;
    } else if ( endTime - now >= GECORUNLOOP_MAX_EPOLL_TIMEOUT ) {
      timeout = GECORUNLOOP_MAX_EPOLL_TIMEOUT;
    }
    GECO_DEBUG("runloop timeout calculated as %d", timeout);
      
    //
    // If there are no data sources then just go to sleep...
    //
    if ( theRunloop->sourceCount == 0 ) {
      GECO_INFO("no sources in runloop, going to sleep");
      GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
      GECOSleepForMicroseconds(timeout);
      GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
      continue;
    } else {
      GECOPollingSourceRec    *source;
      
      //
      // Ensure we have an epoll descriptor open:
      //
      if ( theRunloop->epoll_fd < 0 ) {
        theRunloop->epoll_fd = epoll_create(8);
        if ( theRunloop->epoll_fd < 0 ) {
          GECO_WARN("__GECORunloopRunUntil_Static: failure in epoll_create (errno = %d)", errno);
          return errno;
        }
      }
  
      //
      // Fill-in the dispatch table, registering the descriptors with epoll
      // as we go:
      //
      if ( ! dispatchTable || (theRunloop->sourceCount > dispatchCapacity) ) {
        GECORunloopDispatch   *newDispatchTable = realloc(dispatchTable, theRunloop->sourceCount * sizeof(GECORunloopDispatch));
        
        if ( ! newDispatchTable ) {
          if ( dispatchTable ) free((void*)dispatchTable);
          GECO_WARN("__GECORunloopRunUntil_Static: failed to allocate dispatch table (errno = %d)", errno);
          return ENOMEM;
        }
        dispatchTable = newDispatchTable;
        dispatchCapacity = theRunloop->sourceCount;
      }
      if ( ! dispatchCount ) {
        GECO_DEBUG("reconstructing dispatch table using %u sources", theRunloop->sourceCount);
        source = theRunloop->sources;
        while ( source ) {
          int                   fdOfInterest = source->callbacks.fileDescriptorForPolling(source->theSource);
          
          if ( fdOfInterest >= 0 ) {
            struct epoll_event  ev = {
                                    .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
                                    .data.fd = fdOfInterest
                                  };
            if ( epoll_ctl(theRunloop->epoll_fd, EPOLL_CTL_ADD, fdOfInterest, &ev) == 0 ) {
              dispatchTable[dispatchCount].fd = fdOfInterest;
              dispatchTable[dispatchCount].sourceRec = source;
              dispatchCount++;
              GECO_DEBUG("registered fd %d with epoll fd %d", fdOfInterest, theRunloop->epoll_fd);
            } else {
              GECO_WARN("__GECORunloopRunUntil_Static: failed to register fd %d with epoll fd %d (errno = %d)", fdOfInterest, theRunloop->epoll_fd, errno);
            }
          }
          source = source->link;
        }
        GECOFLAGS_UNSET(theRunloop->flags, GECORunloopFlagResetStaticDispatch);
      }
      GECO_DEBUG("dispatch table constructed with %u descriptors", dispatchCount);
      
      //
      // If we didn't actually register any sources, just sleep...
      //
      if ( dispatchCount == 0 ) {
        GECO_INFO("no sources registered with epoll, going to sleep");
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
        GECOSleepForMicroseconds(timeout);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
      } else {
        struct epoll_event      responseBuffer[dispatchCount];
        
        //
        // Enter polling state:
        //
        theRunloop->state = GECORunloopStatePolling;
        
        //
        // Let any interested sources know:
        //
        GECO_DEBUG("notifying all sources -- didBeginPolling");
        GECORUNLOOP_NOTIFY_POLLINGSOURCES(theRunloop, didBeginPolling);
        
        //
        // Go to sleep and await an event or two:
        //
        GECO_DEBUG("entering epoll_wait(%d, %p, %u, %d)...", theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeWait);
        int eventCount = epoll_wait(theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout);
        GECO_DEBUG("...exited epoll_wait(%d, %p, %u, %d) = %d", theRunloop->epoll_fd, responseBuffer, dispatchCount, timeout, eventCount);
        GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterWait);
        
        //
        // Back to idle state:
        //
        theRunloop->state = GECORunloopStateIdle;
        
        //
        // Let any interested sources know we've exited epoll:
        //
        GECO_DEBUG("notifying all sources -- didEndPolling");
        GECORUNLOOP_NOTIFY_POLLINGSOURCES(theRunloop, didEndPolling);
        
        if ( eventCount < 0 ) {
          //
          // If we were interrupted by a signal we can keep going, otherwise
          // it's time to get outta here:
          //
          if ( errno != EINTR ) {
            GECO_WARN("__GECORunloopRunUntil_Static: error during runloop causing early exit (errno = %d)", errno);
            rc = errno;
            running = false;
          }
        } else if ( (eventCount > 0) && ! GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagExitRunloop) ) {
          int           dispatchIdx, eventIdx;

#ifndef GECO_DEBUG_DISABLE
          //
          // Debugging -- show all event fds:
          //
          eventIdx = 0;
          while ( eventIdx < eventCount ) {
            GECO_DEBUG("  event %08X on fd %d", responseBuffer[eventIdx].events, responseBuffer[eventIdx].data.fd);
            eventIdx++;
          }
#endif

          //
          // Walk the dispatch table:
          //
          dispatchIdx = 0;
          GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityBeforeSources);
          while ( dispatchIdx < dispatchCount ) {
            int                     fdToMatch = dispatchTable[dispatchIdx].fd;
            GECOPollingSourceRec    *source = dispatchTable[dispatchIdx].sourceRec;
            
            eventIdx = 0;
            while ( eventIdx < eventCount ) {
              if ( fdToMatch == responseBuffer[eventIdx].data.fd ) {
                if ( responseBuffer[eventIdx].events & EPOLLIN ) {
                  //
                  // Data present for read on file descriptor:
                  //
                  GECO_DEBUG("notifying source for fd %d -- didReceiveDataAvailable", fdToMatch);
                  GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, source, didReceiveDataAvailable);
                }
                if ( (responseBuffer[eventIdx].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) || (source->callbacks.shouldSourceClose && source->callbacks.shouldSourceClose(source->theSource, theRunloop)) ) {
                  //
                  // Cleanup after a closed descriptor:
                  //
                  GECO_DEBUG("notifying source for closed fd %d", fdToMatch);
                  GECORUNLOOP_NOTIFY_POLLINGSOURCE(theRunloop, source, didReceiveClose);
                  if ( GECOFLAGS_ISSET(source->flags, GECOPollingSourceFlagRemoveOnClose) ) {
                    GECO_DEBUG("removing source for closed fd %d", fdToMatch);
                    GECORunloopRemovePollingSource(theRunloop, source->theSource);
                  }
                }
                break;
              }
              eventIdx++;
            }
            dispatchIdx++;
          }
          GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityAfterSources);
        }
      }
      
      //
      // Do we need to redo the dispatch table?
      //
      if ( GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagResetStaticDispatch) ) {
        GECO_DEBUG("clearing %d sources from the dispatch table", dispatchCount);
        while ( dispatchCount-- ) {
          struct epoll_event  ev = {
                                  .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
                                  .data.fd = dispatchTable[dispatchCount].fd
                                };
          epoll_ctl(theRunloop->epoll_fd, EPOLL_CTL_DEL, dispatchTable[dispatchCount].fd, &ev);
        }
        dispatchCount = 0;
      }
    }
  }
  
  if ( dispatchTable ) {
    GECO_DEBUG("clearing %d sources from the dispatch table", dispatchCount);
    while ( dispatchCount-- ) {
      struct epoll_event  ev = {
                              .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
                              .data.fd = dispatchTable[dispatchCount].fd
                            };
      epoll_ctl(theRunloop->epoll_fd, EPOLL_CTL_DEL, dispatchTable[dispatchCount].fd, &ev);
    }
    free((void*)dispatchTable);
  }
  
  GECORUNLOOP_INVOKE_OBSERVERS(theRunloop, GECORunloopActivityExit);
  
  return rc;
}

int
GECORunloopRunUntil(
  GECORunloopRef  theRunloop,
  time_t          endTime
)
{
  int             rc = 0;
  
  while ( 1 ) {
    rc = GECOFLAGS_ISSET(theRunloop->flags, GECORunloopFlagHasDynamicSources) ? __GECORunloopRunUntil_Dynamic(theRunloop, endTime) : __GECORunloopRunUntil_Static(theRunloop, endTime);
    if ( rc != INT_MAX ) break;
  }
  return rc;
}

//

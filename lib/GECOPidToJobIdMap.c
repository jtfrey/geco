/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOPidToJobIdMap.c
 *
 *  Manage mappings of PID => (jobId, taskId)
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOPidToJobIdMap.h"
#include "GECOLog.h"

//

uint32_t
__GECOPidToJobIdMapPidHashFunction(
  pid_t   aPid
)
{
  uint32_t  hashVal = 0;
  uint8_t   *pidAsBytes = (uint8_t*)&aPid;
  int       pidByteSize = sizeof(pid_t);
  
  // CRC-style hash:
  hashVal = pidAsBytes[0];
  hashVal = ((hashVal << 5) ^ ((hashVal & 0xf8000000) >> 27)) ^ pidAsBytes[1];
  hashVal = ((hashVal << 5) ^ ((hashVal & 0xf8000000) >> 27)) ^ pidAsBytes[2];
  hashVal = ((hashVal << 5) ^ ((hashVal & 0xf8000000) >> 27)) ^ pidAsBytes[3];
  return hashVal;
}

//

typedef struct _GECOPidToJobIdMapNode {
  pid_t                           thePid;
  long int                        jobId, taskId;
  struct _GECOPidToJobIdMapNode   *link;
} GECOPidToJobIdMapNode;

//

GECOPidToJobIdMapNode*
__GECOPidToJobIdMapNodeInit(
  GECOPidToJobIdMapNode *aNode
)
{
  if ( aNode ) {
    aNode->thePid = -1;
    aNode->jobId = -1;
    aNode->taskId = -1;
    aNode->link = NULL;
  }
  return aNode;
}

//

GECOPidToJobIdMapNode*
__GECOPidToJobIdMapNodeAlloc(void)
{
  return __GECOPidToJobIdMapNodeInit(malloc(sizeof(GECOPidToJobIdMapNode)));
}

//
#if 0
#pragma mark -
#endif
//

#ifndef GECOPIDTOJOBIDMAP_HASH_SIZE
#define GECOPIDTOJOBIDMAP_HASH_SIZE 64
#endif
const unsigned int GECOPidToJobIdMapHashSize = GECOPIDTOJOBIDMAP_HASH_SIZE;

#ifndef GECOPIDTOJOBIDMAP_POOL_SIZE
#define GECOPIDTOJOBIDMAP_POOL_SIZE 8
#endif
const unsigned int GECOPidToJobIdMapPoolSize = GECOPIDTOJOBIDMAP_POOL_SIZE;

//

typedef struct _GECOPidToJobIdMap {
  unsigned int              tableSize, nodeCount;
  GECOPidToJobIdMapNode     *nodePool, *staticPoolStart, *staticPoolEnd;
  GECOPidToJobIdMapNode*    nodeTable[0];
} GECOPidToJobIdMap;

//

GECOPidToJobIdMap*
__GECOPidToJobIdMapAlloc(
  unsigned int      tableSize
)
{
  GECOPidToJobIdMap     *newMap;
  
  if ( tableSize <= 1 ) tableSize = GECOPidToJobIdMapHashSize;
  
  newMap = malloc(sizeof(GECOPidToJobIdMap) + tableSize * sizeof(GECOPidToJobIdMapNode*) + GECOPidToJobIdMapPoolSize * sizeof(GECOPidToJobIdMapNode));
  if ( newMap ) {
    newMap->tableSize = tableSize;
    newMap->nodeCount = 0;
    if ( GECOPidToJobIdMapPoolSize > 0 ) {
      GECOPidToJobIdMapNode *node;
      
      newMap->staticPoolStart = node = (GECOPidToJobIdMapNode*)(((void*)newMap) + sizeof(GECOPidToJobIdMap) + tableSize * sizeof(GECOPidToJobIdMapNode*));
      newMap->staticPoolEnd = newMap->staticPoolStart + GECOPidToJobIdMapPoolSize;
      newMap->nodePool = NULL;
      while ( node < newMap->staticPoolEnd ) {
        __GECOPidToJobIdMapNodeInit(node);
        node->link = newMap->nodePool;
        newMap->nodePool = node;
        node++;
      }
    }
    while ( tableSize-- ) newMap->nodeTable[tableSize] = NULL;
  }
  return newMap;
}

//

void
__GECOPidToJobIdMapDealloc(
  GECOPidToJobIdMap     *aMap
)
{
  GECOPidToJobIdMapNode *node, *next;
  unsigned int          i = 0;
  
  // Drop all nodes in the table:
  while ( i < aMap->tableSize ) {
    node = aMap->nodeTable[i++];
    while ( node ) {
      next = node->link;
      if ( node < aMap->staticPoolStart || node >= aMap->staticPoolEnd ) free((void*)node);
      node = next;
    }
  }
  
  // Drop all nodes in the pool:
  node = aMap->nodePool;
  while ( node ) {
    next = node->link;
    if ( node < aMap->staticPoolStart || node >= aMap->staticPoolEnd ) free((void*)node);
    node = next;
  }
  
  free((void*)aMap);
}

//

GECOPidToJobIdMapNode*
__GECOPidToJobIdMapAllocNode(
  GECOPidToJobIdMap     *aMap
)
{
  GECOPidToJobIdMapNode *newNode;
  
  if ( aMap->nodePool ) {
    newNode = aMap->nodePool;
    aMap->nodePool = newNode->link;
  } else {
    newNode = __GECOPidToJobIdMapNodeAlloc();
  }
  return newNode;
}

//

void
__GECOPidToJobIdMapDeallocNode(
  GECOPidToJobIdMap     *aMap,
  GECOPidToJobIdMapNode *aNode
)
{
  __GECOPidToJobIdMapNodeInit(aNode);
  aNode->link = aMap->nodePool;
  aMap->nodePool = aNode;
}

//
#if 0
#pragma mark -
#endif
//

GECOPidToJobIdMapRef
GECOPidToJobIdMapCreate(
  unsigned int    tableSize
)
{
  return __GECOPidToJobIdMapAlloc(tableSize);
}

//

void
GECOPidToJobIdMapDestroy(
  GECOPidToJobIdMapRef  aMap
)
{
  __GECOPidToJobIdMapDealloc(aMap);
}

//

bool
GECOPidToJobIdMapHasJobAndTaskId(
  GECOPidToJobIdMapRef  aMap,
  long int              jobId,
  long int              taskId
)
{
  GECOPidToJobIdMapNode *node, *next;
  unsigned int          i = 0;
  
  // Drop all nodes in the table:
  while ( i < aMap->tableSize ) {
    node = aMap->nodeTable[i++];
    while ( node ) {
      if ( (node->jobId == jobId) && (node->taskId == taskId) ) return true;
      node = node->link;
    }
  }
  return false;
}

//

bool
GECOPidToJobIdMapGetJobAndTaskIdForPid(
  GECOPidToJobIdMapRef  aMap,
  pid_t                 aPid,
  long int              *jobId,
  long int              *taskId
)
{
  unsigned int          i = __GECOPidToJobIdMapPidHashFunction(aPid) % aMap->tableSize;
  GECOPidToJobIdMapNode *node = aMap->nodeTable[i];
  
  while ( node ) {
    if ( node->thePid == aPid ) {
      *jobId = node->jobId;
      *taskId = node->taskId;
      return true;
    }
    if ( node->thePid > aPid ) break;
    node = node->link;
  }
  return false;
}

//

bool
GECOPidToJobIdMapAddPid(
  GECOPidToJobIdMapRef  aMap,
  pid_t                 aPid,
  long int              jobId,
  long int              taskId
)
{
  unsigned int          i = __GECOPidToJobIdMapPidHashFunction(aPid) % aMap->tableSize;
  GECOPidToJobIdMapNode *prev = NULL, *node = aMap->nodeTable[i];
  
  while ( node ) {
    if ( node->thePid == aPid ) return true;
    if ( node->thePid > aPid ) break;
    prev = node;
    node = node->link;
  }
  
  GECOPidToJobIdMapNode *newNode = __GECOPidToJobIdMapAllocNode(aMap);
  if ( newNode ) {
    newNode->thePid = aPid;
    newNode->jobId = jobId;
    newNode->taskId = taskId;
    
    if ( prev ) {
      newNode->link = prev->link;
      prev->link = newNode;
    } else {
      newNode->link = aMap->nodeTable[i];
      aMap->nodeTable[i] = newNode;
    }
    GECO_DEBUG("added mapping pid(%ld) => (%ld, %ld) at hash index %u", aPid, jobId, taskId, i);
    return true;
  }
  return false;
}

//

void
GECOPidToJobIdMapRemovePid(
  GECOPidToJobIdMapRef  aMap,
  pid_t                 aPid
)
{
  unsigned int          i = __GECOPidToJobIdMapPidHashFunction(aPid) % aMap->tableSize;
  GECOPidToJobIdMapNode *prev = NULL, *node = aMap->nodeTable[i];
  
  while ( node ) {
    if ( node->thePid == aPid ) {
      GECO_DEBUG("removed mapping pid(%ld) => (%ld, %ld) at hash index %u", aPid, node->jobId, node->taskId, i);
      if ( prev ) {
        prev->link = node->link;
      } else {
        aMap->nodeTable[i] = node->link;
      }
      __GECOPidToJobIdMapDeallocNode(aMap, node);
      break;
    }
    if ( node->thePid > aPid ) break;
    prev = node;
    node = node->link;
  }
}

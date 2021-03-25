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

#include "GECOResource.h"

#include <pwd.h>
#include <grp.h>

//

#ifndef GECORESOURCE_NODENAME_MAX
#define GECORESOURCE_NODENAME_MAX 16
#endif

#ifndef GECORESOURCE_GPULIST_MAX
#define GECORESOURCE_GPULIST_MAX 24
#endif

#ifndef GECORESOURCE_PHILIST_MAX
#define GECORESOURCE_PHILIST_MAX 24
#endif

#ifndef GECORESOURCE_QSTAT_CMD
#define GECORESOURCE_QSTAT_CMD    "qstat"
#endif
static char     *GECOResourceQstatCmd = GECORESOURCE_QSTAT_CMD;

#ifndef GECO_GE_CELL_PREFIX
#error GECO_GE_CELL_PREFIX must be defined for the build
#endif
static char     *GECOResourceGECellPrefix = GECO_GE_CELL_PREFIX;

//

typedef struct _GECOResourcePerNode {
  const char                  *nodeName;
  bool                        isSlave;
  GECOResourcePerNodeData     perNodeData;
  struct _GECOResourcePerNode *link;
} GECOResourcePerNode;

GECOResourcePerNode           *perNodePool = NULL;

//

GECOResourcePerNode*
__GECOResourcePerNodeAlloc(void)
{
  GECOResourcePerNode   *newRecord = NULL;
  
  if ( perNodePool ) {
    newRecord = perNodePool;
    perNodePool = newRecord->link;
  } else {
    newRecord = malloc(sizeof(GECOResourcePerNode) + GECORESOURCE_NODENAME_MAX + GECORESOURCE_GPULIST_MAX + GECORESOURCE_PHILIST_MAX);
  }
  if ( newRecord ) {
    void                  *p = ((void*)newRecord) + sizeof(*newRecord);
    
    memset(newRecord, 0, sizeof(*newRecord) + GECORESOURCE_NODENAME_MAX + GECORESOURCE_GPULIST_MAX + GECORESOURCE_PHILIST_MAX);
    
    newRecord->nodeName            = p; p += GECORESOURCE_NODENAME_MAX;
    newRecord->perNodeData.gpuList = p; p += GECORESOURCE_GPULIST_MAX;
    newRecord->perNodeData.phiList = p; p += GECORESOURCE_PHILIST_MAX;
  }
  return newRecord;
}

//

void
__GECOResourcePerNodeDealloc(
  GECOResourcePerNode   *oldRecord
)
{
  oldRecord->link = perNodePool;
  perNodePool = oldRecord;
}

//

const char*
GECOResourcePerNodeGetNodeName(
  GECOResourcePerNodeRef    aResource
)
{
  return aResource->nodeName;
}

//

bool
GECOResourcePerNodeGetIsSlave(
  GECOResourcePerNodeRef    aResource
)
{
  return aResource->isSlave;
}

//

void
GECOResourcePerNodeGetNodeData(
  GECOResourcePerNodeRef    aResource,
  GECOResourcePerNodeData   *perNodeData
)
{
  *perNodeData = aResource->perNodeData;
}

//
#if 0
#pragma mark -
#endif
//

typedef struct _GECOResourceSet {
  long int              jobId, taskId;
  const char            *jobName;
  
  const char            *ownerUname;
  uid_t                 ownerUid;
  bool                  isOwnerUidSet;
  const char            *ownerGname;
  gid_t                 ownerGid;
  bool                  isOwnerGidSet;
  
  const char            *workingDirectory;
  
  bool                  isArrayJob;
  bool                  isStandby;
  bool                  shouldConfigPhiForUser;
  GECOLogLevel          traceLevel;
  double                runtimeLimit;
  double                perSlotVirtualMemoryLimit;
  
  unsigned int          nodeCount;
  GECOResourcePerNode   *perNodeList;
} GECOResourceSet;

//

GECOResourceSet*
__GECOResourceSetAlloc(void)
{
  GECOResourceSet     *newSet = malloc(sizeof(GECOResourceSet));
  
  if ( newSet ) memset(newSet, 0, sizeof(*newSet));
  return newSet;
}

//

typedef bool (*GECOResourceSetPerNodeIterator)(GECOResourcePerNode *node, const void *context);

bool
__GECOResourceSetIterate(
  GECOResourceSet                 *theResourceSet,
  GECOResourceSetPerNodeIterator  iterator,
  const void                      *context
)
{
  GECOResourcePerNode             *node = theResourceSet->perNodeList;
  bool                            rc = true;
  
  while ( node && (rc = iterator(node, context)) ) {
    node = node->link;
  }
  return rc;
}

//

  void
  __GECOResourceSetInitOwnerIds(
    GECOResourceSet     *theResourceSet
  )
  {
    struct passwd       *ownerPasswdRec = NULL;
    
    if ( theResourceSet->ownerUname ) {
      if ( (ownerPasswdRec = getpwnam(theResourceSet->ownerUname)) ) {
        theResourceSet->ownerUid = ownerPasswdRec->pw_uid;
        theResourceSet->isOwnerUidSet = true;
      }
    }
    if ( theResourceSet->ownerGname ) {
      struct group   *ownerGroupRec = getgrnam(theResourceSet->ownerGname);
      
      if ( ownerGroupRec ) {
        theResourceSet->ownerGid = ownerGroupRec->gr_gid;
        theResourceSet->isOwnerGidSet = true;
      } else if ( ownerPasswdRec ) {
        theResourceSet->ownerGid = ownerPasswdRec->pw_gid;
        theResourceSet->isOwnerGidSet = true;
      }
    }
  }

//

FILE*
__GECOResourceOpenQStatPipe(
  long int      jobId,
  long int      taskId
)
{
  FILE      *pipeFPtr = NULL;
  int       cmdBufferLen;
  
  cmdBufferLen = snprintf(NULL, 0, "%s -xml -j %ld", GECOResourceQstatCmd, jobId);
  if ( cmdBufferLen ) {
    char    cmdBuffer[cmdBufferLen + 1];
    
    snprintf(cmdBuffer, cmdBufferLen + 1, "%s -xml -j %ld", GECOResourceQstatCmd, jobId);
    GECO_DEBUG("executing for task %ld popen(\"%s\", \"r\")...", taskId, cmdBuffer);
    pipeFPtr = popen(cmdBuffer, "r");
  }
  return pipeFPtr;
}

//

const char*
__GECOResourceXMLGetChildText(
  xmlDocPtr             theXmlDoc,
  xmlNodePtr            baseNode,
  const xmlChar         *nodeName
)
{
  xmlNodePtr            node = baseNode->children;
  
  while ( node ) {
    if ( node->type == XML_ELEMENT_NODE ) {
      if ( strcmp((const char*)nodeName, (const char*)node->name) == 0 ) {
        // Have libxml2 collapse the child node list into a string, substituting
        // entities where they occur:
        return xmlNodeListGetString(theXmlDoc, node->children, 0);
        
        /*node = node->children;
        while ( node ) {
          if ( node->type == XML_TEXT_NODE ) return (const char*)node->content;
          node = node->next;
        }
        break;
        */
      }
    }
    node = node->next;
  }
  return NULL;
}

//

double
__GECOResourceParseMemory(
  const char      *memoryStr
)
{
  double          value = 0.0;
  const char      *endPtr = NULL;
  
  if ( GECO_strtod(memoryStr, &value, &endPtr) ) {
    switch ( *endPtr ) {
    
      case 'G':
        value *= 1024.0;
      case 'M':
        value *= 1024.0;
      case 'K':
        value *= 1024.0;
        break;
      
      case 'g':
        value *= 1000.0;
      case 'm':
        value *= 1000.0;
      case 'k':
        value *= 1000.0;
        break;
        
    }
  }
  return value;
}

//

bool
__GECOResourceSetPerNodeHVMem(
  GECOResourcePerNode *node,
  const void          *context
)
{
  double              *perSlotHVMem = (double*)context;
  
  node->perNodeData.virtualMemoryLimit = node->perNodeData.slotCount * (*perSlotHVMem);
  return true;
}

void
__GECOResourceWalkQstatRequestedResources(
  GECOResourceSet                 *theResourceSet,
  xmlDocPtr                       theXmlDoc,
  xmlXPathContextPtr              xpathCtx,
  xmlNodePtr                      complexesNode,
  GECOResourceSetCreateFailure    *failureReason
)
{
  xmlNodePtr            node = complexesNode->children;
  
  while ( node ) {
    if ( (node->type == XML_ELEMENT_NODE) && (strcmp("element", (const char*)node->name) == 0) ) {
      const char        *complexName = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_name");
      
      if ( complexName ) {
        if ( strcmp("geco_trace_level", complexName) == 0 ) {
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_stringval");
          int           tl;
          
          if ( rsrcValue ) {
            if ( GECO_strtoi((char*)rsrcValue, &tl, NULL) ) theResourceSet->traceLevel = tl;
            xmlFree((xmlChar*)rsrcValue);
          }
        }
        
        else if ( strcmp("h_vmem", complexName) == 0 ) {
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_stringval");
          double        h_vmem;
          
          if ( rsrcValue ) {
            theResourceSet->perSlotVirtualMemoryLimit = h_vmem = __GECOResourceParseMemory(rsrcValue);
            __GECOResourceSetIterate(theResourceSet, __GECOResourceSetPerNodeHVMem, &h_vmem);
            xmlFree((xmlChar*)rsrcValue);
          }
        }
        
        else if ( strcmp("h_rt", complexName) == 0 ) {
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_doubleval");
          double        h_rt;
          
          if ( rsrcValue ) {
            if ( GECO_strtod((char*)rsrcValue, &h_rt, NULL) ) theResourceSet->runtimeLimit = h_rt;
            xmlFree((xmlChar*)rsrcValue);
          }
        }
        
        else if ( strncmp("standby", complexName, 7) == 0 ) {
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_doubleval");
          double        yesno;
          
          if ( rsrcValue ) {
            if ( GECO_strtod((char*)rsrcValue, &yesno, NULL) && (0 != (int)yesno) ) theResourceSet->isStandby = true;
            xmlFree((xmlChar*)rsrcValue);
          }
        }
        
        else if ( strcmp("phi_config_for_user", complexName) == 0 ) {
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"CE_doubleval");
          double        yesno;
          
          if ( rsrcValue ) {
            if ( GECO_strtod((char*)rsrcValue, &yesno, NULL) && (0 != (int)yesno) ) theResourceSet->shouldConfigPhiForUser = true;
            xmlFree((xmlChar*)rsrcValue);
          }
        }
        xmlFree((xmlChar*)complexName);
      }
    }
    node = node->next;
  }
  
  // We have no reason to actually fail, since these items are all optional:
  *failureReason = GECOResourceSetCreateFailureNone;
}

//

void
__GECOResourceWalkQstatGrantedResources(
  GECOResourceSet                 *theResourceSet,
  xmlDocPtr                       theXmlDoc,
  xmlXPathContextPtr              xpathCtx,
  xmlNodePtr                      elementNode,
  GECOResourceSetCreateFailure    *failureReason
)
{
  xmlNodePtr            prevStartNode = xpathCtx->node;
  xmlXPathObjectPtr     matchedNodes;
  
  xpathCtx->node = elementNode;
  
  matchedNodes = xmlXPathEvalExpression((const xmlChar*)".//grl[GRU_name=\"intel_phi\" or GRU_name=\"nvidia_gpu\" or GRU_name=\"m_mem_free\"]", xpathCtx);
  if ( matchedNodes ) {
    xmlNodeSetPtr       nodes = matchedNodes->nodesetval;
    
    if ( nodes ) {
      xmlNodePtr        node;
      int               i = 0, iMax = nodes->nodeNr;
      
      while ( i < iMax ) {
        node = nodes->nodeTab[i++];
        
        if ( node->type == XML_ELEMENT_NODE ) {
          const char    *hostName = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"GRU_host");
          const char    *rsrcName = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"GRU_name");
          const char    *rsrcValue = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"GRU_value");
          
          if ( hostName && rsrcName && rsrcValue ) {
            GECOResourcePerNode   *theNode = GECOResourceSetGetPerNodeWithNodeName(theResourceSet, hostName);
            
            if ( ! theNode ) {
              //
              // Allocate a fresh node:
              //
              if ( (theNode = __GECOResourcePerNodeAlloc()) ) {
                strncpy((char*)theNode->nodeName, hostName, GECORESOURCE_NODENAME_MAX);
                theNode->link = theResourceSet->perNodeList;
                theResourceSet->perNodeList = theNode;
                theResourceSet->nodeCount++;
              }
            }
            if ( theNode ) {
              if ( strcmp("intel_phi", rsrcName) == 0 ) {
                strncpy((char*)theNode->perNodeData.phiList, rsrcValue, GECORESOURCE_PHILIST_MAX);
              }
              
              else if ( strcmp("nvidia_gpu", rsrcName) == 0 ) {
                strncpy((char*)theNode->perNodeData.gpuList, rsrcValue, GECORESOURCE_GPULIST_MAX);
              }
              
              else if ( strcmp("m_mem_free", rsrcName) == 0 ) {
                theNode->perNodeData.memoryLimit = __GECOResourceParseMemory(rsrcValue);
              }
            }
          }
          if ( hostName ) xmlFree((xmlChar*)hostName);
          if ( rsrcName ) xmlFree((xmlChar*)rsrcName);
          if ( rsrcValue ) xmlFree((xmlChar*)rsrcValue);
        }
      }
    }
    xmlXPathFreeObject(matchedNodes);
  }
  
  matchedNodes = xmlXPathEvalExpression((const xmlChar*)".//JAT_granted_destin_identifier_list/element", xpathCtx);
  if ( matchedNodes ) {
    xmlNodeSetPtr       nodes = matchedNodes->nodesetval;
    
    if ( nodes ) {
      xmlNodePtr        node;
      int               i = 0, iMax = nodes->nodeNr;
      
      while ( i < iMax ) {
        node = nodes->nodeTab[i++];
        
        if ( node->type == XML_ELEMENT_NODE ) {
          const char    *hostName = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"JG_qhostname");
          const char    *slotCountStr = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"JG_slots");
          
          if ( hostName && slotCountStr ) {
            GECOResourcePerNode   *theNode = GECOResourceSetGetPerNodeWithNodeName(theResourceSet, hostName);
            long                  slotCount;
            
            if ( ! theNode ) {
              //
              // Allocate a fresh node:
              //
              if ( (theNode = __GECOResourcePerNodeAlloc()) ) {
                strncpy((char*)theNode->nodeName, hostName, GECORESOURCE_NODENAME_MAX);
                theNode->link = theResourceSet->perNodeList;
                theResourceSet->perNodeList = theNode;
                theResourceSet->nodeCount++;
              }
            }
            if ( theNode && GECO_strtol((char*)slotCountStr, &slotCount, NULL)) {
              const char    *slaveStr = __GECOResourceXMLGetChildText(theXmlDoc, node, (const xmlChar*)"JG_tag_slave_job");
              
              theNode->perNodeData.slotCount += slotCount;
              if ( slaveStr ) {
                if ( GECO_strtol(slaveStr, &slotCount, NULL) && (slotCount > 0) ) theNode->isSlave = true;
                xmlFree((xmlChar*)slaveStr);
              }
            }
          }
          if ( hostName ) xmlFree((xmlChar*)hostName);
          if ( slotCountStr ) xmlFree((xmlChar*)slotCountStr);
        }
      }
    }
    xmlXPathFreeObject(matchedNodes);
    
    *failureReason = GECOResourceSetCreateFailureNone;
  }
  xpathCtx->node = prevStartNode;
}

//

bool
__GECOResourceGetQstatMiscellany(
  GECOResourceSet                 *theResourceSet,
  xmlDocPtr                       theXmlDoc,
  xmlXPathContextPtr              xpathCtx,
  GECOResourceSetCreateFailure    *failureReason
)
{
  xmlNodePtr            prevStartNode = xpathCtx->node;
  xmlXPathObjectPtr     matchedNodes;
  
  matchedNodes = xmlXPathEvalExpression((const xmlChar*)"//element/*[self::JB_owner or self::JB_group or self::JB_cwd or self::JB_job_name or self::JB_is_array]", xpathCtx);
  if ( matchedNodes ) {
    xmlNodeSetPtr       nodes = matchedNodes->nodesetval;
    
    if ( nodes ) {
      xmlNodePtr        node;
      int               i = 0, iMax = nodes->nodeNr;
      
      while ( i < iMax ) {
        node = nodes->nodeTab[i++];
        
        if ( node->type == XML_ELEMENT_NODE ) {
          if ( strcmp(node->name, "JB_owner") == 0 ) {
            if ( node->children ) {
              const char    *ownerUname = xmlNodeListGetString(theXmlDoc, node->children, 0);
              
              if ( ownerUname ) {
                theResourceSet->ownerUname = strdup(ownerUname);
                xmlFree((xmlChar*)ownerUname);
              }
            }
          }
          else if ( strcmp(node->name, "JB_group") == 0 ) {
            if ( node->children ) {
              const char    *ownerGname = xmlNodeListGetString(theXmlDoc, node->children, 0);
              
              if ( ownerGname ) {
                theResourceSet->ownerGname = strdup(ownerGname);
                xmlFree((xmlChar*)ownerGname);
              }
            }
          }
          else if ( strcmp(node->name, "JB_cwd") == 0 ) {
            if ( node->children ) {
              const char    *workingDirectory = xmlNodeListGetString(theXmlDoc, node->children, 0);
              
              if ( workingDirectory ) {
                theResourceSet->workingDirectory = strdup(workingDirectory);
                xmlFree((xmlChar*)workingDirectory);
              }
            }
          }
          else if ( strcmp(node->name, "JB_job_name") == 0 ) {
            if ( node->children ) {
              const char    *jobName = xmlNodeListGetString(theXmlDoc, node->children, 0);
              
              if ( jobName ) {
                theResourceSet->jobName = strdup(jobName);
                xmlFree((xmlChar*)jobName);
              }
            }
          }
          else if ( strcmp(node->name, "JB_is_array") == 0 ) {
            if ( node->children ) {
              const char    *isArrayJob = xmlNodeListGetString(theXmlDoc, node->children, 0);
              
              if ( isArrayJob ) {
                if ( ! strcmp(isArrayJob, "1") || ! strcasecmp(isArrayJob, "true") ) {
                  theResourceSet->isArrayJob = true;
                } else {
                  theResourceSet->isArrayJob = false;
                }
                xmlFree((xmlChar*)isArrayJob);
              }
            }
          }
        }
      }
    }
    if ( theResourceSet->ownerUname && theResourceSet->ownerGname ) {
      __GECOResourceSetInitOwnerIds(theResourceSet);
      *failureReason = (theResourceSet->isOwnerUidSet && theResourceSet->isOwnerGidSet) ? GECOResourceSetCreateFailureNone : GECOResourceSetCreateFailureInvalidJobOwner;
    }
    xmlXPathFreeObject(matchedNodes);
  }
}

//
#if 0
#pragma mark -
#endif
//

bool
GECOResourceSetIsJobRunningOnHost(
  long int                      jobId,
  long int                      taskId,
  int                           retryCount
)
{
  char                          path[PATH_MAX];
  size_t                        pathLen;
  bool                          rc = false;
  
  //
  // Try checking the UGE cell for an active job on this host:
  //
  pathLen = snprintf(path, sizeof(path), "%s/%s/active_jobs/%ld.%ld", GECOResourceGECellPrefix, GECOGetHostname(), jobId, taskId);
  if ( pathLen && (pathLen < sizeof(path)) ) {
    rc = GECOIsDirectory(path);
    if ( rc ) GECO_INFO("%ld.%ld is an active job on this host (%s exists)\n", jobId, taskId, path);
  }
  if ( ! rc ) {
    GECOResourceSetCreateFailure  failureReason = 0;
    GECOResourceSetRef            rsrcInfo = GECOResourceSetCreate(jobId, taskId, retryCount, &failureReason);
    bool                          rc = false;
    
    if ( rsrcInfo ) {
      GECOResourcePerNodeRef      thisHost = GECOResourceSetGetPerNodeForHost(rsrcInfo);
      
      if ( thisHost ) {
        rc = true;
        GECO_INFO("%ld.%ld is an active job on this host (per-host resource info exists)\n", jobId, taskId);
      }
      GECOResourceSetDestroy(rsrcInfo);
    }
  }
  return rc;
}

//

GECOResourceSetRef
GECOResourceSetCreate(
  long int                      jobId,
  long int                      taskId,
  int                           retryCount,
  GECOResourceSetCreateFailure  *failureReason
)
{
  GECOResourceSetCreateFailure  localFailureReason;
  GECOResourceSet               *newSet = NULL;
  FILE                          *qstatPipe = NULL;
  uint64_t                      iteration = 1;
  
retry:
  localFailureReason = GECOResourceSetCreateFailureNone;
  qstatPipe = __GECOResourceOpenQStatPipe(jobId, taskId);
  if ( qstatPipe ) {
    newSet = GECOResourceSetCreateWithFileDescriptor(fileno(qstatPipe), jobId, taskId, &localFailureReason);
    pclose(qstatPipe);
  } else {
    localFailureReason = GECOResourceSetCreateFailureQstatFailure;
  }
  if ( ! newSet ) {
    switch ( localFailureReason ) {
    
      case GECOResourceSetCreateFailureQstatFailure:
      case GECOResourceSetCreateFailureMalformedQstatXML: {
        if ( retryCount-- ) {
          GECO_WARN("GECOResourceSetCreate: qstat failed to return job information for %ld.%ld (reason = %d); sleeping then retrying", jobId, taskId, localFailureReason);
          GECOSleepForMicroseconds(iteration++ * 1000000);
          goto retry;
        }
        break;
      }
      
      case GECOResourceSetCreateFailureNoStaticProperties:
      case GECOResourceSetCreateFailureInvalidJobOwner:
      case GECOResourceSetCreateFailureNoRequestedResources:
      case GECOResourceSetCreateFailureNoGrantedResources: {
        if ( retryCount-- ) {
          GECO_WARN("GECOResourceSetCreate: qstat returned inadequate job information for %ld.%ld (reason = %d); sleeping then retrying", jobId, taskId, localFailureReason);
          GECOSleepForMicroseconds(iteration++ * 1000000);
          goto retry;
        }
        break;
      }
      
    }
    if ( failureReason ) *failureReason = localFailureReason;
  }
  return newSet;
}

//

GECOResourceSetRef
GECOResourceSetCreateWithXMLAtPath(
  const char                    *xmlFile,
  long int                      jobId,
  long int                      taskId,
  GECOResourceSetCreateFailure  *failureReason
)
{
  GECOResourceSetRef            newSet = NULL;
  int                           fd = open(xmlFile, O_RDONLY);
  
  if ( fd >= 0 ) {
    newSet = GECOResourceSetCreateWithFileDescriptor(fd, jobId, taskId, failureReason);
    close(fd);
  } else if ( failureReason ) {
    *failureReason = GECOResourceSetCreateFailureCheckErrno;
  }
  return newSet;
}

//

GECOResourceSetRef
GECOResourceSetCreateWithFileDescriptor(
  int                           fd,
  long int                      jobId,
  long int                      taskId,
  GECOResourceSetCreateFailure  *failureReason
)
{
  GECOResourceSetCreateFailure  localFailureReason = GECOResourceSetCreateFailureNone;
  GECOResourceSet               *newSet = NULL;
  
  if ( fd >= 0 ) {
    xmlDocPtr     jobDoc = xmlReadFd(fd, NULL, NULL, XML_PARSE_NOENT | XML_PARSE_NONET);
    
    if ( jobDoc ) {
      //
      // Be sure it's not an unknown job:
      //
      xmlNodePtr                docRoot = xmlDocGetRootElement(jobDoc);
      
      if ( docRoot ) {
        if ( strcmp("unknown_jobs", (const char*)docRoot->name) ) {
          //
          // Locate all the task-specific data for the given task.
          //
          xmlXPathContextPtr      xpathCtx;
          xmlXPathObjectPtr       xpathObj;
          //
          // The character buffer is sized according to the xpath expression used:
          //
          const xmlChar           *taskXPath[64];
          
          snprintf((char*)taskXPath, sizeof(taskXPath), "//JB_ja_tasks/element[JAT_task_number=%ld]", taskId);
          xpathCtx = xmlXPathNewContext(jobDoc);
          if( xpathCtx ) {
            localFailureReason = GECOResourceSetCreateFailureNoGrantedResources;
            //
            // Evaluate the xpath expression against the document:
            //
            xpathObj = xmlXPathEvalExpression((const xmlChar*)taskXPath, xpathCtx);
            if ( xpathObj ) {
              xmlNodeSetPtr   nodes = xpathObj->nodesetval;
              
              if ( nodes ) {
                xmlNodePtr    node;
                int           i = 0, iMax = nodes->nodeNr;
                
                //
                // The xpath matched some nodes, walk through the table and
                // act on the first <element> node found (there should be just
                // one of them):
                //
                while ( i < iMax ) {
                  xmlNodePtr  node = nodes->nodeTab[i++];
                  
                  if ( (node->type == XML_ELEMENT_NODE) && (strcmp("element", (const char*)node->name) == 0) ) {
                    //
                    // Allocate the new object now:
                    //
                    if ( (newSet = __GECOResourceSetAlloc()) ) {
                      newSet->jobId = jobId;
                      newSet->taskId = taskId;
                      //
                      // Evaluate the <element> node and then exit this loop:
                      //
                      __GECOResourceWalkQstatGrantedResources(newSet, jobDoc, xpathCtx, node, &localFailureReason);
                    }
                    break;
                  }
                }
              }
              xmlXPathFreeObject(xpathObj);
              if ( (localFailureReason == GECOResourceSetCreateFailureNone) && newSet ) {
                localFailureReason = GECOResourceSetCreateFailureNoRequestedResources;
                //
                // Find the h_vmem complex:
                //
                xpathObj = xmlXPathEvalExpression((const xmlChar*)"//element/JB_hard_resource_list", xpathCtx);
                if ( xpathObj ) {
                  xmlNodeSetPtr   nodes = xpathObj->nodesetval;
                  
                  if ( nodes ) {
                    xmlNodePtr    node;
                    int           i = 0, iMax = nodes->nodeNr;
                    
                    //
                    // The xpath matched some nodes, walk through the table and
                    // act on the first <element> node found (there should be just
                    // one of them):
                    //
                    while ( i < iMax ) {
                      xmlNodePtr  node = nodes->nodeTab[i++];
                      
                      if ( (node->type == XML_ELEMENT_NODE) && (strcmp("JB_hard_resource_list", (const char*)node->name) == 0) ) {
                        //
                        // Evaluate the <element> node and then exit this loop:
                        //
                        __GECOResourceWalkQstatRequestedResources(newSet, jobDoc, xpathCtx, node, &localFailureReason);
                        break;
                      }
                    }
                  }
                  xmlXPathFreeObject(xpathObj);
                }
              }
              if ( (localFailureReason == GECOResourceSetCreateFailureNone) && newSet ) {
                localFailureReason = GECOResourceSetCreateFailureNoStaticProperties;
                __GECOResourceGetQstatMiscellany(newSet, jobDoc, xpathCtx, &localFailureReason);
              }
            }
            xmlXPathFreeContext(xpathCtx);
          }
        } else if ( failureReason ) {
          localFailureReason = GECOResourceSetCreateFailureJobDoesNotExist;
        }
      } else if ( failureReason ) {
        localFailureReason = GECOResourceSetCreateFailureMalformedQstatXML;
      }
    } else {
      localFailureReason = GECOResourceSetCreateFailureMalformedQstatXML;
    }
  }
  if ( failureReason ) *failureReason = localFailureReason;
  if ( (localFailureReason != GECOResourceSetCreateFailureNone) && newSet ) GECOResourceSetDestroy(newSet);
  return newSet;
}

//

void
GECOResourceSetDestroy(
  GECOResourceSetRef  theResourceSet
)
{
  GECOResourcePerNode   *node = theResourceSet->perNodeList;
  
  if ( theResourceSet->jobName ) free((void*)theResourceSet->jobName);
  if ( theResourceSet->ownerUname ) free((void*)theResourceSet->ownerUname);
  if ( theResourceSet->ownerGname ) free((void*)theResourceSet->ownerGname);
  if ( theResourceSet->workingDirectory ) free((void*)theResourceSet->workingDirectory);
  
  while ( node ) {
    GECOResourcePerNode *next = node->link;
    __GECOResourcePerNodeDealloc(node);
    node = next;
  }
  free(theResourceSet);
}

//

const char*
GECOResourceSetGetJobName(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->jobName;
}

//

const char*
GECOResourceSetGetOwnerUserName(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->ownerUname;
}

//

uid_t
GECOResourceSetGetOwnerUserId(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->ownerUid;
}

//

const char*
GECOResourceSetGetOwnerGroupName(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->ownerGname;
}

//

uid_t
GECOResourceSetGetOwnerGroupId(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->ownerGid;
}

//

bool
GECOResourceSetExecuteAsOwner(
  GECOResourceSetRef  theResourceSet
)
{
  if ( theResourceSet->isOwnerUidSet && theResourceSet->isOwnerGidSet && theResourceSet->workingDirectory ) {
    if (  setgid(theResourceSet->ownerGid) == 0 ) {
      // Now running as the job's owner with the correct gid
      if ( setuid(theResourceSet->ownerUid) == 0 ) {
        // Now running as the job's owner
        if ( chdir(theResourceSet->workingDirectory) == 0 ) return true;
      }
    }
  }
  return false;
}

//

const char*
GECOResourceSetGetWorkingDirectory(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->workingDirectory;
}

//

GECOLogLevel
GECOResourceSetGetTraceLevel(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->traceLevel;
}

//

void
GECOResourceSetSetTraceLevel(
  GECOResourceSetRef  theResourceSet,
  GECOLogLevel        traceLevel
)
{
  theResourceSet->traceLevel = traceLevel;
}

//

double
GECOResourceSetGetPerSlotVirtualMemoryLimit(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->perSlotVirtualMemoryLimit;
}

//

double
GECOResourceSetGetRuntimeLimit(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->runtimeLimit;
}

//

bool
GECOResourceSetGetIsArrayJob(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->isArrayJob;
}

//

bool
GECOResourceSetGetIsStandby(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->isStandby;
}

//

bool
GECOResourceSetGetShouldConfigPhiForUser(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->shouldConfigPhiForUser;
}

//

unsigned int
GECOResourceSetGetNodeCount(
  GECOResourceSetRef  theResourceSet
)
{
  return theResourceSet->nodeCount;
}

//

GECOResourcePerNodeRef
GECOResourceSetGetPerNodeAtIndex(
  GECOResourceSetRef  theResourceSet,
  unsigned int        index
)
{
  GECOResourcePerNode *node = NULL;
  
  if ( index < theResourceSet->nodeCount ) {
    node = theResourceSet->perNodeList;
    
    while ( index-- ) node = node->link;
  }
  return node;
}

//

GECOResourcePerNodeRef
GECOResourceSetGetPerNodeWithNodeName(
  GECOResourceSetRef  theResourceSet,
  const char          *nodeName
)
{
  GECOResourcePerNode *node = theResourceSet->perNodeList;
  
  while ( node && strcmp(node->nodeName, nodeName) ) node = node->link;
  return node;
}

//

GECOResourcePerNodeRef
GECOResourceSetGetPerNodeForHost(
  GECOResourceSetRef  theResourceSet
)
{
  const char      *thisHostname = GECOGetHostname();
  
  if ( thisHostname ) return GECOResourceSetGetPerNodeWithNodeName(theResourceSet, thisHostname);
  return NULL;
}

//

void
GECOResourceSetExport(
  GECOResourceSetRef          theResourceSet,
  GECOResourceSetExportMode   exportMode
)
{
  GECOResourceSetExportForNodeName(theResourceSet, exportMode, NULL);
}

void
GECOResourceSetExportForNodeName(
  GECOResourceSetRef          theResourceSet,
  GECOResourceSetExportMode   exportMode,
  const char                  *nodeName
)
{
  GECOResourcePerNode         *node = theResourceSet->perNodeList;
  const char                  *modeStr = NULL;
  int                         i = 0;
  
  switch ( exportMode ) {
    case GECOResourceSetExportModeUserEnv:
      modeStr = "RESOURCE";
      break;
    case GECOResourceSetExportModeGEProlog:
      modeStr = "PROLOG";
      break;
    case GECOResourceSetExportModeGEEpilog:
      modeStr = "EPILOG";
      break;
    default:
      return;
  }
  
  if ( nodeName ) {
    printf("unset SGE_%1$s_HOSTS SGE_%1$s_NSLOTS SGE_%1$s_MEM SGE_%1$s_VMEM SGE_%1$s_GPU SGE_%1$s_PHI SGE_%1$s_PHI;", modeStr);
  } else {
    printf("SGE_%1$s_HOSTS=(); SGE_%1$s_NSLOTS=(); SGE_%1$s_MEM=(); SGE_%1$s_VMEM=(); SGE_%1$s_GPU=(); SGE_%1$s_PHI=();", modeStr);
  }
  if ( exportMode == GECOResourceSetExportModeGEProlog ) printf("echo '[PROLOG] Resource allocation summary';");
  while ( node ) {
    //
    // Short-circuit if we're seeking a specific host and this isn't it:
    //
    if ( nodeName && (strcasecmp(nodeName, node->nodeName) != 0) ) {
      node = node->link;
      continue;
    }
  
    printf(
        ( nodeName ? " SGE_%1$s_HOSTS='%3$s'; SGE_%1$s_NSLOTS=%4$ld;" : " SGE_%1$s_HOSTS[%2$d]='%3$s'; SGE_%1$s_NSLOTS[%2$d]=%4$ld;" ),
        modeStr,
        i,
        node->nodeName,
        node->perNodeData.slotCount
      );
    if ( exportMode == GECOResourceSetExportModeGEProlog ) printf("echo '[PROLOG]   %ld core%s on \"%s\"';", node->perNodeData.slotCount, (node->perNodeData.slotCount != 1) ? "s" : "", node->nodeName);
    
    if ( (exportMode == GECOResourceSetExportModeGEProlog) && node->perNodeData.memoryLimit > 0.0 ) printf(" echo '[PROLOG]     Memory limit: %.0lf bytes';", node->perNodeData.memoryLimit);
    printf(
        ( nodeName ? " SGE_%1$s_MEM=%3$.0lf;" : " SGE_%1$s_MEM[%2$d]=%3$.0lf;" ),
        modeStr,
        i,
        node->perNodeData.memoryLimit
      );
    
    if ( (exportMode == GECOResourceSetExportModeGEProlog) && node->perNodeData.virtualMemoryLimit > 0.0 ) printf(" echo '[PROLOG]     Virtual memory limit: %.0lf bytes';", node->perNodeData.virtualMemoryLimit);
    printf(
        ( nodeName ? " SGE_%1$s_VMEM=%3$.0lf;" : " SGE_%1$s_VMEM[%2$d]=%3$.0lf;" ),
        modeStr,
        i,
        node->perNodeData.virtualMemoryLimit
      );
    
    if ( (exportMode == GECOResourceSetExportModeGEProlog) && node->perNodeData.gpuList ) printf(" echo '[PROLOG]     nVidia GPU: %s';", node->perNodeData.gpuList);
    printf(
        ( nodeName ? " SGE_%1$s_GPU='%3$s';" : " SGE_%1$s_GPU[%2$d]='%3$s';" ),
        modeStr,
        i,
        (node->perNodeData.gpuList ? node->perNodeData.gpuList : "")
      );
    
    if ( (exportMode == GECOResourceSetExportModeGEProlog) && node->perNodeData.phiList ) printf(" echo '[PROLOG]     Intel Phi: %s';", node->perNodeData.phiList);
    printf(
        ( nodeName ? " SGE_%1$s_PHI='%3$s';" : " SGE_%1$s_PHI[%2$d]='%3$s';" ),
        modeStr,
        i,
        (node->perNodeData.phiList ? node->perNodeData.phiList : "")
      );
    
    //
    // No other nodes matter:
    //
    if ( nodeName ) break;
    
    i++;
    node = node->link;
  }
  printf(
      " SGE_%1$s_JOB_MAXRT=%2$.0lf;"
      " SGE_%1$s_JOB_IS_STANDBY=%3$d;"
      " SGE_%1$s_JOB_VMEM=%4$.0lf;"
      " SGE_%1$s_JOB_TRACELEVEL=%5$d;"
      " SGE_%1$s_JOB_CONFIG_PHI_FOR_USER=%6$d;",
      modeStr,
      theResourceSet->runtimeLimit,
      ( theResourceSet->isStandby ? 1 : 0 ),
      theResourceSet->perSlotVirtualMemoryLimit,
      theResourceSet->traceLevel,
      ( theResourceSet->shouldConfigPhiForUser ? 1 : 0 )
    );
}

//

bool
__GECOResourceSetDeserializeString(
  FILE          *fPtr,
  char          **buffer,
  size_t        bufferLen
)
{
  int           sLen;
  
  if ( fscanf(fPtr, "s%d:", &sLen) == 1 ) {
    if ( *buffer == NULL ) {
      *buffer = malloc(sLen + 1);
      if ( ! *buffer ) return false;
    } else if ( sLen + 1 > bufferLen ) {
      return false;
    }
    if ( sLen > 0 ) {
      sLen = fread(*buffer, 1, sLen, fPtr);
      if ( sLen > 0 ) {
        (*buffer)[sLen] = '\0';
      } else {
        return false;
      }
    } else {
      (*buffer)[0] = '\0';
    }
    return true;
  }
  return false;
}

//

GECOResourceSetRef
GECOResourceSetDeserialize(
  const char            *path
)
{
  GECOResourceSet       *newSet = NULL;
  FILE                  *fPtr = fopen(path, "rb");
  
  if ( fPtr ) {
    if ( (newSet = __GECOResourceSetAlloc()) ) {
      int               count, tmpInt[3], i;
      char              *tmpStr;
      
      count = fscanf(fPtr, "GECOResourceSet_v1{li%ld,li%ld,lf%lf,b%d,lf%lf,i%d,i%d,b%d,b%d",
                  &newSet->jobId, &newSet->taskId,
                  &newSet->runtimeLimit,
                  &tmpInt[0],
                  &newSet->perSlotVirtualMemoryLimit,
                  &newSet->traceLevel,
                  &newSet->nodeCount,
                  &tmpInt[1],
                  &tmpInt[2]
                );
      if ( count != 9 ) {
        GECOResourceSetDestroy(newSet);
        return NULL;
      }
      newSet->isStandby = tmpInt[0] ? true : false;
      newSet->isArrayJob = tmpInt[1] ? true : false;
      newSet->shouldConfigPhiForUser = tmpInt[2] ? true : false;
      
      // Get the job name:
      tmpStr = NULL;
      if ( (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, &tmpStr, 0) && tmpStr ) {
        newSet->jobName = tmpStr;
      } else { 
        GECOResourceSetDestroy(newSet);
        return NULL;
      }
      
      // Get the uname and gname, then fill-in the uid/gid numbers:
      tmpStr = NULL;
      if ( (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, &tmpStr, 0) && tmpStr) {
        newSet->ownerUname = tmpStr;
      } else { 
        GECOResourceSetDestroy(newSet);
        return NULL;
      }
      tmpStr = NULL;
      if ( (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, &tmpStr, 0) && tmpStr) {
        newSet->ownerGname = tmpStr;
      } else { 
        GECOResourceSetDestroy(newSet);
        return NULL;
      }
      __GECOResourceSetInitOwnerIds(newSet);
      
      // Get the working directory:
      tmpStr = NULL;
      if ( (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, &tmpStr, 0) && tmpStr ) {
        newSet->workingDirectory = tmpStr;
      } else { 
        GECOResourceSetDestroy(newSet);
        return NULL;
      }
      
      // Loop over nodes:
      i = 0;
      while ( i < newSet->nodeCount ) {
        GECOResourcePerNode *node = __GECOResourcePerNodeAlloc();
        
        if ( ! node ) {
          GECOResourceSetDestroy(newSet);
          return NULL;
        }
        if ( (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, (char**)&node->nodeName, GECORESOURCE_NODENAME_MAX) ) {
          count = fscanf(fPtr, "{b%d,i%d,lf%lf,lf%lf",
                      &tmpInt[0],
                      &node->perNodeData.slotCount,
                      &node->perNodeData.memoryLimit,
                      &node->perNodeData.virtualMemoryLimit
                    );
          if ( count != 4 ) {
            __GECOResourcePerNodeDealloc(node);
            GECOResourceSetDestroy(newSet);
            return NULL;
          }
          node->isSlave = tmpInt[0] ? true : false;
          
          if ( 
                (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, (char**)&node->perNodeData.gpuList, GECORESOURCE_GPULIST_MAX) && 
                (fscanf(fPtr, ",") >= 0) && __GECOResourceSetDeserializeString(fPtr, (char**)&node->perNodeData.gpuList, GECORESOURCE_GPULIST_MAX) &&
                (fscanf(fPtr, "}") >= 0)
             )
          {
            node->link = newSet->perNodeList;
            newSet->perNodeList = node;
          } else {
            __GECOResourcePerNodeDealloc(node);
            GECOResourceSetDestroy(newSet);
            return NULL;
          }
        } else {
          __GECOResourcePerNodeDealloc(node);
          GECOResourceSetDestroy(newSet);
          return NULL;
        }
        i++;
      }
    }
    fclose(fPtr);
  }
  return newSet;
}

//

bool
GECOResourceSetSerialize(
  GECOResourceSetRef    theResourceSet,
  const char            *path
)
{
  GECOResourcePerNode   *node = theResourceSet->perNodeList;
  FILE                  *fPtr = fopen(path, "wb");
  
  if ( fPtr ) {
    fprintf(fPtr, "GECOResourceSet_v1{li%ld,li%ld,lf%lf,b%d,lf%lf,i%d,i%d,b%d,b%d",
        theResourceSet->jobId, theResourceSet->taskId,
        theResourceSet->runtimeLimit,
        ( theResourceSet->isStandby ? 1 : 0 ),
        theResourceSet->perSlotVirtualMemoryLimit,
        theResourceSet->traceLevel,
        theResourceSet->nodeCount,
        theResourceSet->isArrayJob,
        theResourceSet->shouldConfigPhiForUser
      );
    if ( theResourceSet->jobName ) {
      fprintf(fPtr, ",s%d:%s", strlen(theResourceSet->jobName), theResourceSet->jobName);
    } else {
      fprintf(fPtr, ",s0:");
    }
    if ( theResourceSet->ownerUname ) {
      fprintf(fPtr, ",s%d:%s", strlen(theResourceSet->ownerUname), theResourceSet->ownerUname);
    } else {
      fprintf(fPtr, ",s0:");
    }
    if ( theResourceSet->ownerGname ) {
      fprintf(fPtr, ",s%d:%s", strlen(theResourceSet->ownerGname), theResourceSet->ownerGname);
    } else {
      fprintf(fPtr, ",s0:");
    }
    if ( theResourceSet->workingDirectory ) {
      fprintf(fPtr, ",s%d:%s", strlen(theResourceSet->workingDirectory), theResourceSet->workingDirectory);
    } else {
      fprintf(fPtr, ",s0:");
    }
    while ( node ) {
      fprintf(fPtr, ",s%d:%s{b%d,i%d,lf%lf,lf%lf",
          strlen(node->nodeName), node->nodeName,
          ( node->isSlave ? 1 : 0 ),
          node->perNodeData.slotCount,
          node->perNodeData.memoryLimit,
          node->perNodeData.virtualMemoryLimit
        );
      if ( node->perNodeData.gpuList ) {
        fprintf(fPtr, ",s%d:%s", strlen(node->perNodeData.gpuList), node->perNodeData.gpuList);
      } else {
        fprintf(fPtr, ",s0:");
      }
      if ( node->perNodeData.phiList ) {
        fprintf(fPtr, ",s%d:%s", strlen(node->perNodeData.phiList), node->perNodeData.phiList);
      } else {
        fprintf(fPtr, ",s0:");
      }
      fprintf(fPtr, "}");
      node = node->link;
    }
    fprintf(fPtr, "}");
    fclose(fPtr);
    return true;
  }
  return false;
}

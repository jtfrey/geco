/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECODNetlinkSocket.c
 *  
 *  Polling source that watches for pid termination.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include <sys/socket.h>
#include <linux/connector.h>
#include <linux/netlink.h>
#include <linux/cn_proc.h>

#define GECOD_SEND_MESSAGE_LEN (NLMSG_LENGTH(sizeof(struct cn_msg) + \
				       sizeof(enum proc_cn_mcast_op)))
#define GECOD_RECV_MESSAGE_LEN (NLMSG_LENGTH(sizeof(struct cn_msg) + \
				       sizeof(struct proc_event)))

#define GECOD_SEND_MESSAGE_SIZE    (NLMSG_SPACE(GECOD_SEND_MESSAGE_LEN))
#define GECOD_RECV_MESSAGE_SIZE    (NLMSG_SPACE(GECOD_RECV_MESSAGE_LEN))

#define GECOD_max(x,y) ((y)<(x)?(x):(y))
#define GECOD_min(x,y) ((y)>(x)?(x):(y))

#define GECOD_NLMSG_BUFFER_SIZE (GECOD_max(GECOD_max(GECOD_SEND_MESSAGE_SIZE, GECOD_RECV_MESSAGE_SIZE), 4096))
#define GECOD_MIN_RECV_SIZE (GECOD_min(GECOD_SEND_MESSAGE_SIZE, GECOD_RECV_MESSAGE_SIZE))

#define GECOD_PROC_CN_MCAST_LISTEN (1)
#define GECOD_PROC_CN_MCAST_IGNORE (2)

//

typedef struct {
  int                 fd;
  char                msgBuffer[GECOD_NLMSG_BUFFER_SIZE];
} GECODNetlinkSocket;

//

void
GECODNetlinkSocketDestroySource(
  GECOPollingSource   theSource
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  if ( src->fd >= 0 ) {
    if ( close(src->fd) == 0 ) {
      GECO_DEBUG("GECODNetlinkSocketDestroySource: close(%d) succeeded", src->fd);
    } else {
      GECO_DEBUG("GECODNetlinkSocketDestroySource: close(%d) failed (errno = %d)", src->fd, errno);
    }
    src->fd = -1;
  }
}

//

int
GECODNetlinkSocketFileDescriptorForPolling(
  GECOPollingSource   theSource
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  return src->fd;
}

//

void
GECODNetlinkSocketDidReceiveDataAvailable(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  ssize_t             msgSize;
  struct nlmsghdr     *nl_hdr;
  struct cn_msg       *cn_hdr;
  
  msgSize = read(src->fd, src->msgBuffer, sizeof(src->msgBuffer));
  if ( msgSize > 0 ) {
    bool            isDecoding = true;
    
    //
    // Decode one or more messages:
    //
    nl_hdr = (struct nlmsghdr*)&src->msgBuffer;
    while ( isDecoding && NLMSG_OK(nl_hdr, msgSize) ) {
      switch ( nl_hdr->nlmsg_type ) {
      
        case NLMSG_NOOP:
          break;
          
        case NLMSG_ERROR:
        case NLMSG_OVERRUN:
          isDecoding = false;
          break;
        
        case NLMSG_DONE:
        default: {
          struct proc_event     *event;
          
          cn_hdr = NLMSG_DATA(nl_hdr);
          event = (struct proc_event *)cn_hdr->data;
          switch ( event->what ) {
          
            //
            // A process has exited.
            //
            case PROC_EVENT_EXIT: {
              long int          jobId = -1, taskId = 1;
              pid_t             exitPid = event->event_data.exit.process_pid;
                
              GECO_DEBUG("exit event noted for pid %ld", (long int)exitPid);
              
              // Something we know about?
              if ( GECOPidToJobIdMapGetJobAndTaskIdForPid(GECODPidMappings, exitPid, &jobId, &taskId) ) {
                GECOJobRef      theJob = GECOJobGetExistingObjectForJobIdentifier(jobId, taskId);
                
                GECO_DEBUG("found pid %ld => (%ld,%ld)", (long int)exitPid, jobId, taskId);
                if ( theJob ) {
                  GECO_DEBUG("job %p released", theJob);
                  GECOJobRelease(theJob);
                  GECOPidToJobIdMapRemovePid(GECODPidMappings, exitPid);
                }
              }
              break;
            }
            
          }
          break;
        }
      }
      nl_hdr = NLMSG_NEXT(nl_hdr, msgSize);
    }
  }
}

//

void
GECODNetlinkSocketDidReceiveClose(
  GECOPollingSource   theSource,
  GECORunloopRef      theRunloop
)
{
  GECODNetlinkSocket  *src = (GECODNetlinkSocket*)theSource;
  
  if ( close(src->fd) == 0 ) {
    GECO_DEBUG("GECODNetlinkSocketDidReceiveClose: close(%d) succeeded", src->fd);
  } else {
    GECO_DEBUG("GECODNetlinkSocketDidReceiveClose: close(%d) failed (errno = %d)", src->fd, errno);
  }
  src->fd = -1;
}

//

GECOPollingSourceCallbacks    GECODNetlinkSocketCallbacks = {
                                            .destroySource = GECODNetlinkSocketDestroySource,
                                            .fileDescriptorForPolling = GECODNetlinkSocketFileDescriptorForPolling,
                                            .shouldSourceClose = NULL,
                                            .willRemoveAsSource = NULL,
                                            .didAddAsSource = NULL,
                                            .didBeginPolling = NULL,
                                            .didReceiveDataAvailable = GECODNetlinkSocketDidReceiveDataAvailable,
                                            .didEndPolling = NULL,
                                            .didReceiveClose = GECODNetlinkSocketDidReceiveClose,
                                            .didRemoveAsSource = NULL
                                          };

//

int
GECODNetlinkSocketInit(
  GECODNetlinkSocket    *nlSocket
)
{
	struct sockaddr_nl    localNLAddr;
  int                   localSocket;
  int                   rc;
  ssize_t               count;
	struct nlmsghdr       *nl_hdr;
	struct cn_msg         *cn_hdr;
	enum proc_cn_mcast_op *mcop_msg;
  
  // Start with an invalid fd in the nlSocket:
  nlSocket->fd = -1;
  
  // Allocate a netlink socket:
  localSocket = socket(PF_NETLINK, SOCK_DGRAM, NETLINK_CONNECTOR);
	if (localSocket == -1) {
    GECO_ERROR("GECODNetlinkSocketInit: unable to create netlink socket (errno = %d)", errno);
		return errno;
	}
  
  // Setup this process's netlink address:
	localNLAddr.nl_family   = AF_NETLINK;
	localNLAddr.nl_groups   = CN_IDX_PROC;
	localNLAddr.nl_pid      = getpid();
  
  // Bind the socket to our netlink address:
	rc = bind(localSocket, (struct sockaddr *)&localNLAddr, sizeof(localNLAddr));
	if (rc == -1) {
    GECO_ERROR("GECODNetlinkSocketInit: unable to bind netlink socket to process address (errno = %d)", errno);
		close(localSocket);
    return errno;
	}
  
  // Using the msgBuffer, setup various structural pointers that make
  // up our netlink packet:
  nl_hdr = (struct nlmsghdr *)&nlSocket->msgBuffer;
  cn_hdr = (struct cn_msg *)NLMSG_DATA(nl_hdr);
	mcop_msg = (enum proc_cn_mcast_op*)&cn_hdr->data[0];
  
  // Zero-out all fields of the packet (by zeroing the msgBuffer):
  memset(&nlSocket->msgBuffer, 0, sizeof(nlSocket->msgBuffer));
  // Fill-in netlink header:
	nl_hdr->nlmsg_len = GECOD_SEND_MESSAGE_LEN;
	nl_hdr->nlmsg_type = NLMSG_DONE;
	nl_hdr->nlmsg_flags = 0;
	nl_hdr->nlmsg_seq = 0;
	nl_hdr->nlmsg_pid = getpid();
	// Fill-in the connector header:
	cn_hdr->id.idx = CN_IDX_PROC;
	cn_hdr->id.val = CN_VAL_PROC;
	cn_hdr->seq = 0;
	cn_hdr->ack = 0;
	cn_hdr->len = sizeof(enum proc_cn_mcast_op);
  // Fill-in the payload:
	*mcop_msg = GECOD_PROC_CN_MCAST_LISTEN;
  
  // Send the packet:
  count = send(localSocket, nl_hdr, nl_hdr->nlmsg_len, 0);
  if ( count != nl_hdr->nlmsg_len ) {
    GECO_ERROR("GECODNetlinkSocketInit: unable to register netlink socket attributes (errno = %d)", errno);
    close(localSocket);
    return errno;
  }
    
  // The kernel's netlink address uses pid 1:
  localNLAddr.nl_family  = AF_NETLINK;
  localNLAddr.nl_groups  = CN_IDX_PROC;
  localNLAddr.nl_pid     = 1;
  
  if ( connect(localSocket, (struct sockaddr *)&localNLAddr, sizeof(localNLAddr)) != 0 ) {
    GECO_ERROR("GECODNetlinkSocketInit: unable to connect netlink socket to kernel socket (errno = %d)", errno);
    close(localSocket);
    return errno;
  }
  GECO_INFO("netlink socket %d created and connected to kernel", localSocket);
  nlSocket->fd = localSocket;

  return 0;
}

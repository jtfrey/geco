/*
 *  GECO — Grid Engine Cgroup Orchestrator
 *  GECOQuarantine.c
 *
 *  Quarantine message helpers.
 *
 *  Copyright © 2015
 *  Dr. Jeffrey Frey, University of Delaware
 *
 *  $Id$
 */

#include "GECOQuarantine.h"
#include "GECOLog.h"

#include <openssl/hmac.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>

//

#ifndef MSG_WAITALL
#define MSG_WAITALL 0
#endif

#ifndef MSG_MORE
#define MSG_MORE 0
#endif

//

ssize_t
__GECOQuarantineRecv(
  int           sockfd,
  void          *buf,
  size_t        len,
  int           flags
)
{
  ssize_t       total = 0, rc = -1;
  size_t        fullLen = len;
  bool          keepGoing = true;
  
  while ( keepGoing && (total < fullLen) ) {
    rc = recv(sockfd, buf, len, flags | MSG_WAITALL);
    
    if ( rc < len ) {
      if ( rc > 0 ) {
        buf += rc;
        len -= rc;
        total += rc;
      }
      switch ( errno ) {
      
        case EAGAIN:
          break;
        
        default:
          keepGoing = false;
          break;
      
      }
    } else {
      total += rc;
    }
  }
  return total;
}

//

ssize_t
__GECOQuarantineSend(
  int           sockfd,
  const void    *buf,
  size_t        len,
  int           flags
)
{
  ssize_t       total = 0, rc = -1;
  size_t        fullLen = len;
  bool          keepGoing = true;
  
  while ( keepGoing && (total < fullLen) ) {
    rc = send(sockfd, buf, len, flags);
    
    if ( rc < len ) {
      if ( rc > 0 ) {
        total += rc;
        buf += rc;
        len -= rc;
        total += rc;
      }
      switch ( errno ) {
      
        case EAGAIN:
          break;
        
        default:
          keepGoing = false;
          break;
      
      }
    } else {
      total += rc;
    }
  }
  return total;
}
  

//

typedef union {
  uint8_t       bytes[32];
  uint16_t      halfWords[16];
  uint32_t      words[8];
  uint64_t      longWords[4];
} GECOQuarantineMAC;

//

void
__GECOQuarantineMACToString(
  GECOQuarantineMAC   *theMAC,
  char                *string
)
{
  int       i = 0;
  
  while ( i < 32 ) string += snprintf(string, 3, "%02X", theMAC->bytes[i++]);
}

//

bool
__GECOQuarantineMACGetContext(
  HMAC_CTX      **context
)
{
#include "GECOQuarantineKey.c"

  static HMAC_CTX     digestContext;
  static bool         hasDigestContextBeenInited = false;
  
  if ( ! hasDigestContextBeenInited ) {
    HMAC_CTX_init(&digestContext);
    if ( HMAC_Init_ex(&digestContext, GECOQuarantineHMACKey, sizeof(GECOQuarantineHMACKey), EVP_sha256(), NULL) ) {
      hasDigestContextBeenInited = true;
    } else {
      return false;
    }
  }
  if ( hasDigestContextBeenInited ) {
    *context = &digestContext;
    return true;
  }
  return false;
}

//

bool
__GECOQuarantineMACBegin(void)
{
  HMAC_CTX    *context = NULL;
  
  if ( __GECOQuarantineMACGetContext(&context) ) {
    if ( HMAC_Init_ex(context, NULL, 0, NULL, NULL) ) return true;
  }
  return false;
}

//

bool
__GECOQuarantineMACUpdate(
  const void    *data,
  size_t        dataLen
)
{
  HMAC_CTX    *context = NULL;
  
  if ( __GECOQuarantineMACGetContext(&context) ) {
    if ( HMAC_Update(context, data, dataLen) ) return true;
  }
  return false;
}

//

bool
__GECOQuarantineMACEnd(
  GECOQuarantineMAC     *outMAC
)
{
  HMAC_CTX    *context = NULL;
  
  if ( __GECOQuarantineMACGetContext(&context) ) {
    unsigned int        digestBufferLen = sizeof(GECOQuarantineMAC);
    
    if ( HMAC_Final(context, (unsigned char*)outMAC, &digestBufferLen) ) return true;
  }
  return false;
}  

//
#if 0
#pragma mark -
#endif
//

typedef struct __GECOQuarantineCommand {
  GECOQuarantineCommandId     commandId;
  size_t                      payloadSize;
  const void                  *payloadBytes;
} GECOQuarantineCommand;

//

typedef struct {
  uint64_t    jobId, taskId, jobPid;
} GECOQuarantineCommandJobStarted;

//

typedef struct {
  uint64_t    jobId, taskId;
  uint32_t    success;
} GECOQuarantineCommandAckJobStarted;

//

size_t
__GECOQuarantineCommandStandardPayloadSize(
  GECOQuarantineCommandId     commandId
)
{
  switch ( commandId ) {
  
    case GECOQuarantineCommandIdJobStarted:
      return sizeof(GECOQuarantineCommandJobStarted);
  
    case GECOQuarantineCommandIdAckJobStarted:
      return sizeof(GECOQuarantineCommandAckJobStarted);
  
  }
  return 0;
}

//

GECOQuarantineCommand*
__GECOQuarantineCommandAlloc(
  size_t        payloadSize
)
{
  GECOQuarantineCommand   *newCommand = malloc(sizeof(GECOQuarantineCommand) + payloadSize);
  
  if ( newCommand ) {
    newCommand->commandId = GECOQuarantineCommandIdNoOp;
    newCommand->payloadSize = payloadSize;
    newCommand->payloadBytes = ( payloadSize > 0 ) ? ((void*)newCommand) + sizeof(GECOQuarantineCommand) : NULL;
  }
  return newCommand;
}

//
#if 0
#pragma mark -
#endif
//

enum {
  GECOQuarantineSocketTypeTypeMask  = 0x7,
  GECOQuarantineSocketTypeServer    = 1 << 3
};

const char* GECOQuarantineSocketTypeStrings[] = {
                  "",
                  "path:",
                  "service:",
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  "<unknown>"
                };

//

void
GECOQuarantineSocketInitWithFd(
  int                   socketFd,
  GECOQuarantineSocket  *outSocket
)
{
  outSocket->socketFd = socketFd;
  outSocket->socketType = GECOQuarantineSocketTypeUnknown;
  outSocket->socketAddrInfo = NULL;
}

//

bool
GECOQuarantineSocketOpenServer(
  GECOQuarantineSocketType  socketType,
  const char                *socketAddrInfo,
  unsigned int              retryCount,
  unsigned int              timeoutForRecv,
  unsigned int              timeoutForSend, 
  GECOQuarantineSocket      *outSocket
)
{
  bool                      rc = false;
  int                       sleepLength = 5;
  
  if ( ! socketAddrInfo ) return false;
  
  // Drop any leading whitespace:
  while ( *socketAddrInfo && isspace(*socketAddrInfo) ) socketAddrInfo++;
  
  if ( ! *socketAddrInfo ) return false;
  
  if ( timeoutForRecv < 5 ) timeoutForRecv = 5;
  if ( timeoutForSend < 5 ) timeoutForSend = 5;
  
  socketType = (socketType & GECOQuarantineSocketTypeTypeMask);
  
  if ( socketType == GECOQuarantineSocketTypeInferred ) {
    const char               *originalSocketAddrInfo = socketAddrInfo;
    
    if ( strncmp(socketAddrInfo, "service:", 8) == 0 ) {
      socketType = GECOQuarantineSocketTypeLoopback;
      socketAddrInfo += 8;
    }
    else if ( strncmp(socketAddrInfo, "port:", 5) == 0 ) {
      socketType = GECOQuarantineSocketTypeLoopback;
      socketAddrInfo += 5;
    }
    else if ( strncmp(socketAddrInfo, "path:", 5) == 0 ) {
      socketType = GECOQuarantineSocketTypeFilePath;
      socketAddrInfo += 5;
    }
    // Drop any leading whitespace:
    while ( *socketAddrInfo && isspace(*socketAddrInfo) ) socketAddrInfo++;
    if ( socketType == GECOQuarantineSocketTypeInferred ) {
      switch ( *socketAddrInfo ) {
      
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        default:
          socketType = GECOQuarantineSocketTypeLoopback;
          break;
          
        case '/':
          socketType = GECOQuarantineSocketTypeFilePath;
          break;
        
      }
    }
    GECO_INFO("%s => { type=%d, addrInfo=%s }", originalSocketAddrInfo, socketType, socketAddrInfo);
  }

try_again:
  switch ( socketType ) {
  
    case GECOQuarantineSocketTypeLoopback: {
      struct addrinfo     hints, *address;
      
      memset(&hints, 0, sizeof(hints));
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      if ( getaddrinfo(NULL, socketAddrInfo, &hints, &address) == 0 ) {
        int   sd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        
        if ( sd >= 0 ) {
          if ( bind(sd, address->ai_addr, address->ai_addrlen) == 0 ) {
            GECO_INFO("GECOQuarantineSocketOpenServer: socket %d on service/port %s is initialized", sd, socketAddrInfo);
            outSocket->socketType = socketType | GECOQuarantineSocketTypeServer;
            outSocket->socketAddrInfo = strdup(socketAddrInfo);
            outSocket->socketFd = sd;
            rc = true;
          } else {
            GECO_ERROR("GECOQuarantineSocketOpenServer: failed to bind socket %d to service/port %s (errno = %d)", sd, socketAddrInfo, errno);
            close(sd);
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketOpenServer: unable to allocate socket for service/port %s (errno = %d)", socketAddrInfo, errno);
        }
        freeaddrinfo(address);
      } else {
        GECO_ERROR("GECOQuarantineSocketOpenServer: unable to determine address for service/port = %s (errno = %d)", socketAddrInfo, errno);
      }
      break;
    }
    
    case GECOQuarantineSocketTypeFilePath: {
      int   sd = socket(AF_UNIX, SOCK_STREAM, 0);
      
      if ( sd >= 0 ) {
        struct sockaddr_un    address = {
                                  .sun_family = AF_UNIX,
                                };
        strcpy(address.sun_path, socketAddrInfo);
        if ( GECOIsSocketFile(socketAddrInfo) ) {
          if ( unlink(socketAddrInfo) != 0 ) {
            GECO_ERROR("GECOQuarantineSocketOpenServer: unable to remove existing object at path %s (errno = %d)", socketAddrInfo, errno);
            close(sd);
            return false;
          }
        }
        if ( bind(sd, (struct sockaddr *)&address, strlen(address.sun_path) + sizeof(address.sun_family)) == 0 ) {
          if ( chmod(socketAddrInfo, 0777) == 0 ) {
            GECO_INFO("GECOQuarantineSocketOpenServer: socket %d at path %s is initialized", sd, socketAddrInfo);
            outSocket->socketType = socketType | GECOQuarantineSocketTypeServer;
            outSocket->socketAddrInfo = strdup(socketAddrInfo);
            outSocket->socketFd = sd;
            rc = true;
          } else {
            GECO_ERROR("GECOQuarantineSocketOpenServer: unable to set permissions on socket %d at path %s (errno = %d)", sd, socketAddrInfo, errno);
            close(sd);
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketOpenServer: failed to bind socket %d to path %s (errno = %d)", sd, socketAddrInfo, errno);
          close(sd);
        }
      } else {
        GECO_ERROR("GECOQuarantineSocketOpenServer: unable to allocate socket for %s (errno = %d)", socketAddrInfo, errno);
      }
      break;
    }
  
  }
  if ( rc ) {
    if ( fcntl(outSocket->socketFd, F_SETFL, O_NONBLOCK) == 0 ) {
      struct timeval    timeout = {
                            .tv_sec = timeoutForRecv,
                            .tv_usec = 0
                          };

      if ( setsockopt(outSocket->socketFd, SOL_SOCKET, SO_RCVTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
        timeout.tv_sec = timeoutForSend;
        timeout.tv_usec = 0;
        if ( setsockopt(outSocket->socketFd, SOL_SOCKET, SO_SNDTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
          if ( listen(outSocket->socketFd, 16) == 0 ) {
            GECO_INFO("GECOQuarantineSocketOpenServer: socket %d bound to %s%s is listening",
                outSocket->socketFd,
                GECOQuarantineSocketTypeStrings[(outSocket->socketType & GECOQuarantineSocketTypeTypeMask)],
                outSocket->socketAddrInfo
              );
          } else {
            GECO_ERROR("GECOQuarantineSocketOpenServer: listen on socket %d bound to %s%s failed (errno = %d)",
                outSocket->socketFd,
                GECOQuarantineSocketTypeStrings[(outSocket->socketType & GECOQuarantineSocketTypeTypeMask)],
                outSocket->socketAddrInfo,
                errno
              );
            GECOQuarantineSocketClose(outSocket);
            rc = false;
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketOpenServer: failed to set send timeout on socket %d bound to %s%s (errno = %d)",
              outSocket->socketFd,
              GECOQuarantineSocketTypeStrings[(outSocket->socketType & GECOQuarantineSocketTypeTypeMask)],
              outSocket->socketAddrInfo,
              errno
            );
          GECOQuarantineSocketClose(outSocket);
          rc = false;
        }
      } else {
        GECO_ERROR("GECOQuarantineSocketOpenServer: failed to set receive timeout on socket %d bound to %s%s (errno = %d)",
            outSocket->socketFd,
            GECOQuarantineSocketTypeStrings[(outSocket->socketType & GECOQuarantineSocketTypeTypeMask)],
            outSocket->socketAddrInfo,
            errno
          );
        GECOQuarantineSocketClose(outSocket);
        rc = false;
      }
    } else {
      GECO_ERROR("GECOQuarantineSocketOpenServer: failed to set non-blocking mode on socket %d bound to %s%s (errno = %d)",
            outSocket->socketFd,
            GECOQuarantineSocketTypeStrings[socketType],
            outSocket->socketAddrInfo,
            errno
          );
      GECOQuarantineSocketClose(outSocket);
      rc = false;
    }
  } else if ( retryCount-- > 0 ) {
    GECO_WARN("GECOQuarantineSocketOpenServer: retrying in %d seconds...", sleepLength);
    sleep(sleepLength);
    sleepLength *= 2;
    goto try_again;
  }
  return rc;
}

//

bool
GECOQuarantineSocketOpenClient(
  GECOQuarantineSocketType  socketType,
  const char                *socketAddrInfo,
  unsigned int              retryCount,
  unsigned int              timeoutForRecv,
  unsigned int              timeoutForSend,
  GECOQuarantineSocket      *outSocket
)
{
  bool                      rc = false;
  int                       sleepLength = 5;
  
  if ( ! socketAddrInfo ) return false;
  
  // Drop any leading whitespace:
  while ( *socketAddrInfo && isspace(*socketAddrInfo) ) socketAddrInfo++;
  
  if ( ! *socketAddrInfo ) return false;
  
  if ( timeoutForRecv < 60 ) timeoutForRecv = 60;
  if ( timeoutForSend < 60 ) timeoutForSend = 60;
  
  socketType = (socketType & GECOQuarantineSocketTypeTypeMask);
  
  if ( socketType == GECOQuarantineSocketTypeInferred ) {
    const char               *originalSocketAddrInfo = socketAddrInfo;
    
    if ( strncmp(socketAddrInfo, "service:", 8) == 0 ) {
      socketType = GECOQuarantineSocketTypeLoopback;
      socketAddrInfo += 8;
    }
    else if ( strncmp(socketAddrInfo, "port:", 5) == 0 ) {
      socketType = GECOQuarantineSocketTypeLoopback;
      socketAddrInfo += 5;
    }
    else if ( strncmp(socketAddrInfo, "path:", 5) == 0 ) {
      socketType = GECOQuarantineSocketTypeFilePath;
      socketAddrInfo += 5;
    }
    // Drop any leading whitespace:
    while ( *socketAddrInfo && isspace(*socketAddrInfo) ) socketAddrInfo++;
    if ( socketType == GECOQuarantineSocketTypeInferred ) {
      switch ( *socketAddrInfo ) {
      
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        default:
          socketType = GECOQuarantineSocketTypeLoopback;
          break;
          
        case '/':
          socketType = GECOQuarantineSocketTypeFilePath;
          break;
        
      }
    }
    GECO_INFO("%s => { type=%d, addrInfo=%s }", originalSocketAddrInfo, socketType, socketAddrInfo);
  }

try_again:
  switch ( socketType ) {
  
    case GECOQuarantineSocketTypeLoopback: {
      struct addrinfo     hints, *address;
      
      memset(&hints, 0, sizeof(hints));
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      if ( getaddrinfo(NULL, socketAddrInfo, &hints, &address) == 0 ) {
        int   sd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        
        if ( sd >= 0 ) {
          struct timeval    timeout = {
                                .tv_sec = timeoutForRecv,
                                .tv_usec = 0
                              };

          if ( setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
            timeout.tv_sec = timeoutForSend;
            timeout.tv_usec = 0;
            if ( setsockopt(sd, SOL_SOCKET, SO_SNDTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
loopback_try_again:
              if ( connect(sd, address->ai_addr, address->ai_addrlen) == 0 ) {
                GECO_INFO("GECOQuarantineSocketOpenClient: socket %d is connected to service/port %s", sd, socketAddrInfo);
                outSocket->socketType = socketType;
                outSocket->socketAddrInfo = strdup(socketAddrInfo);
                outSocket->socketFd = sd;
                rc = true;
              } else if ( retryCount-- > 0 ) {
                GECO_WARN("GECOQuarantineSocketOpenClient: retrying connect in %d seconds...", sleepLength);
                sleep(sleepLength);
                sleepLength *= 2;
                goto loopback_try_again;
              } else {
                GECO_ERROR("GECOQuarantineSocketOpenClient: failed to connect to service/port %s (errno = %d)", socketAddrInfo, errno);
              }
            } else {
              GECO_ERROR("GECOQuarantineSocketOpenClient: failed to set send timeout on socket %d for service/port %s (errno = %d)",
                  sd,
                  socketAddrInfo,
                  errno
                );
              close(sd);
              rc = false;
            }
          } else {
            GECO_ERROR("GECOQuarantineSocketOpenClient: failed to set receive timeout on socket %d for service/port %s (errno = %d)",
                sd,
                socketAddrInfo,
                errno
              );
            close(sd);
            rc = false;
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketOpenClient: unable to allocate socket for service/port %s (errno = %d)", socketAddrInfo, errno);
        }
        freeaddrinfo(address);
      } else {
        GECO_ERROR("GECOQuarantineSocketOpenClient: unable to determine address for service/port = %s (errno = %d)", socketAddrInfo, errno);
      }
      break;
    }
    
    case GECOQuarantineSocketTypeFilePath: {
      int   sd = socket(AF_UNIX, SOCK_STREAM, 0);
      
      if ( sd >= 0 ) {
        struct timeval    timeout = {
                              .tv_sec = timeoutForRecv,
                              .tv_usec = 0
                            };

        if ( setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
          timeout.tv_sec = timeoutForSend;
          timeout.tv_usec = 0;
          if ( setsockopt(sd, SOL_SOCKET, SO_SNDTIMEO, (void*)&timeout, sizeof(timeout)) == 0 ) {
            struct sockaddr_un    address = {
                                      .sun_family = AF_UNIX,
                                    };
            strcpy(address.sun_path, socketAddrInfo);
  filepath_try_again:
            if ( GECOIsSocketFile(socketAddrInfo) && (connect(sd, (struct sockaddr *)&address, strlen(address.sun_path) + sizeof(address.sun_family)) == 0) ) {
              GECO_INFO("GECOQuarantineSocketOpenClient: socket %d at path %s is initialized", sd, socketAddrInfo);
              outSocket->socketType = socketType;
              outSocket->socketAddrInfo = strdup(socketAddrInfo);
              outSocket->socketFd = sd;
              rc = true;
            } else if ( retryCount-- > 0 ) {
              GECO_WARN("GECOQuarantineSocketOpenClient: retrying connect in %d seconds...", sleepLength);
              sleep(sleepLength);
              sleepLength *= 2;
              goto filepath_try_again;
            } else {
              GECO_ERROR("GECOQuarantineSocketOpenClient: failed to connect to path %s (errno = %d)", socketAddrInfo, errno);
            }
          } else {
            GECO_ERROR("GECOQuarantineSocketOpenClient: failed to set send timeout on socket %d for %s (errno = %d)",
                sd,
                socketAddrInfo,
                errno
              );
            close(sd);
            rc = false;
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketOpenClient: failed to set recieve timeout on socket %d for %s (errno = %d)",
              sd,
              socketAddrInfo,
              errno
            );
          close(sd);
          rc = false;
        }
      } else {
        GECO_ERROR("GECOQuarantineSocketOpenClient: unable to allocate socket for %s (errno = %d)", socketAddrInfo, errno);
      }
      break;
    }
  
  }
  if ( ! rc && (retryCount-- > 0) ) {
    GECO_WARN("GECOQuarantineSocketOpenClient: retrying in %d seconds...", sleepLength);
    sleep(sleepLength);
    sleepLength *= 2;
    goto try_again;
  }
  return rc;
}

//

bool
GECOQuarantineSocketClose(
  GECOQuarantineSocket  *theSocket
)
{
  bool                  rc = true;
  bool                  isServer = ((theSocket->socketType & GECOQuarantineSocketTypeServer) == GECOQuarantineSocketTypeServer) ? true : false;
  
  if ( theSocket->socketFd >= 0 ) {
    if ( close(theSocket->socketFd) == 0 ) {
      GECO_DEBUG("GECOQuarantineSocketClose: close(%d) succeeded", theSocket->socketFd);
    } else {
      GECO_WARN("GECOQuarantineSocketClose: close(%d) failed (errno = %d)", theSocket->socketFd, errno);
      rc = false;
    }
  }
  switch ( (theSocket->socketType & GECOQuarantineSocketTypeTypeMask) ) {
  
    case GECOQuarantineSocketTypeLoopback: {
      break;
    }
    
    case GECOQuarantineSocketTypeFilePath: {
      if ( GECOIsSocketFile(theSocket->socketAddrInfo) ) {
        if ( unlink(theSocket->socketAddrInfo) != 0 ) {
          GECO_ERROR("GECOQuarantineSocketClose: unable to remove socket file at path %s (errno = %d)", theSocket->socketAddrInfo, errno);
          rc = false;
        } else {
          GECO_DEBUG("GECOQuarantineSocketClose: socket file at path %s removed", theSocket->socketAddrInfo);
        }
      }
      break;
    }
  
  }
  if ( rc ) {
    GECO_DEBUG("GECOQuarantineSocketClose: successfully cleaned-up socket %d bound to %s%s",
          theSocket->socketFd,
          GECOQuarantineSocketTypeStrings[(theSocket->socketType & GECOQuarantineSocketTypeTypeMask)],
          theSocket->socketAddrInfo
        );
  }
  theSocket->socketFd = -1;
  theSocket->socketType = GECOQuarantineSocketTypeInferred;
  if ( theSocket->socketAddrInfo ) {
    free((void*)theSocket->socketAddrInfo);
    theSocket->socketAddrInfo = NULL;
  }
  return rc;
}

//

bool
GECOQuarantineSocketSendCommand(
  GECOQuarantineSocket          *theSocket,
  GECOQuarantineCommandRef      aCommand
)
{
  bool                          rc = false;
  GECOQuarantineMAC             packetMAC;
  uint64_t                      dataLenForced64 = (uint64_t)aCommand->payloadSize;
  
  if ( __GECOQuarantineMACBegin() ) {
    rc =   __GECOQuarantineMACUpdate(&aCommand->commandId, sizeof(aCommand->commandId)) 
        && __GECOQuarantineMACUpdate(&dataLenForced64, sizeof(dataLenForced64))
        && __GECOQuarantineMACUpdate(aCommand->payloadBytes, aCommand->payloadSize)
        && __GECOQuarantineMACEnd(&packetMAC);
    if ( rc ) {
      ssize_t                   sentLen;
      size_t                    expectedLen = sizeof(aCommand->commandId) + sizeof(dataLenForced64) + aCommand->payloadSize + sizeof(packetMAC);
      //
      // Since we set a timeout on the socket for send operations, try to write
      // the entire packet:
      //
      sentLen = __GECOQuarantineSend(theSocket->socketFd, (void*)&aCommand->commandId, sizeof(aCommand->commandId), MSG_MORE);
      sentLen += __GECOQuarantineSend(theSocket->socketFd, (void*)&dataLenForced64, sizeof(dataLenForced64), MSG_MORE);
      sentLen += __GECOQuarantineSend(theSocket->socketFd, aCommand->payloadBytes, aCommand->payloadSize, MSG_MORE);
      sentLen += __GECOQuarantineSend(theSocket->socketFd, (void*)&packetMAC, sizeof(packetMAC), 0);
      if ( sentLen == expectedLen ) {
        char                  macAsString[65];
        
        __GECOQuarantineMACToString(&packetMAC, macAsString);
        GECO_INFO("sent quarantine command = { command=%ld, dataLen=%llu, mac=%s }", aCommand->commandId, (unsigned long long int)aCommand->payloadSize, macAsString);
      } else {
        GECO_ERROR("GECOQuarantineSocketSendCommand: unable to complete command send (errno = %d)", errno);
        rc = false;
      }
    } else {
      GECO_ERROR("GECOQuarantineSocketSendCommand: failed to calculate MAC for command");
    }
  } else {
    GECO_ERROR("GECOQuarantineSocketSendCommand: failed to initialize HMAC context");
  }
  return rc;
}

//

bool
GECOQuarantineSocketRecvCommand(
  GECOQuarantineSocket          *theSocket,
  GECOQuarantineCommandRef      *aCommand
)
{
  bool                          rc = false;
  GECOQuarantineCommandId       recvCommand;
  uint64_t                      dataLenForced64;
  ssize_t                       recvLen;
  size_t                        expectedLen = sizeof(recvCommand) + sizeof(dataLenForced64);
  
  //
  // Since we set a timeout on the socket for recv operations, try to read
  // the entire packet:
  //
  recvLen = __GECOQuarantineRecv(theSocket->socketFd, (void*)&recvCommand, sizeof(recvCommand), MSG_WAITALL);
  recvLen += __GECOQuarantineRecv(theSocket->socketFd, (void*)&dataLenForced64, sizeof(dataLenForced64), MSG_WAITALL);
  if ( recvLen == expectedLen ) {
    if ( dataLenForced64 == __GECOQuarantineCommandStandardPayloadSize(recvCommand) ) {
      GECOQuarantineCommand       *newCommand = __GECOQuarantineCommandAlloc(dataLenForced64);
      
      if ( newCommand ) {
        newCommand->commandId = recvCommand;
        
        expectedLen += dataLenForced64;
        recvLen += __GECOQuarantineRecv(theSocket->socketFd, (void*)newCommand->payloadBytes, newCommand->payloadSize, MSG_WAITALL);
        if ( recvLen == expectedLen ) {
          GECOQuarantineMAC             packetMAC, calculatedMAC;
        
          expectedLen += sizeof(packetMAC);
          recvLen += __GECOQuarantineRecv(theSocket->socketFd, (void*)&packetMAC, sizeof(packetMAC), MSG_WAITALL);
          if ( recvLen == expectedLen ) {
            if ( __GECOQuarantineMACBegin() ) {
              rc =   __GECOQuarantineMACUpdate(&recvCommand, sizeof(recvCommand)) 
                  && __GECOQuarantineMACUpdate(&dataLenForced64, sizeof(dataLenForced64))
                  && __GECOQuarantineMACUpdate(newCommand->payloadBytes, newCommand->payloadSize)
                  && __GECOQuarantineMACEnd(&calculatedMAC);
              if ( rc ) {
                char                  macAsString[65];
                
                __GECOQuarantineMACToString(&packetMAC, macAsString);
                if ( memcmp(&packetMAC, &calculatedMAC, sizeof(packetMAC)) == 0 ) {
                  GECO_INFO("received quarantine command = { command=%ld, dataLen=%lld, mac=%s }", newCommand->commandId, newCommand->payloadSize, macAsString);
                } else {
                  GECO_ERROR("GECOQuarantineSocketRecvCommand: invalid MAC on incoming command = { command=%ld, dataLen=%lld, mac=%s }", newCommand->commandId, newCommand->payloadSize, macAsString);
                  rc = false;
                }
              } else {
                GECO_ERROR("GECOQuarantineSocketRecvCommand: failed to calculate MAC for command");
              }
            } else {
              GECO_ERROR("GECOQuarantineSocketRecvCommand: failed to initialize HMAC context");
            }
          } else {
            GECO_ERROR("GECOQuarantineSocketRecvCommand: partial command MAC recv (%lld of %llu bytes)", recvLen, expectedLen);
          }
        } else {
          GECO_ERROR("GECOQuarantineSocketRecvCommand: partial command payload recv (%lld of %llu bytes)", recvLen, expectedLen);
        }
      } else {
        GECO_ERROR("GECOQuarantineSocketRecvCommand: unable to allocate memory for incoming command %ld (errno = %d)", recvCommand, errno);
      }
      
      if ( rc ) {
        *aCommand = newCommand;
      } else if ( newCommand ) {
        GECOQuarantineCommandDestroy(newCommand);
      }
    } else {
      GECO_ERROR("GECOQuarantineSocketRecvCommand: payload size for command does not match known size (%llu != %llu)",
            (unsigned long long int)dataLenForced64,
            (unsigned long long int)__GECOQuarantineCommandStandardPayloadSize(recvCommand)
          );
    }
  } else {
    GECO_ERROR("GECOQuarantineSocketRecvCommand: partial command header recv (%lld of %llu bytes)", recvLen, expectedLen);
  }
  return rc;
}

//
#if 0
#pragma mark -
#endif
//

void
GECOQuarantineCommandDestroy(
  GECOQuarantineCommandRef  aCommand
)
{
  free((void*)aCommand);
}

//

GECOQuarantineCommandId
GECOQuarantineCommandGetCommandId(
  GECOQuarantineCommandRef  aCommand
)
{
  return aCommand->commandId;
}

//

size_t
GECOQuarantineCommandGetPayloadBytes(
  GECOQuarantineCommandRef  aCommand,
  void                      *buffer,
  size_t                    bufferLen
)
{
  size_t                    copyLen = 0;
  
  if ( aCommand->payloadSize > 0 ) {
    copyLen = (bufferLen < aCommand->payloadSize) ? bufferLen : aCommand->payloadSize;
    memcpy(buffer, aCommand->payloadBytes, copyLen);
  }
  return copyLen;
}

//

size_t
GECOQuarantineCommandGetPayloadSize(
  GECOQuarantineCommandRef  aCommand
)
{
  return aCommand->payloadSize;
}

//
#if 0
#pragma mark -
#endif
//

GECOQuarantineCommandRef
GECOQuarantineCommandJobStartedCreate(
  long int    jobId,
  long int    taskId,
  pid_t       jobPid
)
{
  GECOQuarantineCommand *newCommand = __GECOQuarantineCommandAlloc(sizeof(GECOQuarantineCommandJobStarted));
  
  if ( newCommand ) {
    GECOQuarantineCommandJobStarted   *jobData = (GECOQuarantineCommandJobStarted*)newCommand->payloadBytes;
    
    newCommand->commandId = GECOQuarantineCommandIdJobStarted;
    jobData->jobId = jobId;
    jobData->taskId = taskId;
    jobData->jobPid = jobPid;
  }
  return newCommand;
}

//

long int
GECOQuarantineCommandJobStartedGetJobId(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandJobStarted   *jobData = (GECOQuarantineCommandJobStarted*)aCommand->payloadBytes;
  
  return jobData->jobId;
}

//

long int
GECOQuarantineCommandJobStartedGetTaskId(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandJobStarted   *jobData = (GECOQuarantineCommandJobStarted*)aCommand->payloadBytes;
  
  return jobData->taskId;
}

//

pid_t
GECOQuarantineCommandJobStartedGetJobPid(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandJobStarted   *jobData = (GECOQuarantineCommandJobStarted*)aCommand->payloadBytes;
  
  return jobData->jobPid;
}

//
#if 0
#pragma mark -
#endif
//

GECOQuarantineCommandRef
GECOQuarantineCommandAckJobStartedCreate(
  long int    jobId,
  long int    taskId,
  bool        success
)
{
  GECOQuarantineCommand *newCommand = __GECOQuarantineCommandAlloc(sizeof(GECOQuarantineCommandAckJobStarted));
  
  if ( newCommand ) {
    GECOQuarantineCommandAckJobStarted   *jobData = (GECOQuarantineCommandAckJobStarted*)newCommand->payloadBytes;
    
    newCommand->commandId = GECOQuarantineCommandIdAckJobStarted;
    jobData->jobId = jobId;
    jobData->taskId = taskId;
    jobData->success = success;
  }
  return newCommand;
}

//

long int
GECOQuarantineCommandAckJobStartedGetJobId(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandAckJobStarted   *jobData = (GECOQuarantineCommandAckJobStarted*)aCommand->payloadBytes;
  
  return jobData->jobId;
}

//

long int
GECOQuarantineCommandAckJobStartedGetTaskId(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandAckJobStarted   *jobData = (GECOQuarantineCommandAckJobStarted*)aCommand->payloadBytes;
  
  return jobData->taskId;
}

//

bool
GECOQuarantineCommandAckJobStartedGetSuccess(
  GECOQuarantineCommandRef  aCommand
)
{
  GECOQuarantineCommandAckJobStarted   *jobData = (GECOQuarantineCommandAckJobStarted*)aCommand->payloadBytes;
  
  return ( jobData->success ? true : false );
}

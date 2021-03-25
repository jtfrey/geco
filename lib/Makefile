#
#
#

-include ../Makefile.inc

VERSION				= $(shell cat version.txt)
REVISION			= $(shell cat revision.txt)

CFLAGS				+= -O0 -g
CPPFLAGS			+= -DGECORESOURCE_QSTAT_CMD='"/opt/shared/univa/current/bin/lx-amd64/qstat"' \
                                   -DGECO_GE_CELL_PREFIX='"/opt/shared/univa/cells/farber-8.2/spool"' \
				   -DGECO_PREFIX='"'$(PREFIX)'"' -DGECOCGROUP_PREFIX='"$(GECOCGROUP_PREFIX)"' \
                                   -DGECO_LIB_VERSION='"'$(VERSION)-$(REVISION)'"'
LIBS				+= -lcrypto

#
##
#

TARGET				= libGECO

OBJECTS				= GECO.o \
				  GECOLog.o \
				  GECOIntegerSet.o \
				  GECOResource.o \
				  GECOPidToJobIdMap.o \
				  GECOCGroup.o \
				  GECORunloop.o \
				  GECOJob.o \
				  GECOQuarantine.o

HEADERS				= GECO.h \
				  GECOLog.h \
				  GECOIntegerSet.h \
				  GECOResource.h \
				  GECOPidToJobIdMap.h \
				  GECOCGroup.h \
				  GECORunloop.h \
				  GECOJob.h \
				  GECOQuarantine.h

install_LDFLAGS			:= $(LDFLAGS)
install_LIBS			:= $(LIBS)

default: $(TARGET).a $(TARGET).so

install: install_$(TARGET).a install_$(TARGET).so

-include ../Makefile.rules


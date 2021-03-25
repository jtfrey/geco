#
#
#

-include ../Makefile.inc

VERSION                         = $(shell cat version.txt)
REVISION                        = $(shell cat revision.txt)

CPPFLAGS			+= -I../lib -DGECO_GECOD_VERSION='"'$(VERSION)-$(REVISION)'"'

install_LDFLAGS			:= $(LDFLAGS) -L$(LIBDIR) -Wl,--rpath,$(LIBDIR)
LDFLAGS				+= -L../lib -Wl,--rpath,$(shell cd ../lib ; pwd)

install_LIBS			:= $(LIBS) -lxml2 -lGECO
LIBS				+= -lxml2 -lGECO

BINDIR				= $(SBINDIR)

#
##
#

TARGET				= gecod

OBJECTS				= gecod.o

default: $(TARGET)

install: install_$(TARGET)

-include ../Makefile.rules


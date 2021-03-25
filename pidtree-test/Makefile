#
#
#

-include ../Makefile.inc

CPPFLAGS			+= -I../lib

install_LDFLAGS			:= $(LDFLAGS) -L$(LIBDIR) -Wl,--rpath,$(LIBDIR)
LDFLAGS				+= -L../lib -Wl,--rpath,$(shell cd ../lib ; pwd)

install_LIBS			:= $(LIBS) -lxml2 -lGECO
LIBS				+= -lxml2 -lGECO

#
##
#

TARGET				= pidtree-test

OBJECTS				= pidtree-test.o

default: $(TARGET)

install::

-include ../Makefile.rules


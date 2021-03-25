#
#
#

-include ../Makefile.inc

CPPFLAGS			+= -I../lib -DGECO_LDPRELOAD_VALUE='"libGECOLdPreload.so"'

install_LDFLAGS			:= $(LDFLAGS) -L$(LIBDIR) -Wl,--rpath,$(LIBDIR)
LDFLAGS				+= -L../lib -Wl,--rpath,$(shell cd ../lib ; pwd)

install_LIBS			:= $(LIBS) -lxml2 -lGECO
LIBS				+= -lxml2 -lGECO

BINDIR				= $(SBINDIR)

#
##
#

TARGET				= geco_shepherd

OBJECTS				= geco_shepherd.o

default: $(TARGET)

install: install_$(TARGET)

-include ../Makefile.rules


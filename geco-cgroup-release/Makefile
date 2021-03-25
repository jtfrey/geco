#
#
#

-include ../Makefile.inc

CPPFLAGS			+= -I../lib

install_LDFLAGS			:= $(LDFLAGS) -L$(LIBDIR) -Wl,--rpath,$(LIBDIR)
LDFLAGS				+= -L../lib -Wl,--rpath,$(shell cd ../lib ; pwd)

install_LIBS			:= $(LIBS) -lxml2 -Wl,-Bstatic -lGECO -Wl,-Bdynamic
LIBS				+= -lxml2 -Wl,-Bstatic -lGECO -Wl,-Bdynamic

SUBSYSTEMS			= blkio cpu cpuacct cpuset devices freezer memory net_cls

#
##
#

TARGET				= geco-cgroup-release

OBJECTS				= geco-cgroup-release.o

default: $(TARGET)

install: install_$(TARGET)
	$(foreach subsys, $(SUBSYSTEMS), $(shell ln -f -s $(TARGET) $(BINDIR)/$(subsys)))

-include ../Makefile.rules


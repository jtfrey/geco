#
#
#

SUBPROJ	= lib \
	  geco-rsrcinfo \
	  geco-cgroup-release \
	  integer-set-test \
	  runloop-test \
	  pidtree-test \
	  geco-preload-lib \
	  gecod \
	  geco_prolog \
	  geco_epilog \
	  geco_shepherd \
	  geco_bootmic \
	  geco_resetmic \
	  geco_cgroupcleanup

default::
	@for proj in $(SUBPROJ); do \
	  (cd $$proj; $(MAKE)); \
	done

install:: default $(BIN_DIR) $(LIB_DIR) $(INCLUDE_DIR)
	@for proj in $(SUBPROJ); do \
	  (cd $$proj; $(MAKE) install); \
	done

clean::
	@for proj in $(SUBPROJ); do \
	  (cd $$proj; $(MAKE) clean); \
	done


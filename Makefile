#\
  Copyright 2003-2016 MarkLogic Corporation \
 \
  Licensed under the Apache License, Version 2.0 (the "License"); \
  you may not use this file except in compliance with the License. \
  You may obtain a copy of the License at \
 \
     http://www.apache.org/licenses/LICENSE-2.0 \
 \
  Unless required by applicable law or agreed to in writing, software \
  distributed under the License is distributed on an "AS IS" BASIS, \
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. \
  See the License for the specific language governing permissions and \
  limitations under the License.


TOP = $(CURDIR)
SRC = $(TOP)/src
INCLUDES = $(TOP)/inc
ALLMAKES = $(INCLUDES)/allmakes


include $(INCLUDES)/defs

PKGS=stress
JARFILES=$(SRC)/$(JARS:.jar=)/*.class
SUBDIRS = $(SRC)/test/stress $(SRC)/test/telemetry $(SRC)/test/utilities


#CLASSPATH = -classpath $(INCLUDEJARS)
#CLASSFILES =

all: allmakes $(SUBDIRS)
	@for i in $(SUBDIRS); do (cd $$i; $(MAKE) $(MFLAGS) all); done

allmakes:
	$(RM) $(RMFLAGS) $(ALLMAKES)
	echo "# -*-Makefile-*-"                                 >$(ALLMAKES)
	echo "TOP=$(TOP)"                                     >>$(ALLMAKES)

clean: allmakes
	@for i in $(SUBDIRS); do (cd $$i; $(MAKE) $(MFLAGS) clean); done

distclean:
	@for i in $(SUBDIRS); do (cd $$i; $(MAKE) $(MFLAGS) distclean); done


# Major Makefile variables.
# - JAVAFILES is all of the Java files in the project, including test cases and
#   build tools.

DFSPACKAGES = common
JARFILE = gson-2.8.6.jar
JAVAFILES = */*.java */*/*.java

# Javadoc-related variables.
# - DOCDIR gives the relative path to the directory into which the documentation
#   generated by the docs target will be placed.
# - ALLDOCDIR is the same for the docs-all target.
# - DOCLINK is the URL to Javadoc for the standard Java class library.

DOCDIR = javadoc
ALLDOCDIR = javadoc-all
DOCLINK = https://docs.oracle.com/javase/8/docs/api/
NAMINGDIR = pythondocs

# Define the variable CPSEPARATOR, the classpath separator character. This is
# : on Unix-like systems and ; on Windows. The separator is returned by a
# Java program implemented in build/PathSeparator.java. The Makefile fragment
# included here is made to depend on build/PathSeparator.class to ensure that
# the program is compiled before make procedes past this line.

include build/Makefile.separator

# Compile all Java files.
.PHONY : all-classes
all-classes :
	javac -cp $(JARFILE) $(JAVAFILES)
	pip3 install -r requirements.txt | grep -v 'already satisfied' || true
	# TODO: add command to compile your naming and storage server.
	# end

# Run conformance tests.
.PHONY : test
test : all-classes
	java -cp ".$(CPSEPARATOR)$(JARFILE)" test.ConformanceTests

.PHONY : checkpoint
checkpoint : all-classes
	java -cp ".$(CPSEPARATOR)$(JARFILE)" test.ConformanceCheckpointTests

# Delete all intermediate and final output and leave only the source.
.PHONY : clean
clean :
	rm -rf $(JAVAFILES:.java=.class)  $(DOCDIR) $(ALLDOCDIR)
	rm -rf $(NAMINGDIR)

# Generate documentation for the public interfaces of the principal packages.
.PHONY : docs
docs :
	javadoc -cp  ".$(CPSEPARATOR)$(JARFILE)" -link $(DOCLINK) -d $(DOCDIR) $(DFSPACKAGES)
	pdoc --html -o $(NAMINGDIR) naming/NamingServer.py naming/Structures.py

# Generate documentation for all classes and all members in all packages.
.PHONY : docs-all
docs-all :
	javadoc -cp  ".$(CPSEPARATOR)$(JARFILE)" -link $(DOCLINK) -private -sourcepath \
	     -d $(ALLDOCDIR) $(DFSPACKAGES) test test.common test.naming \
		test.util build
	pdoc --html -o $(NAMINGDIR) naming/NamingServer.py naming/Structures.py


# Dependencies for the Makefile fragment reporting the classpath separator.
build/Makefile.separator : build/PathSeparator.class

build/PathSeparator.class : build/PathSeparator.java
	javac build/PathSeparator.java

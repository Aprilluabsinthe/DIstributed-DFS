MAKEFILE TARGETS

** IMPORTANT:
Make sure you update the Makefile to compile your naming server and storage server correctly!
Before running test, make sure you update the test/Config.java so that the test code could start your servers!


## Project Detail
This project is done by Yuan Gu(guy) and Di Lu(dilu). for 14736-Distributed `System-Project 3-Distributed File System`

the Naming Server is written in Python and the Storage Server is written in Java.

the requirements for python is listed in `requirements.txt`. This has already be configued in `makefile`, You should config it by simply run `make`


## Documentations
see docs of Storage Server in `/javadocs`
see docs of Naming Server in `/pythondocs`


## How to use
To compile all Java and python files, execute
        `make`

To run all checkpoint test cases, run
        `make checkpoint`

To run all test cases, run
        `make test`

To generate documentation, execute
        `make docs`

The documentation can then be viewed at javadoc/index.html. 
Alternatively, complete documentation of all classes and members can be
generated using
        `make docs-all`
and then viewed at javadoc-all/index.html.

To clean the build directories, execute
        `make clean`

# WiredTiger litmus tests
In order to support lock free algorithms in the WiredTiger codebase, we define a number of litmus test. These test are intended to be run by the herd7 simulator.

For any algorithm which has defined litmus tests they can be found under that algorithm's subdirectory.

To run the litmus tests either install and run herd7, instructions [here](https://github.com/herd/herdtools7/blob/master/INSTALL.md). Or run them from the web interface found [here](http://diy.inria.fr/www/#).

If a litmus test is required for X86 there should be one defined for ARM64 as well. The reverse is not neccesarily true as X86 has a stronger memory model than ARM.

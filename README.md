## Overview

ExPyRe (EXecute PYthon REmotely) is a package for evaluating a python function via a queuing system (e.g. Sun Grid Engine, PBS or Slurm), and return the output. The only restriction is that the function needs to be pickleable. 

Evaluation can take place on the same system as the main python process
that is calling the functions, or on a remote system (via passwordless
ssh).

In case of remote execution, the package can transfer auxiliary files that are needed for the execution of the function, and transfer back any newly generated files. 

For examples and more information see [documentation](http://libatoms.github.io/ExPyRe). 

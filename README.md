## Overview

A class for evaluating python functions via a queuing system, aimed at
functions that take all their inputs as arguments (not files) and return
all their outputs in the return values.

Evaluation can take place on the same system as the main python process
that is calling the functions, or on a remote system (via passwordless
ssh).

For examples and more information see documentation. 

Documentation may be checked out from `gh-pages` branch or built with

```
cd docs
make html 
open build/html/index.html
```

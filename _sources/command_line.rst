.. _command_line:

################################################################
Command line utilities
################################################################

There is a command line interface, currently only for simple interaction with the jobs database. Available commands

- ``xpr ls``: list jobs
- ``xpr rm``: delete jobs (and optionally local/remote stage directories)
- ``xpr db_unlock`` (unlock jobs database if it was locked when a process crashed)

Use ``xpr --help`` for more info:

.. click:: expyre.cli.cli:cli
   :prog: expyre
   :nested: full
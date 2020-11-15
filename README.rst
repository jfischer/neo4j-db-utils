===============
Neo4j DB Utils
===============

This file contains some utilities for working with the Neo4j graph database.

neo4j_db_utils
--------------
This is a python package for building importers for the Neo4j bulk import
utility. It works with Python 3.36+ and generates a
set of csv files in the expected format.

To use it, you create a script that imports the definitions from
import_defs.py and provides subclass implementations of ``Node`` and
``MapReduceTemplate`` (and potentially ``Relationship`` if ``SimpleRelationship``
is not sufficient for your graph.

You then define a ``main()`` function that calls ``build_import.add_command_args()``
to add in the arguments which control the output files. You typically add a
positional command line argument for the input file(s) and then parse the
arguments. Now, you call ``run()`` which does the work of generating the
csv files.

Examples
~~~~~~~~
See the ``examples`` subdirectory, which contains an import builder for a simple edge
list graph format. To run it, type::

    python simple_edge_list.py simple_edge_graph.txt

TODO
~~~~
Things to do:

* Need more conceptual explanation.
* Remove dependency on six (was for 2.7 support)

neoctl
------
``neoctl`` is a Python command line script to create, start, stop, and destroy Neo4j databases
running in a Docker container.

Before the first time you use it, you need to install docker and login to docker hub.

The ``neoctl`` script can be invoked as follows::

  neoctl [-h] [--neo4j-root NEO4J_ROOT] [--import-directory IMPORT_DIRECTORY] [--password PASSWORD] COMMAND [COMMAND ...]

  positional arguments:
    COMMAND               Command to run, one of create, start, stop, status, or destroy

  optional arguments:
    -h, --help            show this help message and exit
    --neo4j-root NEO4J_ROOT
                          Root directory for Neo4j files, defaults to $HOME/neo4j
    --import-directory IMPORT_DIRECTORY
                          Directory for import files, defaults to the current directory. Used only when creating database.
    --password PASSWORD   Password for neo4j user. Needed for create and start commands.
  
When you run the ``create`` subcommand, the script expects the bulk loader csv
files to be in the directory specified by --import-directory. Note that create imports the
database but does not leave an instance running. To start it afterward, run the ``start`` command.

License
-------
Copyright 2018-2020 by Jeff Fischer. This is made available
under the 3-clause BSD license.

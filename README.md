Ingres (Vector) Go Library
=============================

Compilation
------------

    # II_SYSTEM should be set
    make iiapi.pc
    go build

Usage
------

Supposed to be used like any other database/sql library.

DSN examples

* dbname
* vnode::dbname
* vnode::dbname/db_class

Vnodes could be set up with `netutil` utility.

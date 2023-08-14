Ingres (Vector) Go Library (OpenAPI)
=====================================

Compilation
------------

    # II_SYSTEM environment variable should be set
    make iiapi.pc
    PKG_CONFIG_PATH=. go build

Usage
------

Supposed to be used like any other database/sql library.

DSN examples

* dbname
* vnode::dbname
* vnode::dbname/db_class
* vnode::dbname?username=actian&password=pass

Vnodes could be set up with `netutil` utility.

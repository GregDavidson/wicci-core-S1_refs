# Directory: Wicci/Core/S1_refs

This project is dependent on [Wicci Core, C_lib, S0_lib](https://github.com/GregDavidson/wicci-core-S0_lib).

## Typed References and OOP in SQL

The files here set up the mechanisms for

* Typed References to Objects
* Operations bound to Types
* Methods bound to Operations and Classes

Objects are Instances of Classes implemented as Rows of
specific Tables, along with possibly some other Rows
accessed through Foreign Keys.

Typed References are implemented as Integers consisting of a
Tag and a Row ID.

Tags are associated with a Type Oid and a Table Oid.

Although all of the requisite information exists in
PostgreSQL tables, the time critical dispatching code is
implemented in C, utilizing PostgreSQL's Server Programming
eXtensions, SPX.

The first argument of an Operation should be a Typed
Reference, which allows dispatching to an appropriate method.
Alas, there are no multi-methods; in fact, only a few
operation signatures are currently available.  It would be
great to have these limitations lifted!


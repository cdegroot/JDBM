$Id: README.txt,v 1.3 2001/08/28 06:46:36 boisvert Exp $

Package readme for: jdbm.*
Package version: see VERSION
Package status: beta
Package URL: http://jdbm.sourceforge.net
Package License: BSD


JDBM Project


WHAT IT IS
==========

- Easy API to persist Java objects (or byte arrays).  
- Basic layers are meant to ressemble the popular GDBM API.
- Transaction support for fail-safe operation.



WHAT IT'S *NOT*
===============

- NOT a multiuser database
- NOT compatible with existing GDBM databases
- NOT an SQL database (no query capabilities)
- NOT a full-blown object-oriented database
  (missing query, locking, schema, garbage collecting, ...)


FEATURES
========

- Simple Hashtable-like interface.

- 100% Java code.

- Fast: Uses no persistent rollback buffer.  An in-memory transaction log is
  kept.  (It can be disabled for even greater performance.)

- Transactional:  Updates are garanteed to be atomic and durable once commited.

- Auto-recovery.  If your application crashes, the next time your application
  opens the database, it will automatically recover any completed transactions.

- Allows implicit or explicit transaction demarcation (commit, rollback)
  through a low-level API called "jdbm.recman.RecordManager".

- (Planned) Support for JTA (Java Transaction API) to allow participation 
  in distributed transactions.


LIMITATIONS
===========

- Size of objects and transaction is limited to available memory size.

- Practical limit on database size is set by your operating system.


CONTRIBUTORS
============

Cees de Groot <cg@cdegroot.com> wrote the original RecordManager 
implementation and started the project.

Alex Boisvert <boisvert@intalio.com> added GDBM-like interface, 
HTree and BTree implementations, caching, rollback functionality 
and some bug fixing.

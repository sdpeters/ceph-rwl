===========================
 Ceph Storage Cluster APIs
===========================

The Ceph Storage Cluster has a messaging layer protocol that enables clients to
interact with Ceph Monitors and Ceph OSD Daemons. ``librados`` provides this
functionality to Ceph Clients in the form of a library.  All Ceph Clients either
use ``librados`` or the same functionality  encapsulated in ``librados`` to
interact with the object store.  For example, ``librbd`` and ``libcephfs``
leverage this functionality. You may use ``librados`` to interact with Ceph
directly (e.g., an application that talks to Ceph, your own interface to Ceph,
etc.). 

For an overview of where ``librados`` appears in the technology stack, 
see :doc:`../../architecture`.

.. toctree:: 

   librados (C) <librados>
   librados (C++) <libradospp>
   librados (Python) <python>
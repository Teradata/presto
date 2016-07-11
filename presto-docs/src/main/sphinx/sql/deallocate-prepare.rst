.. _deallocate-prepare:

==================
DEALLOCATE PREPARE
==================

Synopsis
--------

.. code-block:: none

    DEALLOCATE PREPARE statement_name

Description
-----------

Removes a statement with the name ``statement_name`` from the list of prepared
statements for a session. For more information on preparing a statement see
:ref:`prepare`.

Examples
--------

Deallocate a statement with the name ``my_query``::

    DEALLOCATE PREPARE my_query;


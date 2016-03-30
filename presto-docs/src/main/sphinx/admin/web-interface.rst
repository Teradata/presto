======================
Web Interface
======================

Presto provides a web interface for monitoring and managing queries. This web
interface is accessible through ``example.net:8080``. Replace ``example.net:8080``
to match the host and port of the Presto coordinator as specified in :ref:`config_properties`

The main page has a list of queries along with information like unique query id, query text,
query state, percentage completed, issuing user name and source from which this query
originated. The most recently run query is at the top of the page.

The possible query states are as follows:

* ``QUEUED`` -- Query has been accepted and is awaiting execution.
* ``PLANNING`` -- Query is being planned.
* ``STARTING`` -- Query execution is being started.
* ``RUNNING`` -- Query has at least one running task.
* ``BLOCKED`` -- Query is blocked and is waiting for resources (buffer space, memory, splits, etc.).
* ``FINISHING`` -- Query is finishing (e.g. commit for autocommit queries).
* ``FINISHED`` -- Query has finished executing and all output has been consumed.
* ``FAILED`` -- Query execution failed.

The ``BLOCKED`` state is normal, but if it is persistent, it should be investigated.
It has many potential causes: insufficient memory or splits, disk or network I/O bottlenecks, data skew
(all the data goes to a few workers), a lack of parallelism (only a few workers available), or computationally
expensive stages of the query following a given stage.  Additionally, a query can be in
the ``BLOCKED`` state if a client is not processing the data fast enough (common with "SELECT \*" queries).

For more detailed information about a query, simply click the query ID link.
The query detail page has a summary section, graphical representation of various stages of the
query and a list of tasks. Each task id can be clicked to get more information about that task.
In the summary section there is a button to kill the currently running query in case the user
decides to do so for an unwanted long running query. There are couple of visualizations available
in the summary section, task execution and timeline. There is a JSON version of information and
statistics of the given query which is present in the summary section with the tag ``Raw``. These
visualizations and other statistics can be used to analyze where time is being spent for a query
that is not performing as per the expectations.

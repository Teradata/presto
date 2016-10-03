=============================
Secure Internal Communication
=============================

The Presto cluster can be configured to use secured communication. Communication
between Presto nodes can be secured with SSL/TLS.

Internal SSL/TLS configuration
------------------------------

SSL/TLS is configured in the `config.properties` file.  The SSL/TLS on the
worker and coordinator nodes are configured using the same set of properties.
Every node in the cluster must be configured. Nodes that have not been
configured, or are configured incorrectly, will not be able to communicate with
other nodes in the cluster.

To enable SSL/TLS for Presto internal communication, do the following:

1. Disable HTTP endpoint.

.. code-block:: none

    http-server.http.enabled=false

.. warning::

    You can enable HTTPS while leaving HTTP enabled. In most cases this is a
    security hole. If you are certain you want to use this configuration, you
    should consider using an firewall to limit access to the HTTP endpoint to
    only those hosts that should be allowed to use it.

2. Specify each node's fully-qualified hostname. This will be different for
   every host.  Hosts should be in the same domain to make it easy to create
   the correct SSL/TLS certificates. e.g.: `coordinator.example.com`,
   `worker1.example.com`, `worker2.example.com`.

.. code-block:: none

    node.external-address=<node-domain>

3. Generate a Java Keystore File. Every Presto node must be able to connect to
   any other node within the same cluster. It is possible to create unique
   certificates for every node using the fully-qualified hostname of each host,
   create a keystore that contains all the public keys for all of the hosts,
   and specify it for the client (`http-client.https.keystore.path`). In most
   cases it will be simpler to use a wildcard in the certificate as shown
   below.

.. code-block:: none

    keytool -genkeypair -alias example.com -keyalg RSA -keystore keystore.jks
    Enter keystore password:
    Re-enter new password:
    What is your first and last name?
      [Unknown]:  *.example.com
    What is the name of your organizational unit?
      [Unknown]:
    What is the name of your organization?
      [Unknown]:
    What is the name of your City or Locality?
      [Unknown]:
    What is the name of your State or Province?
      [Unknown]:
    What is the two-letter country code for this unit?
      [Unknown]:
    Is CN=*.example.com, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
      [no]:  yes

    Enter key password for <presto>
            (RETURN if same as keystore password):

.. Note: Replace `example.com` with the appropriate domain.

4. Distribute the Java Keystore File across the Presto cluster.

5. Enable the HTTPS endpoint.

.. code-block:: none

    http-server.https.enabled=true
    http-server.https.port=<https port>
    http-server.https.keystore.path=<keystore path>
    http-server.https.keystore.key=<keystore password>

6. Configure the internal HTTP client to use the Java keystore file.

.. code-block:: none

    http-client.https.keystore.path=<keystore path>
    http-client.https.keystore.key=<keystore password>

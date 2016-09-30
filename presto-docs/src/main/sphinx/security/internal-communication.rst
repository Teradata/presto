=============================
Secure Internal Communication
=============================

The Presto cluster can be configured to use secured communication. Communication
between Presto nodes can be secured with encryption, authentication and authorization.

.. Note: At this step only encryption is supported

Encrypted Internal Communication Configuration
----------------------------------------------

Encryption is configured in `config.properties` file.
Encryption properties are identical for all of the Presto node roles (coordinator, worker)
and must be set on every node in the cluster.

To enable encryption for the Presto internal communication follow the next steps:

1. Disable HTTP endpoint.

.. code-block:: none

    http-server.http.enabled=false

.. warning::

    Leaving HTTP endpoint enabled leads to a security hole.

2. Specify node's hosts. Every node must have it's own host configured.
Hosts must be in the same parent domain to be able to easily generate custom SSL
certificates. e.g.: `coordinator.example.com`, `worker1.example.com`, `worker2.example.com`.

.. code-block:: none

    node.external-address=<node-domain>

3. Generate Java Keystore File. Every Presto node must be able to connect to any other node within
the same cluster. It is possible to create private certificates for every single node for the
exact domain, and than specify it for the server (`http-server.https.keystore.path`), and create a
keystore that contains all the public keys for all the domains, and specify it for the
client (`http-client.https.keystore.path`). But for simplicity single keystore for the base domain
with wildcard can be created.

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

.. Note: Replace `example.com` with your actual top-level domain

4. Distribute generated Java Keystore File across the Presto cluster.

5. Enable HTTPS endpoint.

.. code-block:: none

    http-server.https.enabled=true
    http-server.https.port=<https port>
    http-server.https.keystore.path=<keystore path>
    http-server.https.keystore.key=<keystore password>

6. Provide Java Keystore File location to internal HTTP clients.

.. code-block:: none

    http-client.https.keystore.path=<keystore path>
    http-client.https.keystore.key=<keystore password>

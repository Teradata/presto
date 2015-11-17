===========================
CLI Kerberos Authentication
===========================

Presto provides limited experimental support for Kerberos authentication when the command-line interface is used.

Presto coordinator node configuration
-------------------------------------

To use Kerberos based authentication, the following configuration properties must defined for the Presto coordinator node:

.. code-block:: none

    http.server.authentication.enabled=true
    http.server.authentication.krb5.service-name=presto.coordinator.https
    http.authentication.krb5.config=/etc/krb5.conf
    http-server.https.enabled=true
    http-server.https.port=7778
    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

* ``http.server.authentication.enabled``: Defines that Presto authentication should be enabled. Must be set to ``true``
* ``http.server.authentication.krb5.service-name``: Defines the Kerberos server name for the Presto coordinator
* ``http.authentication.krb5.config``: Kerberos configuration file
* ``http-server.https.enabled``: Specifies if Presto server should listen for HTTPS
* ``http-server.https.port``: HTTPS server port
* ``http-server.https.keystore.path``: Path of keystore file to be used by HTTPS server
* ``http-server.https.keystore.key``: Keystore password

Presto CLI execution
--------------------

To use the Kerberos on client side, the following extra parameters must be passed to presto-cli:

.. code-block:: none

    --enable-authentication
    --krb5-config-path /etc/krb5.conf
    --krb5-remote-service-name presto.coordinator.https
    --keystore-path /etc/java_keystore.jks
    --keystore-password the_keystore_password

* ``--enable-authentication``: Defines that Presto authentication should be enabled.
* ``--krb5-config-path``: Kerberos configuration file
* ``--krb5-remote-service-name``: Presto coordinator Kerberos service name
* ``--keystore-``: Path of keystore file to be used by HTTPS server
* ``--keystore-password``: Keystore password

Access controller implementation
--------------------------------

Additionally the user needs to provide an implementation of access controller.
This is done by implementing the ``SystemAccessControlFactory`` and ``SystemAccessControl`` interfaces.

``SystemAccessControlFactory`` is responsible for creating a ``SystemAccessControl`` instance. It also
defines a ``SystemAccessControl`` name which used by the user in a Presto configuration.

``SystemAccessControl`` is responsible for:
 * verifying if a given Kerberos principal is authorized to execute queries as the specified user
 * checking if a given user can alter values for a given system property

The implementation of ``SystemAccessControl`` and ``SystemAccessControlFactory`` must be wrapped as a plugin and
installed on a Presto cluster.

Using implemented access controller
-----------------------------------

To select and configure the implemented access controller, the ``access-controls.properties`` configuration
file must be added on the Presto coordinator node.
The file must contain the `access-control.name` property which should match the name of the implemented access controller.

Other entries defined in the configuration file are passed as a map to the
``SystemAccessControlFactory.crate(Map<String, String> config)`` method.


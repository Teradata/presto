===================
LDAP Authentication
===================

Presto can be configured to enable frontend LDAP authentication over
HTTPS for clients, such as the :ref:`cli_ldap`, or the JDBC and ODBC
drivers. At present only simple LDAP authentication mechanism involving
user name and password is supported. The Presto client sends a user name 
and password to the coordinator and coordinator validates these
credentials using an external LDAP service.

To enable LDAP authentication for Presto, configuration changes are made on
the Presto coordinator. No changes are required to the worker configuration;
only the communication from the clients to the coordinator is authenticated.

Presto Server Configuration
---------------------------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. _ldap_server:

LDAP Server
~~~~~~~~~~~

You can configure the Presto coordinator to authenticate against one
of the following types of LDAP server implementations:

    * Active Directory
    * OpenLDAP

Presto requires Secure LDAP (LDAPS), so make sure you have TLS
enabled on your LDAP server.

TLS Configuration on Presto Coordinator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need to import the LDAP server's TLS certificate to the default Java
truststore of the Presto coordinator to secure TLS connection. You can use
the following example `keytool` command to import the certificate
``ldap_server.crt``, to the truststore on the coordinator.

.. code-block:: none

    $ keytool -import -keystore <JAVA_HOME>/jre/lib/security/cacerts -trustcacerts -alias ldap_server -file ldap_server.crt

In addition to this, access to the Presto coordinator should be
through HTTPS. You can do it by creating a :ref:`server_java_keystore` on
the coordinator.

Presto Coordinator Node Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must make the following changes to the environment prior to configuring the
Presto coordinator to use LDAP authentication and HTTPS. 

 * :ref:`ldap_server`
 * :ref:`server_java_keystore`

You also need to make following changes to the Presto configuration files.

config.properties
~~~~~~~~~~~~~~~~~

LDAP authentication is configured in the coordinator node's
config.properties file. Properties that need to be added, along
with their sample values, are listed below.

.. code-block:: none

    authentication.ldap.enabled=true
    authentication.ldap.url=ldaps://ldap-server:636

    http-server.https.enabled=true
    http-server.https.port=8443

    http-server.https.keystore.path=/etc/presto_keystore.jks
    http-server.https.keystore.key=keystore_password

In addition to the above properties, you will need to add LDAP server implementation 
specific properties. 

For Active Directory:

.. code-block:: none

    authentication.ldap.server-type=active_directory
    authentication.ldap.ad-domain=corp.domain.com

For OpenLDAP:

.. code-block:: none

    authentication.ldap.server-type=openLDAP
    authentication.ldap.base-dn=OU=Asia,DC=corp,DC=domain,DC=com

======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.enabled``                         Enable LDAP authentication for the Presto coordinator.
                                                        Must be set to ``true``. Default value is ``false``.
``authentication.ldap.url``                             The url to the LDAP server. The url scheme must be
                                                        ``ldaps://`` and Presto allows only Secure LDAP.
``authentication.ldap.server-type``                     The type of LDAP server implementation.
                                                        Set to ``active_directory`` or ``openLDAP``.
``authentication.ldap.ad-domain``                       The domain name of the Active Directory server.
                                                        This property is required only if 
                                                        ``authentication.ldap.server-type`` is set to
                                                        ``Active_Directory``. Eg: ``corp.domain.com``.
``authentication.ldap.base-dn``                         The base LDAP distinguished name for the user who
                                                        who tries to connect to the server. This property is 
                                                        required if ``authentication.ldap.server-type`` is set
                                                        to ``openLDAP`` or when specifying LDAP group
                                                        membership properties.
``http-server.https.enabled``                           Enables HTTPS access for the Presto coordinator.
                                                        Should be set to ``true``. Default value is 
                                                        ``false``.
``http-server.https.port``                              HTTPS server port.
``http-server.https.keystore.path``                     The location of the Java Keystore file that will be
                                                        used to secure TLS.
``http-server.https.keystore.key``                      The password for the keystore. This must match the
                                                        password you specified when creating the keystore.
======================================================= ======================================================

Authorization based on LDAP Group Membership
********************************************

You can further restrict the set of users allowed to connect to the Presto
coordinator based on their group memberships. This optional feature is enabled
if you set ``authentication.ldap.group-dn`` property.
Only users belonging to the group with distinguished name
``authentication.ldap.group-dn`` are authenticated successfully.

In addition to the basic LDAP authentication properties, you need group-membership specific
properties in ``config.properties``. Properties that need to be added, along
with their sample values, are listed below.

.. code-block:: none

    authentication.ldap.base-dn=OU=America,DC=corp,DC=domain,DC=com
    authentication.ldap.group-dn=CN=AuthorizedGroup,OU=America,DC=corp,DC=domain,DC=com

For Active Directory:

.. code-block:: none

    authentication.ldap.user-object-class=person

For OpenLDAP:

.. code-block:: none

    authentication.ldap.user-object-class=inetOrgPerson

.. note::

    Presto does not yet support providing multiple groups for 
    ``authentication.ldap.group-dn``. Nested group authorization, where you
    want to authorize a user who belongs to a group which is a member of
    ``authentication.ldap.group-dn`` is also not supported.
    
    For OpenLDAP, for this feature to work, make sure you enable the
    ``memberOf`` `overlay <http://www.openldap.org/doc/admin24/overlays.html>`_.


======================================================= ======================================================
Property                                                Description
======================================================= ======================================================
``authentication.ldap.base-dn``                         The base LDAP distinguished name for the user who
                                                        who tries to connect to the server.
``authentication.ldap.group-dn``                        The entire LDAP :abbr:`dn (distinguished name)` for
                                                        the group which must be authorized to access Presto.
                                                        The user trying to connect to the server with base
                                                        :abbr:`dn (distinguished name)`
                                                        ``authentication.ldap.base-dn`` must belong to this
                                                        group for successful authentication.
``authentication.ldap.user-object-class``               The LDAP objectClass the user implements in LDAP.
                                                        Eg: ``person``.
======================================================= ======================================================


.. _cli_ldap:

Presto CLI
----------

Environment Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

TLS Configuration
~~~~~~~~~~~~~~~~~

Access to the Presto coordinator should be through HTTPS when using LDAP
authentication. The Presto CLI can use either a :ref:`Java Keystore
<server_java_keystore>` file or :ref:`Java Truststore <cli_java_truststore>`
for its TLS configuration. 

If you are using keystore file, it can be copied to the client machine and used
for its TLS configuration. If you are using truststore, you can either use
default java truststores or create a custom truststore on the CLI. We do not
recommend using self-signed certificates in production.

Presto CLI Execution
^^^^^^^^^^^^^^^^^^^^

In addition to the options that are required when connecting to a Presto
coordinator that does not require LDAP authentication, invoking the CLI
with LDAP support enabled requires a number of additional command line
options. You can either use ``--keystore-*`` or ``--truststore-*`` properties
to secure TLS connection. The simplest way to invoke the CLI is with a
wrapper script.

.. code-block:: none

    #!/bin/bash

    ./presto \
    --server https://presto-coordinator.example.com:8443 \
    --keystore-path /tmp/presto.jks \
    --keystore-password password \
    --truststore-path /tmp/presto_truststore.jks
    --truststore-password password
    --catalog <catalog> \
    --schema <schema> \
    --user <LDAP user>
    --password <password for the user>

=============================== =========================================================================
Option                          Description
=============================== =========================================================================
``--server``                    The address and port of the Presto coordinator.  The port must
                                be set to the port the Presto coordinator is listening for HTTPS
                                connections on. Presto CLI does not support using `http` scheme for
                                the url when using LDAP authentication.
``--keystore-path``             The location of the Java Keystore file that will be used
                                to secure TLS.
``--keystore-password``         The password for the keystore. This must match the
                                password you specified when creating the keystore.
``--truststore-path``           The location of the Java Truststore file that will be used
                                to secure TLS.
``--truststore-password``       The password for the truststore. This must match the
                                password you specified when creating the truststore.
``--user``                      The LDAP username. For Active Directory this should be your
                                ``sAMAccountName`` and for OpenLDAP this should be the ``uid`` of
                                the user.
``--password``                  The password for the ``user``.
=============================== =========================================================================

Troubleshooting
---------------

Java Keystore File Verification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Verify the password for a keystore file and view its contents using
:ref:`troubleshooting_keystore`.

SSL Debugging for Presto CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you encounter any SSL related errors when running Presto CLI, you can run CLI using ``-Djavax.net.debug=ssl``
parameter for debugging. You should use the Presto CLI executable jar to enable this. Eg:

.. code-block:: none

    java -Djavax.net.debug=ssl \
    -jar \
    presto-cli-<version>-executable.jar \
    --server https://coordinator:8443 \
    <other_cli_arguments>

Common SSL errors
~~~~~~~~~~~~~~~~~

java.security.cert.CertificateException: No subject alternative names present
*****************************************************************************

This error is seen when the Presto coordinator’s certificate is invalid and does not have the IP you provide
in the ``--server`` argument of the CLI. You will have to regenerate the coordinator's SSL certificate 
with the appropriate :abbr:`SAN (Subject Alternative Name)` added.

Adding a SAN to this certificate is required in cases where ``https://`` uses IP address in the URL rather
than the domain contained in the coordinator's certificate, and the certificate does not contain the
:abbr:`SAN (Subject Alternative Name)` parameter with the matching IP address as an alternative attribute.

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# noinspection PyUnresolvedReferences
from docutils import nodes, utils
# noinspection PyUnresolvedReferences
from sphinx.errors import SphinxError

TD_LINK_SERVER = 'http://www.teradata.com/presto'
TD_LINK_CLI = 'http://www.teradata.com/presto'
TD_LINK_JDBC = 'http://www.teradata.com/presto'
TD_LINK_DEMO_CDH = 'http://www.teradata.com/presto'
TD_LINK_DEMO_HDP = 'http://www.teradata.com/presto'
TD_LINK_PRESTO_DOCS = 'http://www.teradata.com/presto'
TD_LINK_PRESTO_YARN_PACKAGE = 'http://www.teradata.com/presto'
TD_LINK_PRESTO_AMBARI_PLUGIN = 'http://www.teradata.com/presto'
TD_LINK_SLIDER = 'http://archive.apache.org/dist/incubator/slider/0.80.0-incubating/slider-assembly-0.80.0-incubating-all.tar.gz'
TD_LINK_ODBC_MAC = 'http://www.teradata.com/presto'
TD_LINK_ODBC_WIN64 = 'http://www.teradata.com/presto'
TD_LINK_ODBC_WIN32 = 'http://www.teradata.com/presto'
TD_LINK_ODBC_LINUX64 = 'http://www.teradata.com/presto'
TD_LINK_ODBC_LINUX32 = 'http://www.teradata.com/presto'
TD_LINK_ODBC_DOCS = 'http://www.teradata.com/presto'

GROUP_ID = 'com.facebook.presto'
ARTIFACTS = {
    'server': ('presto-server-rpm-0.141t', 'rpm', TD_LINK_SERVER),
    'server-old': ('presto-server-rpm4.6-0.141t', 'rpm', TD_LINK_SERVER),
    'cli': ('presto-cli-0.141t', 'jar', TD_LINK_CLI),
    'jdbc': ('presto-jdbc-0.141t', 'jar', TD_LINK_JDBC),
    'demo-cdh': ('presto-demo-cdh-0.141t', 'ova', TD_LINK_DEMO_CDH),
    'demo-hdp': ('presto-demo-hdp-0.141t', 'ova', TD_LINK_DEMO_HDP),
    'presto-yarn-package': ('presto-yarn-package-1.0.1-0.141t', 'zip', TD_LINK_PRESTO_YARN_PACKAGE),
    'presto-ambari-plugin': ('ambari-presto-1.0', 'tar.gz', TD_LINK_PRESTO_DOCS),
    'presto-admin': ('prestoadmin-1.2', 'tar.bz2', TD_LINK_PRESTO_DOCS),
    'apache-slider': ('slider-assembly-0.80.0-incubating-all', 'tar.gz', TD_LINK_PRESTO_DOCS),
    'presto-docs': ('presto-docs-0.141t-download', 'zip', TD_LINK_PRESTO_DOCS),

    'odbc-mac': ('TeradataPrestoODBC-1.0.0.1001', 'dmg', TD_LINK_ODBC_MAC),
    'odbc-windows-64': ('TeradataPrestoODBC64-1.0.0.1001', 'msi', TD_LINK_ODBC_WIN64),
    'odbc-windows-32': ('TeradataPrestoODBC32-1.0.0.1001', 'msi', TD_LINK_ODBC_WIN32),
    'odbc-linux-64': ('TeradataPrestoODBC-64bit-1.0.0.1001', 'rpm', TD_LINK_ODBC_LINUX64,),
    'odbc-linux-32': ('TeradataPrestoODBC-32bit-1.0.0.1001', 'rpm', TD_LINK_ODBC_LINUX32),
    'odbc-documentation': ('Teradata ODBC Driver for Presto Install Guide', 'pdf', TD_LINK_ODBC_DOCS)
}

def setup(app):
    # noinspection PyDefaultArgument,PyUnusedLocal
    def download_link_role(role, rawtext, text, lineno, inliner, options={}, content=[]):

        if not text in ARTIFACTS:
            inliner.reporter.error('Unsupported download type: ' + text)
            return [], []

        artifact, packaging, uri = ARTIFACTS[text]

        title = artifact + '.' + packaging
        uri = uri

        node = nodes.reference(title, title, internal=False, refuri=uri)

        return [node], []

    app.add_role('download', download_link_role)

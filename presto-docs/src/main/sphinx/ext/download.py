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

GROUP_ID = 'com.facebook.presto'
ARTIFACTS = {
    'server': ('presto-server', 'rpm', TD_LINK_SERVER),
    'cli': ('presto-cli', 'jar', TD_LINK_CLI),
    'jdbc': ('presto-jdbc', 'jar', TD_LINK_JDBC)
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

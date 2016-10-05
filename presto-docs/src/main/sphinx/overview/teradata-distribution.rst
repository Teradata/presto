===============================
Teradata Distribution of Presto
===============================

Teradata's distribution of Presto is 100% open source and can be found on Teradata's fork of Presto on GitHub `Teradata/presto <https://github.com/Teradata/presto>`_. All Teradata's contributions to Presto are contributed back to the upstream PrestoDB  version on GitHub `prestodb/presto <https://github.com/prestodb/presto>`_ in the form of `pull requests <https://github.com/prestodb/presto/pulls>`_.

We try very hard to remain as close to the PrestoDB distribution as possible. Teradata's distribution of Presto is always based on a PrestoDB release along with many of Teradata's open pull requests applied. The code in the pull requests have been rigorously tested to meet Teradata’s quality standards and these pull requests will eventually make their way in upstream. This could mean a feature in Teradata’s distribution of Presto may be available earlier than the feature in a PrestoDB release.

Versioning
----------
We follow the general concept  from the article on `semver.org <http://semver.org>`_ The general idea is to keep our versioning like this:
FBMajor.FBMinor.FBPatch-t.TDMinor.TDPatch

For example: 0.148-t.1.2 indicates Teradata’s distribution of Presto was based on PrestoDB’s distribution at 0.148. We add the –t suffix to denote the Teradata distribution. And the 1.2 is the Teradata minor release and Teradata patch number.


#!/bin/bash -e

cd $(dirname $(readlink -f $0))/../etc

VERSION=$(cd ..; mvn -B org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' )

presto-devenv hadoop start

presto-devenv presto build-image --presto-source ../.. --master-config master/ --worker-config worker/  --no-build
presto-devenv presto start

presto-devenv status --wait

echo Running product tests, you can attach debugger at port 5005
java \
  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar ../target/presto-product-tests-${VERSION}-executable.jar \
  --report-dir ../target/tests-report \
  $*
PRODUCT_TESTS_RETURN_STATUS=$?

read -n 1 -p "Should stop docker clusters (y/n)? " ANSWER
echo
if [ x$ANSWER == "xy" ]; then
  presto-devenv presto stop
  presto-devenv hadoop stop

  read -n 1 -p "Should clean docker clusters (y/n)? " ANSWER
  echo
  if [ x$ANSWER == "xy" ]; then
    presto-devenv presto clean
    presto-devenv hadoop clean
  fi
fi

exit $PRODUCT_TESTS_RETURN_STATUS

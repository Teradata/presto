# Presto product tests

## Test configuration

Test assume that you have hadoop and presto cluster running. For setting up development environment we
recommend presto-docker-presto-devenv project. This presto-docker-presto-devenv contains tools for building and running
docker images to be used on local developer machine for easy setup of presto instance together with dependencies
like hadoop. You can also start presto on your machine locally from IDE (please check main presto README).

To make it work you need either:
 - define environment variables for hadoop ${HADOOP_MASTER} and presto ${PRESTO_MASTER} with their IP numbers,
and ${PRESTO_PRODUCT_TESTS_ROOT} with path to the presto product test directory
 - create test-configuration-local.yaml with following (example content):

```
databases:
  hive:
    host: 172.16.2.10

  presto:
    host: 192.168.205.1
```

## Running tests

Product tests are not run by default. To start them use run following command:

```
java -jar target/presto-product-tests-*-executable.jar --config-local file://`pwd`/tempto-configuration-local.yaml
```

## Running tests with Using preconfigured docker based clusters

In case you do not have an access to hadoop cluster or do not want to spent your time on configuration, this section
describes how to run product tests with preconfigured docker based clusters.

### Prerequisites

* docker >= 1.5

[https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)

For linux users:
```
wget -qO- https://get.docker.com/ | sh
```

For Mac OS X you need to install docker-machine.

[https://docs.docker.com/engine/installation/mac/](https://docs.docker.com/engine/installation/mac/).

* Python >= 2.6

* Vagrant >= 1.8

* Presto docker development environment tool

```
pip install presto-docker-devenv
```

### Running product tests

Below manual describe how to set up and teardown docker clusters needed to run product tests. 
It also covers actual product tests execution with usage of these clusters.

All of the steps from 2 to 8 are covered by ```presto-product-tests/bin/run.sh```. 
You can use this script instead manually going through these steps. 

1. Create docker-machine (only for Mac OS X users)

When using Mac OS X and docker-machine, you need to create a machine with 4GB memory limit. 
It is required as this machine is going to run one node hadoop and three node presto cluster.

```
docker-machine create -d virtualbox --virtualbox-memory 4096 default
```

> Note when you are using Linux ```presto-devenv``` will bind hadoop and presto ports to localhost address.
> Please ensure that 8080, 8088, 10000, 50070, 500075 are free.

2. Start hadoop cluster

```
presto-devenv hadoop start
```

3. Build presto project

4. Create and start presto cluster

```
presto-devenv presto build-image --presto-source . --master-config presto-product-tests/etc/master/ --worker-config presto-product-tests/etc/worker/  --no-build
presto-devenv presto start
```

5. Wait all the components to be ready.

To see if all the components are ready you can use below command.

```
presto-devenv status
```

6. Run product tests

For Mac OS X users:

```
eval $(docker env <machine>)
cd presto-product-tests/etc
java -jar ../target/presto-product-tests-${VERSION}-executable.jar 
```

For Linux users:

```
cd presto-product-tests/etc
java -jar ../target/presto-product-tests-${VERSION}-executable.jar 
```

> Note that some tests may run queries too big to fit into docker resource constraints.
> To exclude these tests from execution you use below switch to run product tests command.
> `-x big_query,quarantine`

You can run product tests from your IDE, all you need to set is:
 - ensure that docker machine environment variables are set
 ```
  eval $(docker env <machine>)
 ```
 - set build directory to ```presto-product-tests/etc```


7. Stop clusters

```
presto-devenv presto stop
presto-devenv hadoop stop
```

8. Clean clusters data

This is going to remove docker containers and image files.

```
presto-devenv presto clean
presto-devenv hadoop clean
```

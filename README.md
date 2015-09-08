HBase on Apache Mesos
======================

Starts Master node, optional number of Stargate REST nodes and region slave nodes.

Prerequisites
--------------------------
1. Install `tar`, `unzip`, `wget` in your build host. Set proxy for maven / gradle and wget if needed.
2. Install `curl` for all hosts in cluster.
3. `$JAVA_HOME` needs to be set on the host running your HBase scheduler. This can be set through setting the environment variable on the host, `export JAVA_HOME=/path/to/jre`, or specifying the environment variable in Marathon.

Building HBase-Mesos
--------------------------
1. Customize configuration in `conf/*-site.xml`. All configuration files updated here will be used by the scheduler and also bundled with the executors.
2. `./bin/build-hbase`
3. Run `./bin/build-hbase nocompile` to skip the `gradlew clean package` step and just re-bundle the binaries.
4, Run `./bin/build-hbase install PATH` to extract final archive in given PATH.
5. To remove the project build output and downloaded binaries, run `./bin/build-hbase clean`.

**NOTE:** The build process builds the artifacts under the `$PROJ_DIR/build` directory.  A number of zip and tar files are cached under the `cache` directory for faster subsequent builds.   The tarball used for installation is hbase-mesos-x.x.x.tgz which contains the scheduler and the executor to be distributed.

Installing HBase-Mesos on your Cluster
--------------------------
1. Install HDFS in your cluster - you can use your current hadoop distribution or run HDFS on Apache Mesos - https://github.com/jan-zajic/mesos-hdfs
2. Upload `hbase-mesos-*.tgz` to a node in your Mesos cluster (which is built to `$PROJ_DIR/build/hbase-mesos-x.x.x.tgz`).
2. Extract it with `tar zxvf hbase-mesos-*.tgz`.
3. Optional: Customize any additional configurations that weren't updated at compile time in `hbase-mesos-*/conf/*-site.xml` Note that if you update hbase-site.xml, it will be used by the scheduler and bundled with the executors. However, core-site.xml and mesos-site.xml will be used by the scheduler only.
4. Check that `hostname` on that node resolves to a non-localhost IP; update /etc/hosts if necessary.

Starting HBase-Mesos scheduler standalone
--------------------------
1. `cd hbase-mesos-*`
2. `./bin/hbase-mesos`
3. Check the Mesos web console to wait until all tasks are RUNNING (monitor status in JN sandboxes)

Starting HBase-Mesos scheduler using marathon
--------------------------


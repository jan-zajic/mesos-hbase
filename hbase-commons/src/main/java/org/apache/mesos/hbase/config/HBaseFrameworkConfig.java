package org.apache.mesos.hbase.config;

import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Provides executor configurations for launching processes at the slave leveraging hadoop
 * configurations.
 */
@Singleton
public class HBaseFrameworkConfig {

  private Configuration hadoopConfig;

  private static final int DEFAULT_HADOOP_HEAP_SIZE = 512;
  private static final int DEFAULT_EXECUTOR_HEAP_SIZE = 256;
  private static final int DEFAULT_SLAVENODE_HEAP_SIZE = 1024;
  private static final int DEFAULT_MASTERNODE_HEAP_SIZE = 4096;
  private static final int DEFAULT_STARGATE_HEAP_SIZE = 256;

  private static final double DEFAULT_CPUS = 0.5;
  private static final double DEFAULT_EXECUTOR_CPUS = DEFAULT_CPUS;
  private static final double DEFAULT_NAMENODE_CPUS = 1;
  private static final double DEFAULT_STARGATE_CPUS = 0.2;
  private static final double DEFAULT_DATANODE_CPUS = 1;

  private static final double DEFAULT_JVM_OVERHEAD = 1.15;
  private static final int DEFAULT_FAILOVER_TIMEOUT = 31449600;
  private static final int DEFAULT_ZK_TIME_MS = 20000;
  private static final int DEFAULT_RECONCILIATION_TIMEOUT = 30;
  private static final int DEFAULT_DEADNODE_TIMEOUT = 90;

  private static final int DEFAULT_STARGATE_NODE_COUNT = 2;
  
  private static final int DEFAULT_STARGATE_PORT = 8088;
  
  private final Log log = LogFactory.getLog(HBaseFrameworkConfig.class);

  public HBaseFrameworkConfig(Configuration conf) {
    setConf(conf);
  }

  private void setConf(Configuration conf) {
    this.hadoopConfig = conf;
  }

  private Configuration getConf() {
    return hadoopConfig;
  }

  public HBaseFrameworkConfig() {
    // The path is configurable via the mesos.conf.path system property
    // so it can be changed when starting up the scheduler via bash
    Properties props = System.getProperties();
    Path configPath = new Path(props.getProperty("mesos.conf.path", "etc/hadoop/mesos-site.xml"));
    Configuration configuration = new Configuration();
    configuration.addResource(configPath);
    setConf(configuration);
  }

  public String getPrincipal() {
    return getConf().get("mesos.hbase.principal", "");
  }

  public String getSecret() {
    return getConf().get("mesos.hbase.secret", "");
  }

  public boolean cramCredentialsEnabled() {
    String principal = getPrincipal();
    String secret = getSecret();
    boolean principalExists = !principal.isEmpty();
    boolean secretExists = !secret.isEmpty();

    return principalExists && secretExists;
  }

  public boolean usingMesosDns() {
    return Boolean.valueOf(getConf().get("mesos.hbase.mesosdns", "false"));
  }

  public String getMesosDnsDomain() {
    return getConf().get("mesos.hbase.mesosdns.domain", "mesos");
  }

  public boolean usingNativeHadoopBinaries() {
    return Boolean.valueOf(getConf().get("mesos.hbase.native-hadoop-binaries", "false"));
  }

  public String getExecutorPath() {
    return getConf().get("mesos.hbase.executor.path", ".");
  }

  public String getConfigPath() {
    return getConf().get("mesos.hbase.config.path", "conf/hbase-site.xml");
  }

  public int getHadoopHeapSize() {
    return getConf().getInt("mesos.hbase.hadoop.heap.size", DEFAULT_HADOOP_HEAP_SIZE);
  }

  public int getSlaveNodeHeapSize() {
    return getConf().getInt("mesos.hbase.slave.heap.size", DEFAULT_SLAVENODE_HEAP_SIZE);
  }

  public int getMasterNodeHeapSize() {
    return getConf().getInt("mesos.hbase.master.heap.size", DEFAULT_MASTERNODE_HEAP_SIZE);
  }

  public int getStargateNodeHeapSize() {
    return getConf().getInt("mesos.hbase.stargate.heap.size", DEFAULT_STARGATE_HEAP_SIZE);
  }
  
  public int getExecutorHeap() {
    return getConf().getInt("mesos.hbase.executor.heap.size", DEFAULT_EXECUTOR_HEAP_SIZE);
  }

  public int getStargateNodeCount() {
    return getConf().getInt("mesos.hbase.stargate.node.count", DEFAULT_STARGATE_NODE_COUNT);
  }
  
  public int getStargateServerPort() {
    return getConf().getInt("mesos.hbase.stargate.port", DEFAULT_STARGATE_PORT);  
  }
  
  public int getTaskHeapSize(String taskName) {
    int size;
    switch (taskName) {
      case "masternode":
        size = getMasterNodeHeapSize();
        break;
      case "slavenode":
        size = getSlaveNodeHeapSize();
        break;
      case "stargatenode":
        size = getStargateNodeHeapSize();
        break;
      default:
        final String msg = "Invalid request for heapsize for taskName = " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return size;
  }

  public double getJvmOverhead() {
    return getConf().getDouble("mesos.hbase.jvm.overhead", DEFAULT_JVM_OVERHEAD);
  }

  public String getJvmOpts() {
    return getConf().get(
        "mesos.hbase.jvm.opts", ""
            + "-XX:+UseConcMarkSweepGC "
            + "-XX:+CMSClassUnloadingEnabled "
            + "-XX:+UseTLAB "
            + "-XX:+AggressiveOpts "
            + "-XX:+UseCompressedOops "
            + "-XX:+UseFastEmptyMethods "
            + "-XX:+UseFastAccessorMethods "
            + "-Xss256k "
            + "-XX:+AlwaysPreTouch "
            + "-XX:+UseParNewGC "
            + "-Djava.library.path=/usr/lib:/usr/local/lib:lib/native");
  }

  public double getExecutorCpus() {
    return getConf().getDouble("mesos.hbase.executor.cpus", DEFAULT_EXECUTOR_CPUS);
  }

  public double getMasterNodeCpus() {
    return getConf().getDouble("mesos.hbase.master.cpus", DEFAULT_NAMENODE_CPUS);
  }
  
  public double getStargateNodeCpus() {
    return getConf().getDouble("mesos.hbase.stargate.cpus", DEFAULT_STARGATE_CPUS);
  }

  public double getSlaveNodeCpus() {
    return getConf().getDouble("mesos.hbase.slave.cpus", DEFAULT_DATANODE_CPUS);
  }

  public double getTaskCpus(String taskName) {
    double cpus = DEFAULT_CPUS;
    switch (taskName) {
      case "masternode":
        cpus = getMasterNodeCpus();
        break;
      case "slavenode":
        cpus = getSlaveNodeCpus();
        break;
      case "stargatenode":
        cpus = getStargateNodeCpus();
      default:
        final String msg = "Invalid request for CPUs for taskName= " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return cpus;
  }

  public String getFrameworkName() {
    return getConf().get("mesos.hbase.framework.name", "hbase");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", DEFAULT_FAILOVER_TIMEOUT);
  }

  // TODO(elingg) Most likely this user name will change to HDFS
  public String getHbaseUser() {
    return getConf().get("mesos.hbase.user", "root");
  }

  // TODO(elingg) This role needs to be updated.
  public String getHbaseRole() {
    return getConf().get("mesos.hbase.role", "*");
  }

  public String getMesosMasterUri() {
    return getConf().get("mesos.master.uri", "zk://localhost:2181/mesos");
  }

  public String getDataDir() {
    return getConf().get("mesos.hbase.data.dir", "/var/lib/hbase/data");
  }

  public String getSecondaryDataDir() {
    return getConf().get("mesos.hbase.secondary.data.dir", "/var/run/hadoop-hbase");
  }

  public String getHaZookeeperQuorum() {
    return getConf().get("mesos.hbase.zkfc.ha.zookeeper.quorum", "localhost:2181");
  }

  public String getHbaseRootDir() {
    return getConf().get("hbase.rootdir", "file:///var/hbase/data");
  }

  public Boolean usingMesosHdfs() {
    return Boolean.valueOf(getConf().get("mesos.hbase.mesoshdfs", "true"));
  }

  public String getDfsNameServices() {
    return getConf().get("dfs.nameservices", "hdfs");
  }

  public String getStateZkServers() {
    return getConf().get("mesos.hbase.state.zk", "localhost:2181");
  }

  public int getStateZkTimeout() {
    return getConf().getInt("mesos.hbase.state.zk.timeout.ms", DEFAULT_ZK_TIME_MS);
  }

  public String getNativeLibrary() {
    return getConf().get("mesos.native.library", "/usr/local/lib/libmesos.so");
  }

  public String getFrameworkMountPath() {
    return getConf().get("mesos.hbase.framework.mnt.path", "/opt/mesosphere");
  }

  public String getFrameworkHostAddress() {
    String hostAddress = getConf().get("mesos.hbase.framework.hostaddress");
    if (hostAddress == null) {
      try {
        hostAddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new ConfigurationException(e);
      }
    }
    return hostAddress;
  }

  // The port can be changed by setting the PORT0 environment variable
  // See /bin/hbase-mesos for more details
  public int getConfigServerPort() {
    String configServerPortString = System.getProperty("mesos.hbase.config.server.port");
    if (configServerPortString == null) {
      configServerPortString = getConf().get("mesos.hbase.config.server.port", "8765");
    }
    return Integer.parseInt(configServerPortString);
  }

  public int getReconciliationTimeout() {
    return getConf().getInt("mesos.reconciliation.timeout.seconds", DEFAULT_RECONCILIATION_TIMEOUT);
  }

  public int getDeadNodeTimeout() {
    return getConf().getInt("mesos.hbase.deadnode.timeout.seconds", DEFAULT_DEADNODE_TIMEOUT);
  }

  public String getJreUrl() {
    return getConf().get("mesos.hbase.jre-url",
        "https://downloads.mesosphere.io/java/jre-7u76-linux-x64.tar.gz");
  }

  public String getLdLibraryPath() {
    return getConf().get("mesos.hbase.ld-library-path", "/usr/local/lib");
  }

  public String getJreVersion() {
    return getConf().get("mesos.hbase.jre-version", "jre1.7.0_76");
  }
}

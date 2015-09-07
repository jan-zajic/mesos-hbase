package org.apache.mesos.hbase.util;

/**
 * Constants for HDFS.
 */
public final class HBaseConstants {

  // Total number of master nodes
  public static final Integer TOTAL_MASTER_NODES = 1;

  public static final String DEVELOPMENT_MODE_PROPERTY = "mesos.hbase.devel";
  
  // Messages
  public static final String RELOAD_CONFIG = "reload config";

  // NodeIds
  public static final String MASTER_NODE_ID = "masternode";
  public static final String SLAVE_NODE_ID = "slavenode";
  
  // NameNode TaskId
  public static final String MASTER_NODE_TASKID = ".masternode.masternode.";

  // ExecutorsIds
  public static final String NODE_EXECUTOR_ID = "NodeExecutor";
  public static final String MASTER_NODE_EXECUTOR_ID = "NodeExecutor";

  // Path to Store HDFS Binary
  public static final String HBASE_BINARY_DIR = "hbase";

  // Current HDFS Binary File Name
  public static final String HBASE_BINARY_FILE_NAME = "hbase-mesos-executor-0.1.0.tgz";

  // HDFS Config File Name
  public static final String HBASE_CONFIG_FILE_NAME = "hbase-site.xml";

  //region servers file name
  public static final String REGION_SERVERS_FILENAME = "regionservers";
  
  private HBaseConstants() {
  }
  
  public static boolean isDevelopmentMode()
  {
      return System.getProperty("mesos.hbase.devel") != null;
  }
  
}

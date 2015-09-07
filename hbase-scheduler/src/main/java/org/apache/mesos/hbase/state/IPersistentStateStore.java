package org.apache.mesos.hbase.state;

import org.apache.mesos.Protos;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Entry point for persistence for the HDFS scheduler.
 */
public interface IPersistentStateStore {

  void setFrameworkId(Protos.FrameworkID id);

  Protos.FrameworkID getFrameworkId();

  void removeTaskId(String taskId);

  Set<String> getAllTaskIds();

  void addHBaseNode(Protos.TaskID taskId, String hostname, String taskType, String taskName);

  Map<String, String> getPrimaryNodeTaskNames();

  List<String> getDeadNameNodes();

  List<String> getDeadDataNodes();

  Map<String, String> getPrimaryNodes();
  
  Map<String, String> getRegionNodes();

  boolean dataNodeRunningOnSlave(String hostname);

  boolean nameNodeRunningOnSlave(String hostname);

}

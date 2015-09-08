package org.apache.mesos.hbase.state;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.mesos.hbase.config.HBaseFrameworkConfig;
import org.apache.mesos.hbase.util.HBaseConstants;

import static org.apache.mesos.hbase.util.NodeTypes.*;

/**
 * Persistence is handled by the Persistent State classes.  This class does the following:.
 * 1) transforms raw types to hdfs types and protobuf types
 * 2) handles exception logic and rethrows PersistenceException
 */

@Singleton
public class PersistentStateStore implements IPersistentStateStore {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private IHBaseStore hdfsStore;

  private DeadNodeTracker deadNodeTracker;

  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String NAMENODE_TASKNAMES_KEY = "nameNodeTaskNames";

  // TODO (elingg) we need to also track ZKFC's state
  // TODO (nicgrayson) add tests with in-memory state implementation for zookeeper

  @Inject
  public PersistentStateStore(HBaseFrameworkConfig hdfsFrameworkConfig, IHBaseStore hdfsStore,
      DeadNodeTracker deadNodeTracker) {
    if (!HBaseConstants.isDevelopmentMode())
      MesosNativeLibrary.load(hdfsFrameworkConfig.getNativeLibrary());
    this.hdfsStore = hdfsStore;
    this.deadNodeTracker = deadNodeTracker;

    int deadNameNodes = getDeadNameNodes().size();
    int deadDataNodes = getDeadDataNodes().size();

    deadNodeTracker.resetDeadNodeTimeStamps(deadNameNodes, deadDataNodes);
  }

  @Override
  public void setFrameworkId(Protos.FrameworkID id) {

    try {
      if(id == null)
        hdfsStore.setRawValueForId(FRAMEWORK_ID_KEY, new byte[]{});
      else
        hdfsStore.setRawValueForId(FRAMEWORK_ID_KEY, id.toByteArray());
    } catch (ExecutionException | InterruptedException e) {
      logger.error("Unable to set frameworkId", e);
      throw new PersistenceException(e);
    }
  }

  @Override
  public Protos.FrameworkID getFrameworkId() {
    Protos.FrameworkID frameworkID = null;
    byte[] existingFrameworkId;
    try {
      existingFrameworkId = hdfsStore.getRawValueForId(FRAMEWORK_ID_KEY);
      if (existingFrameworkId.length > 0) {
        frameworkID = Protos.FrameworkID.parseFrom(existingFrameworkId);
      }
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Unable to get FrameworkID from state store.", e);
      throw new PersistenceException(e);
    } catch (InvalidProtocolBufferException e) {
      logger.error("Unable to parse frameworkID", e);
      throw new PersistenceException(e);
    }
    return frameworkID;
  }

  @Override
  public void removeTaskId(String taskId) {
    // TODO (elingg) optimize this method/ Possibly index by task id instead of hostname/
    // Possibly call removeTask(slaveId, taskId) to avoid iterating through all maps

    if (removeTaskIdFromNameNodes(taskId) ||
        removeTaskIdFromDataNodes(taskId)) {
      logger.debug("task id: " + taskId + " removed");
    } else {
      logger.warn("task id: " + taskId + " request to be removed doesn't exist");
    }
  }

  @Override
  public void addHBaseNode(Protos.TaskID taskId, String hostname, String taskType, String taskName) {
    switch (taskType) {
      case HBaseConstants.MASTER_NODE_ID:
        addNameNode(taskId, hostname, taskName);
        break;
      case HBaseConstants.SLAVE_NODE_ID:
        addDataNode(taskId, hostname);
        break;
      default:
        logger.error("Task name unknown");
    }
  }

  private void addDataNode(Protos.TaskID taskId, String hostname) {
    Map<String, String> dataNodes = getRegionNodes();
    dataNodes.put(hostname, taskId.getValue());
    setDataNodes(dataNodes);
  }

  private void addNameNode(Protos.TaskID taskId, String hostname, String taskName) {
    Map<String, String> nameNodes = getPrimaryNodes();
    nameNodes.put(hostname, taskId.getValue());
    setNameNodes(nameNodes);
    Map<String, String> nameNodeTaskNames = getPrimaryNodeTaskNames();
    nameNodeTaskNames.put(taskId.getValue(), taskName);
    setNameNodeTaskNames(nameNodeTaskNames);
  }

  @Override
  public Map<String, String> getPrimaryNodeTaskNames() {
    return getNodesMap(NAMENODE_TASKNAMES_KEY);
  }

  @Override
  public List<String> getDeadNameNodes() {
    List<String> deadNameHosts = new ArrayList<>();

    if (deadNodeTracker.masterNodeTimerExpired()) {
      removeDeadNameNodes();
    } else {
      Map<String, String> nameNodes = getPrimaryNodes();
      final Set<Map.Entry<String, String>> nameNodeEntries = nameNodes.entrySet();
      for (Map.Entry<String, String> nameNode : nameNodeEntries) {
        if (nameNode.getValue() == null) {
          deadNameHosts.add(nameNode.getKey());
        }
      }
    }
    return deadNameHosts;
  }

  private void removeDeadNameNodes() {
    deadNodeTracker.resetNameNodeTimeStamp();
    Map<String, String> nameNodes = getPrimaryNodes();
    List<String> deadNameHosts = getDeadNameNodes();
    for (String deadNameHost : deadNameHosts) {
      nameNodes.remove(deadNameHost);
      logger.info("Removing NN Host: " + deadNameHost);
    }
    setNameNodes(nameNodes);
  }

  @Override
  public List<String> getDeadDataNodes() {
    List<String> deadDataHosts = new ArrayList<>();

    if (deadNodeTracker.slaveNodeTimerExpired()) {
      removeDeadDataNodes();
    } else {
      Map<String, String> dataNodes = getRegionNodes();
      final Set<Map.Entry<String, String>> dataNodeEntries = dataNodes.entrySet();
      for (Map.Entry<String, String> dataNode : dataNodeEntries) {
        if (dataNode.getValue() == null) {
          deadDataHosts.add(dataNode.getKey());
        }
      }
    }
    return deadDataHosts;
  }

  private void removeDeadDataNodes() {
    deadNodeTracker.resetDataNodeTimeStamp();
    Map<String, String> dataNodes = getRegionNodes();
    List<String> deadDataHosts = getDeadDataNodes();
    for (String deadDataHost : deadDataHosts) {
      dataNodes.remove(deadDataHost);
      logger.info("Removing DN Host: " + deadDataHost);
    }
    setDataNodes(dataNodes);
  }

  @Override
  public Map<String, String> getPrimaryNodes() {
    return getNodesMap(MASTERNODES_KEY);
  }

  @Override
  public Map<String, String> getRegionNodes() {
    return getNodesMap(SLAVENODES_KEY);
  }

  @Override
  public boolean dataNodeRunningOnSlave(String hostname) {
    return getRegionNodes().containsKey(hostname);
  }

  @Override
  public boolean nameNodeRunningOnSlave(String hostname) {
    return getPrimaryNodes().containsKey(hostname);
  }

  @Override
  public Set<String> getAllTaskIds() {
    Set<String> allTaskIds = new HashSet<String>();
    Collection<String> nameNodes = getPrimaryNodes().values();
    Collection<String> dataNodes = getRegionNodes().values();
    allTaskIds.addAll(nameNodes);
    allTaskIds.addAll(dataNodes);
    return allTaskIds;

  }

  private Map<String, String> getNodesMap(String key) {
    try {
      HashMap<String, String> nodesMap = hdfsStore.get(key);
      if (nodesMap == null) {
        return new HashMap<>();
      }
      return nodesMap;
    } catch (Exception e) {
      logger.error(String.format("Error while getting %s in persistent state", key), e);
      return new HashMap<>();
    }
  }

  private boolean removeTaskIdFromNameNodes(String taskId) {
    boolean nodesModified = false;

    Map<String, String> nameNodes = getPrimaryNodes();
    if (nameNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : nameNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          nameNodes.put(entry.getKey(), null);
          setNameNodes(nameNodes);
          Map<String, String> nameNodeTaskNames = getPrimaryNodeTaskNames();
          nameNodeTaskNames.remove(taskId);
          setNameNodeTaskNames(nameNodeTaskNames);

          deadNodeTracker.resetNameNodeTimeStamp();
          nodesModified = true;
        }
      }
    }
    return nodesModified;
  }

  private boolean removeTaskIdFromDataNodes(String taskId) {
    boolean nodesModified = false;

    Map<String, String> dataNodes = getRegionNodes();
    if (dataNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : dataNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          dataNodes.put(entry.getKey(), null);
          setDataNodes(dataNodes);

          deadNodeTracker.resetDataNodeTimeStamp();
          nodesModified = true;
        }
      }
    }
    return nodesModified;
  }

  private void setNameNodes(Map<String, String> nameNodes) {
    try {
      hdfsStore.set(MASTERNODES_KEY, nameNodes);
    } catch (Exception e) {
      logger.error("Error while setting name nodes in persistent state", e);
    }
  }

  private void setNameNodeTaskNames(Map<String, String> nameNodeTaskNames) {
    try {
      hdfsStore.set(NAMENODE_TASKNAMES_KEY, nameNodeTaskNames);
    } catch (Exception e) {
      logger.error("Error while setting name node task names in persistent state", e);
    }
  }

  private void setDataNodes(Map<String, String> dataNodes) {
    try {
      hdfsStore.set(SLAVENODES_KEY, dataNodes);
    } catch (Exception e) {
      logger.error("Error while setting data nodes in persistent state", e);
    }
  }
}

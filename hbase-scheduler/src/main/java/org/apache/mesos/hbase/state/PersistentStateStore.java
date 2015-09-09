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
 * Persistence is handled by the Persistent State classes. This class does the
 * following:. 1) transforms raw types to hbase types and protobuf types 2)
 * handles exception logic and rethrows PersistenceException
 */
@Singleton
public class PersistentStateStore implements IPersistentStateStore
{

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private IHBaseStore hbaseStore;

  private DeadNodeTracker deadNodeTracker;

  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String MASTERNODE_TASKNAMES_KEY = "masterNodeTaskNames";

  // TODO (elingg) we need to also track ZKFC's state
  // TODO (nicgrayson) add tests with in-memory state implementation for zookeeper
  @Inject
  public PersistentStateStore(HBaseFrameworkConfig hbaseFrameworkConfig, IHBaseStore hbaseStore,
      DeadNodeTracker deadNodeTracker)
  {
    if (!HBaseConstants.isDevelopmentMode()) {
      MesosNativeLibrary.load(hbaseFrameworkConfig.getNativeLibrary());
    }
    this.hbaseStore = hbaseStore;
    this.deadNodeTracker = deadNodeTracker;

    int deadMasterNodes = getDeadMasterNodes().size();
    int deadDataNodes = getDeadDataNodes().size();
    int deadStargateNodes = getDeadStargateNodes().size();

    deadNodeTracker.resetDeadNodeTimeStamps(deadMasterNodes, deadDataNodes, deadStargateNodes);
  }

  @Override
    public void setFrameworkId(Protos.FrameworkID id)
    {

        try {
            if (id == null) {
                hbaseStore.setRawValueForId(FRAMEWORK_ID_KEY, new byte[]{});
            } else {
                hbaseStore.setRawValueForId(FRAMEWORK_ID_KEY, id.toByteArray());
            }
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Unable to set frameworkId", e);
            throw new PersistenceException(e);
        }
    }

  @Override
    public Protos.FrameworkID getFrameworkId()
    {
        Protos.FrameworkID frameworkID = null;
        byte[] existingFrameworkId;
        try {
            existingFrameworkId = hbaseStore.getRawValueForId(FRAMEWORK_ID_KEY);
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
  public void removeTaskId(String taskId)
  {
    // TODO (elingg) optimize this method/ Possibly index by task id instead of hostname/
    // Possibly call removeTask(slaveId, taskId) to avoid iterating through all maps

    if (removeTaskIdFromMasterNodes(taskId)
        || removeTaskIdFromDataNodes(taskId)) {
      logger.debug("task id: " + taskId + " removed");
    } else {
      logger.warn("task id: " + taskId + " request to be removed doesn't exist");
    }
  }

  @Override
  public void addHBaseNode(Protos.TaskID taskId, String hostname, String taskType, String taskName)
  {
    switch (taskType) {
      case HBaseConstants.MASTER_NODE_ID:
        addPrimaryNode(taskId, hostname, taskName);
        break;
      case HBaseConstants.SLAVE_NODE_ID:
        addDataNode(taskId, hostname);
        break;
      case HBaseConstants.STARGATE_NODE_ID:
        addStargateNode(taskId, hostname);
      default:
        logger.error("Task name unknown");
    }
  }

  private void addDataNode(Protos.TaskID taskId, String hostname)
  {
    Map<String, String> dataNodes = getRegionNodes();
    dataNodes.put(hostname, taskId.getValue());
    setDataNodes(dataNodes);
  }

  private void addStargateNode(Protos.TaskID taskId, String hostname)
  {
    Map<String, String> dataNodes = getStargateNodes();
    dataNodes.put(hostname, taskId.getValue());
    setStargateNodes(dataNodes);
  }

  private void addPrimaryNode(Protos.TaskID taskId, String hostname, String taskName)
  {
    Map<String, String> primaryNodes = getPrimaryNodes();
    primaryNodes.put(hostname, taskId.getValue());
    setPrimaryNodes(primaryNodes);
    Map<String, String> primaryNodeTaskNames = getPrimaryNodeTaskNames();
    primaryNodeTaskNames.put(taskId.getValue(), taskName);
    setPrimaryNodeTaskNames(primaryNodeTaskNames);
  }

  @Override
  public Map<String, String> getPrimaryNodeTaskNames()
  {
    return getNodesMap(MASTERNODE_TASKNAMES_KEY);
  }

  @Override
    public List<String> getDeadMasterNodes()
    {
        List<String> deadMasterHosts = new ArrayList<>();

        if (deadNodeTracker.masterNodeTimerExpired()) {
            removeDeadPrimaryNodes();
        } else {
            Map<String, String> masterNodes = getPrimaryNodes();
            final Set<Map.Entry<String, String>> masterNodeEntries = masterNodes.entrySet();
            for (Map.Entry<String, String> masterNode : masterNodeEntries) {
                if (masterNode.getValue() == null) {
                    deadMasterHosts.add(masterNode.getKey());
                }
            }
        }
        return deadMasterHosts;
    }

  private void removeDeadPrimaryNodes()
  {
    deadNodeTracker.resetMasterNodeTimeStamp();
    Map<String, String> masterNodes = getPrimaryNodes();
    List<String> deadMasterHosts = getDeadMasterNodes();
    for (String deadMasterHost : deadMasterHosts) {
      masterNodes.remove(deadMasterHost);
      logger.info("Removing dead master node Host: " + deadMasterHost);
    }
    setPrimaryNodes(masterNodes);
  }

  @Override
    public List<String> getDeadDataNodes()
    {
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

  private void removeDeadDataNodes()
  {
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
  public Map<String, String> getPrimaryNodes()
  {
    return getNodesMap(MASTERNODES_KEY);
  }

  @Override
  public Map<String, String> getRegionNodes()
  {
    return getNodesMap(SLAVENODES_KEY);
  }

  @Override
  public boolean slaveNodeRunningOnSlave(String hostname)
  {
    return getRegionNodes().containsKey(hostname);
  }

  @Override
  public boolean masterNodeRunningOnSlave(String hostname)
  {
    return getPrimaryNodes().containsKey(hostname);
  }

  @Override
  public Set<String> getAllTaskIds()
  {
    Set<String> allTaskIds = new HashSet<String>();
    Collection<String> masterNodes = getPrimaryNodes().values();
    Collection<String> dataNodes = getRegionNodes().values();
    allTaskIds.addAll(masterNodes);
    allTaskIds.addAll(dataNodes);
    return allTaskIds;

  }

  private Map<String, String> getNodesMap(String key)
    {
        try {
            HashMap<String, String> nodesMap = hbaseStore.get(key);
            if (nodesMap == null) {
                return new HashMap<>();
            }
            return nodesMap;
        } catch (Exception e) {
            logger.error(String.format("Error while getting %s in persistent state", key), e);
            return new HashMap<>();
        }
    }

  private boolean removeTaskIdFromMasterNodes(String taskId)
  {
    boolean nodesModified = false;

    Map<String, String> primaryNodes = getPrimaryNodes();
    if (primaryNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : primaryNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          primaryNodes.put(entry.getKey(), null);
          setPrimaryNodes(primaryNodes);
          Map<String, String> masterNodeTaskNames = getPrimaryNodeTaskNames();
          masterNodeTaskNames.remove(taskId);
          setPrimaryNodeTaskNames(masterNodeTaskNames);

          deadNodeTracker.resetMasterNodeTimeStamp();
          nodesModified = true;
        }
      }
    }
    return nodesModified;
  }

  private boolean removeTaskIdFromDataNodes(String taskId)
  {
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

  private void setPrimaryNodes(Map<String, String> primaryNodes)
  {
    try {
      hbaseStore.set(MASTERNODES_KEY, primaryNodes);
    } catch (Exception e) {
      logger.error("Error while setting primary nodes in persistent state", e);
    }
  }

  private void setPrimaryNodeTaskNames(Map<String, String> primaryNodeTaskNames)
  {
    try {
      hbaseStore.set(MASTERNODE_TASKNAMES_KEY, primaryNodeTaskNames);
    } catch (Exception e) {
      logger.error("Error while setting primary node task names in persistent state", e);
    }
  }

  private void setDataNodes(Map<String, String> dataNodes)
  {
    try {
      hbaseStore.set(SLAVENODES_KEY, dataNodes);
    } catch (Exception e) {
      logger.error("Error while setting data nodes in persistent state", e);
    }
  }

  @Override
  public Map<String, String> getStargateNodes()
  {
    return getNodesMap(STARGATENODES_KEY);
  }

  @Override
    public List<String> getDeadStargateNodes()
    {
        List<String> deadDataHosts = new ArrayList<>();

        if (deadNodeTracker.stargateNodeTimerExpired()) {
            removeDeadStargateNodes();
        } else {
            Map<String, String> dataNodes = getStargateNodes();
            final Set<Map.Entry<String, String>> dataNodeEntries = dataNodes.entrySet();
            for (Map.Entry<String, String> dataNode : dataNodeEntries) {
                if (dataNode.getValue() == null) {
                    deadDataHosts.add(dataNode.getKey());
                }
            }
        }
        return deadDataHosts;
    }

  private void removeDeadStargateNodes()
  {
    deadNodeTracker.resetStargateNodeTimeStamp();
    Map<String, String> dataNodes = getStargateNodes();
    List<String> deadDataHosts = getDeadStargateNodes();
    for (String deadDataHost : deadDataHosts) {
      dataNodes.remove(deadDataHost);
      logger.info("Removing Rest Host: " + deadDataHost);
    }
    setStargateNodes(dataNodes);
  }

  private void setStargateNodes(Map<String, String> dataNodes)
  {
    try {
      hbaseStore.set(STARGATENODES_KEY, dataNodes);
    } catch (Exception e) {
      logger.error("Error while setting stargate nodes in persistent state", e);
    }
  }

}

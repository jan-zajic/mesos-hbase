package org.apache.mesos.hbase.state;

import org.apache.commons.lang.time.DateUtils;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.mesos.hbase.config.HBaseFrameworkConfig;

import static org.apache.mesos.hbase.util.NodeTypes.*;

/**
 * Manages the timeout timestamps for each node type.
 */
public class DeadNodeTracker {

  private Map<String, Timestamp> timestampMap;
  private String[] nodes = {
      JOURNALNODES_KEY, NAMENODES_KEY, DATANODES_KEY};

  private HBaseFrameworkConfig hdfsFrameworkConfig;

  public DeadNodeTracker(HBaseFrameworkConfig hdfsFrameworkConfig) {
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
    initializeTimestampMap();
  }

  private void initializeTimestampMap() {
    timestampMap = new HashMap<>();
    for (String node : nodes) {
      resetNodeTimeStamp(node);
    }
  }

  private void resetNodeTimeStamp(String nodeType) {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
    timestampMap.put(nodeType, new Timestamp(date.getTime()));
  }

  public void resetDeadNodeTimeStamps(int deadJournalNodes, int deadNameNodes, int deadDataNodes) {
    if (deadJournalNodes > 0) {
      resetJournalNodeTimeStamp();
    }

    if (deadNameNodes > 0) {
      resetNameNodeTimeStamp();
    }

    if (deadDataNodes > 0) {
      resetDataNodeTimeStamp();
    }
  }

  public void resetJournalNodeTimeStamp() {
    resetNodeTimeStamp(JOURNALNODES_KEY);
  }

  public void resetNameNodeTimeStamp() {
    resetNodeTimeStamp(NAMENODES_KEY);
  }

  public void resetDataNodeTimeStamp() {
    resetNodeTimeStamp(DATANODES_KEY);
  }

  public boolean journalNodeTimerExpired() {
    return nodeTimerExpired(JOURNALNODES_KEY);
  }

  public boolean nameNodeTimerExpired() {
    return nodeTimerExpired(NAMENODES_KEY);
  }

  public boolean dataNodeTimerExpired() {
    return nodeTimerExpired(DATANODES_KEY);
  }

  private boolean nodeTimerExpired(String nodeType) {
    Timestamp timestamp = timestampMap.get(nodeType);
    return timestamp != null && timestamp.before(new Date());
  }
}

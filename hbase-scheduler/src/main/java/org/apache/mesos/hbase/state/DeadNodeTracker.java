package org.apache.mesos.hbase.state;

import com.google.inject.Inject;
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
public class DeadNodeTracker
{

  private Map<String, Timestamp> timestampMap;
  private String[] nodes = {
      MASTERNODES_KEY, SLAVENODES_KEY};

  private HBaseFrameworkConfig hbaseFrameworkConfig;

  @Inject
  public DeadNodeTracker(HBaseFrameworkConfig hbaseFrameworkConfig)
  {
    this.hbaseFrameworkConfig = hbaseFrameworkConfig;
    initializeTimestampMap();
  }

  private void initializeTimestampMap()
    {
        timestampMap = new HashMap<>();
        for (String node : nodes) {
            resetNodeTimeStamp(node);
        }
    }

  private void resetNodeTimeStamp(String nodeType)
  {
    Date date = DateUtils.addSeconds(new Date(), hbaseFrameworkConfig.getDeadNodeTimeout());
    timestampMap.put(nodeType, new Timestamp(date.getTime()));
  }

  public void resetDeadNodeTimeStamps(int deadNameNodes, int deadDataNodes, int deadStargateNodes)
  {
    if (deadNameNodes > 0) {
      resetNameNodeTimeStamp();
    }

    if (deadDataNodes > 0) {
      resetDataNodeTimeStamp();
    }

    if (deadStargateNodes > 0) {
      resetStargateNodeTimeStamp();
    }
  }

  public void resetNameNodeTimeStamp()
  {
    resetNodeTimeStamp(MASTERNODES_KEY);
  }

  public void resetDataNodeTimeStamp()
  {
    resetNodeTimeStamp(SLAVENODES_KEY);
  }

  public boolean masterNodeTimerExpired()
  {
    return nodeTimerExpired(MASTERNODES_KEY);
  }

  public boolean slaveNodeTimerExpired()
  {
    return nodeTimerExpired(SLAVENODES_KEY);
  }

  private boolean nodeTimerExpired(String nodeType)
  {
    Timestamp timestamp = timestampMap.get(nodeType);
    return timestamp != null && timestamp.before(new Date());
  }

  boolean stargateNodeTimerExpired()
  {
    return nodeTimerExpired(STARGATENODES_KEY);
  }

  void resetStargateNodeTimeStamp()
  {
    resetNodeTimeStamp(STARGATENODES_KEY);
  }
}

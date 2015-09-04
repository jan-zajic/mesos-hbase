package org.apache.mesos.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hbase.scheduler.HdfsScheduler;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import org.apache.mesos.hbase.config.HBaseFrameworkConfig;

/**
 * Provides DNS resolving specific to HDFS.
 */
public class DnsResolver {
  private final Log log = LogFactory.getLog(DnsResolver.class);

  static final int NN_TIMER_PERIOD = 10000;

  private final HdfsScheduler scheduler;
  private final HBaseFrameworkConfig hdfsFrameworkConfig;

  public DnsResolver(HdfsScheduler scheduler, HBaseFrameworkConfig hdfsFrameworkConfig) {
    this.scheduler = scheduler;
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
  }

  public boolean nameNodesResolvable() {
    if (!hdfsFrameworkConfig.usingMesosDns()) {
      return true;
    } //short circuit since Mesos handles this otherwise
    Set<String> hosts = new HashSet<>();
    for (int i = 1; i <= HBaseConstants.TOTAL_MASTER_NODES; i++) {
      hosts.add(HBaseConstants.MASTER_NODE_ID + i + "." + hdfsFrameworkConfig.getFrameworkName() +
        "." + hdfsFrameworkConfig.getMesosDnsDomain());
    }
    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.warn("Couldn't resolve host " + host);
        success = false;
        break;
      }
    }
    return success;
  }

  public void sendMessageAfterNNResolvable(final SchedulerDriver driver,
      final Protos.TaskID taskId, final Protos.SlaveID slaveID, final String message) {
    if (!hdfsFrameworkConfig.usingMesosDns()) {
      // short circuit since Mesos handles this otherwise
      scheduler.sendMessageTo(driver, taskId, slaveID, message);
      return;
    }
    Timer timer = new Timer();
    PreNNInitTask task = new PreNNInitTask(this, scheduler, driver, taskId, slaveID, message);
    timer.scheduleAtFixedRate(task, 0, NN_TIMER_PERIOD);
  }
}

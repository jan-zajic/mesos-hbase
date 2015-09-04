package org.apache.mesos.hbase.util;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hbase.scheduler.HBaseScheduler;

import java.util.TimerTask;

/**
 * Used for a NameNode init timer to see if DNS is complete.
 */
public class PreNNInitTask extends TimerTask {

  private final DnsResolver dnsResolver;
  private final HBaseScheduler scheduler;
  private final SchedulerDriver driver;
  private final Protos.TaskID taskId;
  private final Protos.SlaveID slaveID;
  private final String message;

  public PreNNInitTask(DnsResolver dnsResolver, HBaseScheduler scheduler, SchedulerDriver driver,
      Protos.TaskID taskId,
      Protos.SlaveID slaveID, String message) {
    this.dnsResolver = dnsResolver;
    this.scheduler = scheduler;
    this.driver = driver;
    this.taskId = taskId;
    this.slaveID = slaveID;
    this.message = message;
  }

  @Override
  public void run() {
    if (dnsResolver.masterNodesResolvable()) {
      scheduler.sendMessageTo(driver, taskId, slaveID, message);
      this.cancel();
    }
  }
}

package org.apache.mesos.hbase.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.mesos.hbase.util.HBaseConstants;

/**
 * Manages the "Live" state of running tasks.
 */
@Singleton
public class LiveState {
  private final Log log = LogFactory.getLog(LiveState.class);

  private Set<Protos.TaskID> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.RECONCILING_TASKS;
  // TODO (nicgrayson) Might need to split this out to jns, nns, and dns if dns too big
  //TODO (elingg) we need to also track ZKFC's state
  private Map<String, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  
  public void addStagingTask(Protos.TaskID taskId) {
    stagingTasks.add(taskId);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    stagingTasks.remove(taskID);
  }

  public Map<String, Protos.TaskStatus> getRunningTasks() {
    return runningTasks;
  }

  public void removeRunningTask(Protos.TaskID taskId) {
    runningTasks.remove(taskId.getValue());
  }

  @SuppressWarnings("PMD")
  public void updateTaskForStatus(Protos.TaskStatus status) {
    runningTasks.put(status.getTaskId().getValue(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    this.currentAcquisitionPhase = phase;
  }
  
  public int getNameNodeSize() {
    return countOfRunningTasksWith(HBaseConstants.MASTER_NODE_TASKID);
  }

  private int countOfRunningTasksWith(final String nodeId) {
    return Sets.filter(runningTasks.keySet(), new Predicate<String>() {
      @Override
      public boolean apply(String taskId) {
        return taskId.contains(nodeId);
      }
    }).size();
  }
}

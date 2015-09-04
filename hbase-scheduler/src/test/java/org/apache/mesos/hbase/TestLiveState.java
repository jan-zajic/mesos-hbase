package org.apache.mesos.hbase;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.hbase.state.LiveState;
import org.apache.mesos.hbase.util.HBaseConstants;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestLiveState {

  private LiveState liveState;

  @Test
  public void addsAndRemovesStagingTasks() {
    liveState.addStagingTask(createTaskInfo("journalnode").getTaskId());
    assertEquals(1, liveState.getStagingTasksSize());
    liveState.removeStagingTask(createTaskInfo("journalnode").getTaskId());
    assertEquals(0, liveState.getStagingTasksSize());
  }

  @Before
  public void setup() {
    liveState = new LiveState();
  }

  private Protos.TaskStatus createTaskStatus(String taskId, Integer taskNumber, String message) {
    return Protos.TaskStatus.newBuilder()
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave." + taskNumber.toString()))
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskId + "." + taskNumber.toString()))
        .setState(Protos.TaskState.TASK_RUNNING)
        .setMessage(message)
        .build();
  }

  private Protos.TaskInfo createTaskInfo(String taskName) {
    return Protos.TaskInfo.newBuilder()
        .setName(taskName)
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskName + "." + "1"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave.1"))
        .setData(ByteString.copyFromUtf8(
            String.format("bin/hbase-mesos-%s", taskName)))
        .build();
  }
}

package org.apache.mesos.hbase.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hbase.config.HBaseFrameworkConfig;
import org.apache.mesos.hbase.file.FileUtils;
import org.apache.mesos.hbase.util.HBaseConstants;

import java.io.File;
import java.nio.charset.Charset;

/**
 * The executor for the Primary Master Node Machine.
 */
public class MasterNodeExecutor extends AbstractNodeExecutor {

  private final Log log = LogFactory.getLog(MasterNodeExecutor.class);

  private Task masterNodeTask;

  /**
   * The constructor for the primary name node which saves the configuration.
   */
  @Inject
  MasterNodeExecutor(HBaseFrameworkConfig hdfsFrameworkConfig) {
    super(hdfsFrameworkConfig);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(MasterNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order.
   * 1) Start Journal Node
   * 2) Receive Activate Message
   * 3) Start Name Node
   * 4) Start ZKFC Node
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(),
        task.getCmd()));
    if (taskInfo.getTaskId().getValue().contains(HBaseConstants.MASTER_NODE_TASKID)) {
      masterNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(masterNodeTask.getTaskInfo().getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(HBaseConstants.MASTER_NODE_TASKID)) {
      task = masterNodeTask;
    }

    if (task != null && task.getProcess() != null) {
      task.getProcess().destroy();
      task.setProcess(null);
    }
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_KILLED)
        .build());
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    log.info("Executor asked to shutdown");
    if (masterNodeTask != null) {
      killTask(d, masterNodeTask.getTaskInfo().getTaskId());
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    super.frameworkMessage(driver, msg);
    String messageStr = new String(msg, Charset.defaultCharset());
    File nameDir = new File(hdfsFrameworkConfig.getDataDir() + "/name");
    if (messageStr.equals(HBaseConstants.MASTER_NODE_INIT_MESSAGE)
        || messageStr.equals(HBaseConstants.MASTER_NODE_BOOTSTRAP_MESSAGE)) {
      if (nameDir.exists() && messageStr.equals(HBaseConstants.MASTER_NODE_INIT_MESSAGE)) {
        log.info(String
            .format("NameNode data directory %s already exists, not formatting",
                nameDir));
      } else {
        FileUtils.deleteDirectory(nameDir);
        if (!nameDir.mkdirs()) {
          final String errorMsg = "unable to make directory: " + nameDir;
          log.error(errorMsg);
          throw new ExecutorException(errorMsg);
        }
        runCommand(driver, masterNodeTask, "bin/hdfs-mesos-namenode " + messageStr);
      }
      startProcess(driver, masterNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(masterNodeTask.getTaskInfo().getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setMessage(messageStr)
          .build());
    }
  }
}

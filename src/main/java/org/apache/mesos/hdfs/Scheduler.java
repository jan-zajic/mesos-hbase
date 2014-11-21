package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.state.State;
import org.apache.mesos.hdfs.util.ResourceRoles;
import org.apache.mesos.hdfs.util.ResourceUtils;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.state.ZooKeeperState;
import org.joda.time.DateTime;
import org.joda.time.Seconds;


public class Scheduler implements org.apache.mesos.Scheduler, Runnable {
  public static final Log log = LogFactory.getLog(Scheduler.class);
  private final SchedulerConf conf;
  private final String localhost;
  private final ResourceUtils resourceUtils = new ResourceUtils(this);
  private Map<OfferID, Offer> pendingOffers;
  private boolean frameworkInitialized = false;
  private boolean initializingCluster = false;
  private Set<TaskID> stagingTasks = new HashSet<>();
  //TODO(elingg) simplify number of variables used
  private boolean firstNameNodeLaunched = false;
  private int nameNodesInitialized = 0;

  @Inject
  public Scheduler(SchedulerConf conf) {
    this.conf = conf;
    initClusterState();
    pendingOffers = new ConcurrentHashMap<>();

    try {
      localhost = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
    
  private void initClusterState() {
    MesosNativeLibrary.load(conf.getNativeLibrary());
    ZooKeeperState zkState = new ZooKeeperState(conf.getStateZkServers(),
        conf.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/" + conf.getClusterName());
    State state = new State(zkState);
    ClusterState clusterState = ClusterState.getInstance();
    clusterState.init(state);
  }

  public SchedulerConf getConf() {
    return conf;
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " status=" + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    try {
      ClusterState clusterState = ClusterState.getInstance();
      clusterState.getState().setFrameworkId(frameworkId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
    reregistered(driver, masterInfo);
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework.");
    ClusterState clusterState = ClusterState.getInstance();
    clusterState.clear();
  }

  private void launchNode(SchedulerDriver driver, Offer offer, ResourceRoles roles,
      String nodeName, List<String> taskNames, String executorName) {
    ClusterState clusterState = ClusterState.getInstance();
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
        taskNames.toString()));
    int confServerPort = conf.getConfigServerPort();
    // TODO(elingg)  Make sure the machine is only being used for one thing by assigning the host
    // for one purpose.  @See https://github.com/mesosphere/hdfs/issues/11
    String taskIdName = String.format("%s.%d", nodeName, System.currentTimeMillis());
    ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
        .setName(nodeName + " executor")
        .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
        .addAllResources(
            Arrays.asList(
                Resource.newBuilder()
                    .setName("cpus")
                    .setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder()
                        .setValue(conf.getExecutorCpus()).build())
                    .setRole(roles.cpuRole)
                    .build(),
                Resource.newBuilder()
                    .setName("mem")
                    .setType(Value.Type.SCALAR)
                    .setScalar(
                        Value.Scalar.newBuilder()
                            .setValue(conf.getExecutorHeap() * conf.getJvmOverhead()).build())
                    .setRole(roles.memRole)
                    .build(),
                Resource.newBuilder()
                    .setType(Value.Type.RANGES)
                    .setName("ports")
                    .setRole(roles.portsRole)
                    .setRanges(
                        Value.Ranges.newBuilder()
                            .addRange(Value.Range.newBuilder()
                                .setBegin(roles.portsBegin)
                                .setEnd(roles.portsEnd)))
                    .build()))
        .setCommand(CommandInfo.newBuilder()
            .addAllUris(Arrays.asList(
                CommandInfo.URI.newBuilder().setValue(conf.getExecUri())
                    .build(),
                CommandInfo.URI.newBuilder().setValue(
                    String.format("http://%s:%d/hdfs-site.xml", localhost, confServerPort))
                    .build()))
            .setEnvironment(Environment.newBuilder()
                .addAllVariables(Arrays.asList(
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_OPTS")
                        .setValue(conf.getJvmOpts()).build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_HEAPSIZE")
                        .setValue(String.format("%d", conf.getHadoopHeapSize())).build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_NAMENODE_OPTS")
                        .setValue("-Xmx" + conf.getNamenodeHeapSize() + "m").build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_DATANODE_OPTS")
                        .setValue("-Xmx" + conf.getDatanodeHeapSize() + "m").build(),
                    Environment.Variable.newBuilder()
                        .setName("EXECUTOR_OPTS")
                        .setValue("-Xmx" + conf.getExecutorHeap() + "m").build())))
            .setValue("env ; cd hadoop-2.* && exec java $HADOOP_OPTS $EXECUTOR_OPTS " +
                      "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName)
            .build())
        .build();

    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskName : taskNames) {
      List<Resource> resources = resourceUtils.buildResources(roles, conf.getTaskCpus(taskName),
          conf.getTaskHeapSize(taskName));

      TaskID taskId = TaskID.newBuilder()
          .setValue(String.format("task.%s.%s", taskName, taskIdName))
          .build();
      TaskInfo task = TaskInfo.newBuilder()
          .setExecutor(executorInfo)
          .setName(taskName)
          .setTaskId(taskId)
          .setSlaveId(offer.getSlaveId())
          .addAllResources(resources)
          .setData(ByteString.copyFromUtf8(
              String.format("bin/hdfs-mesos-%s", taskName)))
          .build();
      tasks.add(task);

      stagingTasks.add(taskId);
      clusterState.addTask(taskId,
          new DfsTask(taskName, offer.getSlaveId().getValue(), offer.getHostname()));
    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
  }

  private void launchNamenode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    if (!firstNameNodeLaunched) {
      launchNode(driver, offer, roles, "namenode",
          Arrays.asList("namenode", "zkfc", "journalnode"), "PrimaryNameNodeExecutor");
      firstNameNodeLaunched = true;
    } else {
      launchNode(driver, offer, roles, "namenode",
          Arrays.asList("namenode", "zkfc", "journalnode"), "SecondaryNameNodeExecutor");
    }
  }

  private void launchJournalnode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    launchNode(driver, offer, roles, "journalnode", Arrays.asList("journalnode"), "NodeExecutor");
  }

  private void launchDatanode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    launchNode(driver, offer, roles, "datanode", Arrays.asList("datanode"), "NodeExecutor");
  }

  private void launchInitialJournalnodes(SchedulerDriver driver, Collection<Offer> offers) {
    int journalnodes = 0;
    for (Offer offer : offers) {
      pendingOffers.remove(offer.getId());
      launchJournalnode(driver, offer, resourceUtils.sufficientRolesForJournalnode(offer));
      journalnodes++;
      if (journalnodes >= conf.getJournalnodeCount()) {
        return;
      }
    }
  }

  private void launchInitialNamenodes(SchedulerDriver driver, Collection<Offer> offers) {
    int namenodes = 0;
    // Find offer that matches
    for (Offer offer : offers) {
      ResourceRoles roles = resourceUtils.sufficientRolesForNamenode(offer);
      if (roles != null) {
        pendingOffers.remove(offer.getId());
        launchNamenode(driver, offer, roles);
        namenodes++;
        if (namenodes >= 2) {
          return;
        }
      }
    }
  }

  @Override
  synchronized public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    //TODO(elingg) all data nodes can be launched together after the other nodes have initialized.
    //Remove this waiting period for datanodes.
    log.info(String.format("Received %d offers", offers.size()));
    if (!stagingTasks.isEmpty()) {
      log.info("Declining offers because tasks are currently staging");
      for (Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    ClusterState clusterState = ClusterState.getInstance();
    if (clusterState.getNamenodes().size() == 0 && clusterState.getJournalnodes().size() == 0) {
      // Cluster must be formatted! Looks like we're starting fresh?
      log.info("No namenodes or journalnodes found.  Collecting offers until we have sufficient"
          + "capacity to launch.");

      int namenodes = 0;
      int journalnodes = 0;
      List<Offer> incomingOffers = new ArrayList<>();
      incomingOffers.addAll(offers);
      incomingOffers.addAll(pendingOffers.values());
      for (Offer offer : incomingOffers) {
        if (namenodes < 2 && resourceUtils.sufficientRolesForNamenode(offer) != null
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          namenodes++;
          pendingOffers.put(offer.getId(), offer);
        } else if (journalnodes < conf.getJournalnodeCount()
            && resourceUtils.sufficientRolesForJournalnode(offer) != null
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          journalnodes++;
          pendingOffers.put(offer.getId(), offer);
        } else {
          driver.declineOffer(offer.getId());
        }
      }
      log.info(String.format(
          "Currently have %d pending offers, with journalnodes=%d and namenodes=%d",
          pendingOffers.size(), journalnodes, namenodes));
      if (!initializingCluster && namenodes == 2 && journalnodes >= conf.getJournalnodeCount()) {
        log.info("Launching initial nodes with pending offers");
        initializingCluster = true;
        launchInitialNamenodes(driver, pendingOffers.values());
        launchInitialJournalnodes(driver, pendingOffers.values());
        // Decline any remaining offer
        for (OfferID offerID : pendingOffers.keySet()) {
          driver.declineOffer(offerID);
        }
        pendingOffers.clear();
      }
      return;
    }

    List<Offer> remainingOffers = new ArrayList<>();
    remainingOffers.addAll(offers);

    if (initializingCluster) {
      log.info(String.format("Declining remaining %d offers pending initialization",
          remainingOffers.size()));
      for (Offer offer : remainingOffers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    offers = remainingOffers;
    remainingOffers = new ArrayList<>();

    // Check to see if we can launch some datanodes
    for (Offer offer : offers) {
      ResourceRoles roles = resourceUtils.sufficientRolesForDatanode(offer);
      if (roles != null && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
        launchDatanode(driver, offer, roles);
      } else {
        remainingOffers.add(offer);
      }
    }

    // Decline remaining offers
    log.info(String.format("Declining %d offers", remainingOffers.size()));
    for (Offer offer : remainingOffers) {
      driver.declineOffer(offer.getId());
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId=" + slaveId.getValue());
  }


  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    ClusterState clusterState = ClusterState.getInstance();
    log.info(String.format(
        "Received status update for taskId=%s state=%s message='%s' stagingTasks.size=%d",
            status.getTaskId().getValue(),
            status.getState().toString(),
            status.getMessage(),
            stagingTasks.size()));

    DfsTask dfsTask = clusterState.getDfsTask(status.getTaskId());

    if (status.getState().equals(TaskState.TASK_FAILED)
        || status.getState().equals(TaskState.TASK_FINISHED)
        || status.getState().equals(TaskState.TASK_KILLED)
        || status.getState().equals(TaskState.TASK_LOST)) {
      stagingTasks.remove(status.getTaskId());
      clusterState.removeTask(status);
    } else if (status.getState().equals(TaskState.TASK_RUNNING)) {
      stagingTasks.remove(status.getTaskId());
      clusterState.updateTask(status);
      //TODO(elingg) get rid of this extra variable and also DFS task object.  Instead use cluster
      // state or a cleaner way of keeping track of what is running.
      // Get rid of this nameNodesInitialized variable.
      if (dfsTask.type == DfsTask.Type.NN) {
        nameNodesInitialized++;
        if (nameNodesInitialized == 2) {
          initializingCluster = false;
        }
      }
    }
  }

  @Override
  public void run() {
    ClusterState clusterState = ClusterState.getInstance();
    FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
        .setName("HDFS " + conf.getClusterName())
        .setFailoverTimeout(conf.getFailoverTimeout())
        .setUser(conf.getHdfsUser())
        .setRole(conf.getHdfsRole())
        .setCheckpoint(true);

    try {
      FrameworkID frameworkID = clusterState.getState().getFrameworkID();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
        frameworkInitialized = true;
      }
    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
        conf.getMesosMasterUri());
    driver.run().getValueDescriptor().getFullName();
  }

  public static class DfsTask {
    public final Type type;
    public TaskStatus taskStatus;
    public String slaveId;
    public String hostname;

    public DfsTask(String type, String slaveId, String hostname) {
      switch (type) {
        case "namenode" :
          this.type = Type.NN;
          break;
        case "journalnode" :
          this.type = Type.JN;
          break;
        case "datanode" :
          this.type = Type.DN;
          break;
        case "zkfc" :
          this.type = Type.ZKFC;
          break;
        default :
          throw new RuntimeException("Invalid type: " + type);
      }
      this.slaveId = slaveId;

      // Make sure this is the actual hostname.
      try {
        InetAddress addr = InetAddress.getByName(hostname);
        this.hostname = addr.getHostAddress();
      } catch (UnknownHostException e) {
        log.error(e);
        this.hostname = hostname;
      }
    }

    public enum Type {
      NN,
      DN,
      JN,
      ZKFC,
    }
  }

}
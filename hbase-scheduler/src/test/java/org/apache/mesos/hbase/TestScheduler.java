package org.apache.mesos.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hbase.scheduler.HBaseScheduler;
import org.apache.mesos.hbase.state.AcquisitionPhase;
import org.apache.mesos.hbase.state.LiveState;
import org.apache.mesos.hbase.state.IPersistentStateStore;
import org.apache.mesos.hbase.util.DnsResolver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.mesos.hbase.config.HBaseFrameworkConfig;
import org.apache.mesos.hbase.util.HBaseConstants;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class TestScheduler {

  private final HBaseFrameworkConfig hdfsFrameworkConfig = new HBaseFrameworkConfig(
      new Configuration());

  @Mock
  SchedulerDriver driver;

  @Mock
  IPersistentStateStore persistenceStore;

  @Mock
  LiveState liveState;

  @Mock
  DnsResolver dnsResolver;

  @Captor
  ArgumentCaptor<Collection<Protos.TaskInfo>> taskInfosCapture;

  HBaseScheduler scheduler;

  @Test
  public void statusUpdateWasStagingNowRunning() {
    Protos.TaskID taskId = createTaskId("1");

    scheduler.statusUpdate(driver, createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).removeStagingTask(taskId);
  }

  @Test
  public void statusUpdateTransitionFromAcquiringJournalNodesToStartingNameNodes() {
    Protos.TaskID taskId = createTaskId("1");

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.START_MASTER_NODES);
  }

  @Test
  public void statusUpdateAcquiringJournalNodesNotEnoughYet() {
    Protos.TaskID taskId = createTaskId("1");

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState, never()).transitionTo(AcquisitionPhase.START_MASTER_NODES);
  }

  @Test
  public void statusUpdateTransitionFromStartingNameNodesToFormateNameNodes() {
    Protos.TaskID taskId = createTaskId(HBaseConstants.MASTER_NODE_TASKID + "1");
    Protos.SlaveID slaveId = createSlaveId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.START_MASTER_NODES);
    when(liveState.getMasterNodeSize()).thenReturn(2);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.SLAVE_NODES);
  }

  @Test
  public void statusUpdateTransitionFromFormatNameNodesToDataNodes() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);
    when(liveState.getMasterNodeSize()).thenReturn(HBaseConstants.TOTAL_MASTER_NODES);

    scheduler.statusUpdate(
        driver,
        createTaskStatus(createTaskId(HBaseConstants.MASTER_NODE_TASKID),
            Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.SLAVE_NODES);
  }

  @Test
  public void statusUpdateAquiringDataNodesJustStays() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState, never()).transitionTo(any(AcquisitionPhase.class));
  }

  @Test
  public void launchesMasterNodeWhenInMasternode1Phase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.START_MASTER_NODES);
    when(persistenceStore.getPrimaryNodeTaskNames()).thenReturn(new HashMap<String, String>());

    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    assertTrue(taskInfosCapture.getValue().size() == 1);
    Iterator<Protos.TaskInfo> taskInfoIterator = taskInfosCapture.getValue().iterator();
    String firstTask = taskInfoIterator.next().getName();
  }

  @Test
  public void declinesAnyOffersPastWhatItNeeds() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0),
            createTestOffer(1),
            createTestOffer(2),
            createTestOffer(3)
            ));

    verify(driver, times(3)).declineOffer(any(Protos.OfferID.class));
  }

  @Test
  public void launchesDataNodesWhenInDatanodesPhase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0)
            )
        );

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Protos.TaskInfo taskInfo = taskInfosCapture.getValue().iterator().next();
    assertTrue(taskInfo.getName().contains(HBaseConstants.SLAVE_NODE_ID));
  }

  @Test
  public void removesTerminalTasksFromLiveState() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);

    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("0"),
        Protos.TaskState.TASK_FAILED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("1"),
        Protos.TaskState.TASK_FINISHED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("2"),
        Protos.TaskState.TASK_KILLED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("3"),
        Protos.TaskState.TASK_LOST));

    verify(liveState, times(4)).removeStagingTask(any(Protos.TaskID.class));
    verify(liveState, times(4)).removeRunningTask(any(Protos.TaskID.class));
  }

  @Test
  public void declinesOffersWithNotEnoughResources() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.SLAVE_NODES);
    Protos.Offer offer = createTestOfferWithResources(0, 0.1, 64);

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));

    verify(driver, times(1)).declineOffer(offer.getId());
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.scheduler = new HBaseScheduler(hdfsFrameworkConfig, liveState, persistenceStore);
  }

  private Protos.TaskID createTaskId(String id) {
    return Protos.TaskID.newBuilder().setValue(id).build();
  }

  private Protos.OfferID createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build();
  }

  private Protos.SlaveID createSlaveId(String slaveId) {
    return Protos.SlaveID.newBuilder().setValue(slaveId).build();
  }

  private Protos.ExecutorID createExecutorId(String executorId) {
    return Protos.ExecutorID.newBuilder().setValue(executorId).build();
  }

  private Protos.Offer createTestOffer(int instanceNumber) {
    return Protos.Offer.newBuilder()
        .setId(createTestOfferId(instanceNumber))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave" + instanceNumber).build())
        .setHostname("host" + instanceNumber)
        .build();
  }

  private Protos.Offer createTestOfferWithResources(int instanceNumber, double cpus, int mem) {
    return Protos.Offer.newBuilder()
        .setId(createTestOfferId(instanceNumber))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave" + instanceNumber).build())
        .setHostname("host" + instanceNumber)
        .addAllResources(Arrays.asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(cpus).build())
                .setRole("*")
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(mem).build())
                .setRole("*")
                .build()))
        .build();
  }

  private Protos.TaskStatus createTaskStatus(Protos.TaskID taskID, Protos.TaskState state) {
    return Protos.TaskStatus.newBuilder()
        .setTaskId(taskID)
        .setState(state)
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
        .setMessage("From Test")
        .build();
  }
}

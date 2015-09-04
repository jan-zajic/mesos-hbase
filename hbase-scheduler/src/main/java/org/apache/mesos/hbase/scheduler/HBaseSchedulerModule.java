package org.apache.mesos.hbase.scheduler;

import com.google.inject.AbstractModule;
import org.apache.mesos.hbase.state.IPersistentStateStore;
import org.apache.mesos.hbase.state.PersistentStateStore;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class HBaseSchedulerModule extends AbstractModule 
{

  @Override
  protected void configure() 
  {
    bind(IPersistentStateStore.class).to(PersistentStateStore.class);
  }
  
}

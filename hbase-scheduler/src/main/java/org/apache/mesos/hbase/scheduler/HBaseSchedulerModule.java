package org.apache.mesos.hbase.scheduler;

import com.google.inject.AbstractModule;
import org.apache.mesos.hbase.state.DeadNodeTracker;
import org.apache.mesos.hbase.state.HBaseDevelopmentStore;
import org.apache.mesos.hbase.state.HBaseZkStore;
import org.apache.mesos.hbase.state.IHBaseStore;
import org.apache.mesos.hbase.state.IPersistentStateStore;
import org.apache.mesos.hbase.state.PersistentStateStore;
import org.apache.mesos.hbase.util.HBaseConstants;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class HBaseSchedulerModule extends AbstractModule
{
  
    @Override
    protected void configure()
    {
         bind(DeadNodeTracker.class);
         if(HBaseConstants.isDevelopmentMode())
             bind(IHBaseStore.class).to(HBaseDevelopmentStore.class);              
         else
             bind(IHBaseStore.class).to(HBaseZkStore.class);

         bind(IPersistentStateStore.class).to(PersistentStateStore.class);  
    }
  
}

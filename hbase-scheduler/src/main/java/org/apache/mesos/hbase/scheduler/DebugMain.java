package org.apache.mesos.hbase.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hbase.config.ConfigServer;
import org.apache.mesos.hbase.util.HBaseConstants;

/**
 *
 * @author jzajic
 */
public class DebugMain {

  public static void main(String[] args)
  {
    System.setProperty(HBaseConstants.DEVELOPMENT_MODE_PROPERTY, Boolean.TRUE.toString());
    Injector injector = Guice.createInjector(new HBaseSchedulerModule());
    injector.getInstance(ConfigServer.class);
  }

}

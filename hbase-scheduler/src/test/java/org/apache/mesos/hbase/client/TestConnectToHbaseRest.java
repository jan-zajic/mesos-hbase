package org.apache.mesos.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.model.VersionModel;

/**
 *
 * @author jzajic
 */
public class TestConnectToHbaseRest {
  
  public static void main(String[] args) throws Exception
  {
      Cluster cluster = new Cluster();
      for(String arg : args)
        cluster.add(arg, 8088);
      
      Client client = new Client(cluster);
      RemoteAdmin remoteAdmin = new RemoteAdmin(client, new Configuration());   
      VersionModel restVersion = remoteAdmin.getRestVersion();
      System.out.println(restVersion.toString());
  }
  
}

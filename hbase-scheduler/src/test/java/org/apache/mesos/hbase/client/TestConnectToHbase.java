package org.apache.mesos.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author jzajic
 */
public class TestConnectToHbase
{

  public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

  public static void main(String[] args) throws Exception
    {
        String hbaseZookeeperQuorum="127.0.0.1,192.168.0.1";
        int hbaseZookeeperClientPort=2181;
        // You need a configuration object to tell the client where to connect.
        // When you create a HBaseConfiguration, it reads in whatever you've set
        // into your hbase-site.xml and in hbase-default.xml, as long as these can
        // be found on the CLASSPATH
        Configuration hConf = HBaseConfiguration.create();
        hConf.set(TestConnectToHbase.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
        hConf.set(TestConnectToHbase.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, String.valueOf(hbaseZookeeperClientPort));
        hConf.set("hbase.cluster.distributed", "true");
        hConf.set("hbase.rootdir", "hdfs://hdfs/hbase");
        //timeout settings - see http://hadoop-hbase.blogspot.cz/2012/09/hbase-client-timeouts.html
        String smallTimeout = "200";
        hConf.set("zookeeper.session.timeout", smallTimeout);
        hConf.set("hbase.rpc.timeout", smallTimeout);
        hConf.set("zookeeper.recovery.retry", "2");
        hConf.set("zookeeper.recovery.retry.intervalmill", "10");
        hConf.set("hbase.client.retries.number", "2");
        hConf.set("hbase.client.pause", "10");
        hConf.set("hbase.client.operation.timeout", "100");
        
        try (Connection connection = ConnectionFactory.createConnection(hConf);) 
        {
            if(!connection.isAborted())
            {
                try(Table table = connection.getTable(TableName.valueOf("myLittleHBaseTable"));)
                {
                    Put p = new Put(Bytes.toBytes("myLittleRow"));
                    p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
                    table.put(p); 
                }
            }
        }
    }
}

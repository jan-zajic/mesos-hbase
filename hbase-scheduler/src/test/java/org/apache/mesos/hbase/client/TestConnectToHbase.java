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
public class TestConnectToHbase {

  public static void main(String[] args) throws Exception
  {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH
    Configuration config = HBaseConfiguration.create();
    config.set("", null);    
    try(Connection connection = ConnectionFactory.createConnection(config);)
    {
        Table table = connection.getTable(TableName.valueOf("myLittleHBaseTable"));
        Put p = new Put(Bytes.toBytes("myLittleRow"));
        p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
        table.put(p);
    }
  }

}

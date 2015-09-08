package org.apache.mesos.hbase;

import com.floreysoft.jmte.Engine;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 *
 * @author jzajic
 */
public class TestEngine {

  @Test
    public void testReplacement()
    {
    String template = "<?xml version=\"1.0\"?>\n" +
        "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
        "<configuration>\n" +
        "  <property>\n" +
        "    <name>hbase.master</name>\n" +
        "    <value>${primary1Hostname}:60000</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>hbase.rootdir</name>\n" +
        "    <value>${hbaseRootDir}</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>hbase.cluster.distributed</name>\n" +
        "    <value>true</value>\n" +
        "  </property>\n" +
        "  <property>\n" +
        "    <name>hbase.zookeeper.quorum</name>\n" +
        "    <value>${haZookeeperQuorum}</value>\n" +
        "  </property>\n" +
        "</configuration>\n" +
        "";
        Engine engine = new Engine();
        Map<String, Object> model = new HashMap<>();
        model.put("hbaseRootDir", "testdir");      
        String content = engine.transform(template, model);
        System.out.println(content);
        assert content.contains("testdir");
    }
}

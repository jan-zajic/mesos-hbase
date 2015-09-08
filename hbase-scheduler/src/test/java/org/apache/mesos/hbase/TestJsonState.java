package org.apache.mesos.hbase;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.mesos.hbase.util.HBaseConstants;
import org.apache.mesos.hbase.util.HdfsConfFileUrlJsonFinder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

/**
 *
 * @author jzajic
 */
public class TestJsonState {

  @Test
  public void testIt() throws Exception
  {
    URL jsonURL = TestJsonState.class.getResource("/state.json");
    ObjectMapper mapper = new ObjectMapper();
    HdfsConfFileUrlJsonFinder finder = new HdfsConfFileUrlJsonFinder(mapper);
    String findedUrl = finder.findUrl(jsonURL);
    System.out.println(findedUrl);
    assert findedUrl.equals("http://192.168.1.170:31564/hdfs-site.xml");
  }

}

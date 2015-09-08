
package org.apache.mesos.hbase.util;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author jzajic
 */
public class HdfsConfFileUrlJsonFinder {

    private ObjectMapper mapper;

    public HdfsConfFileUrlJsonFinder(ObjectMapper mapper)
    {
        this.mapper = mapper;
    }
    
    public String findUrl(URL jsonURL) throws IOException
    {
        JsonNode rootNode = mapper.readTree(jsonURL);
        Iterator<JsonNode> frameworkIterator = rootNode.path("frameworks").iterator();
        for (Iterator<JsonNode> iterator = frameworkIterator; iterator.hasNext();) {
            JsonNode framework = iterator.next();
            
            JsonNode nameNode = framework.path("name");
            if(nameNode.getTextValue().equals("hdfs"))
            {
                JsonNode uris = framework.findValue("uris");
                Iterator<JsonNode> urisIterator = uris.iterator();
                for (Iterator<JsonNode> iterator2 = urisIterator; iterator.hasNext();) {

                        String value = iterator2.next().findValue("value").getTextValue();
                        if(value.contains(HBaseConstants.HDFS_CONFIG_FILE_NAME))
                            return value;
                };
            }
        }     
        return null;
    }
    
}

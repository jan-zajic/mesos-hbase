
package org.apache.mesos.hbase.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.Charsets;

/**
 *
 * @author jzajic
 */
public class HBaseDevelopmentStore implements IHBaseStore
{

    private Map<String,Object> values = new HashMap<>();
    
    @Override
    public byte[] getRawValueForId(String id) throws ExecutionException, InterruptedException
    {
        byte[] value = (byte[]) values.get(id);
        if(value == null)
            return new byte[]{};
        else
            return value;
    }
    
    @Override
    public void setRawValueForId(String id, byte[] frameworkId) throws ExecutionException, InterruptedException
    {
        values.put(id, frameworkId);
    }
    
    @Override
    public <T> T get(String key) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException
    {
        return (T) values.get(key);
    }

    @Override
    public <T> void set(String key, T object) throws InterruptedException, ExecutionException, IOException
    {
        values.put(key, object);
    }
    
}

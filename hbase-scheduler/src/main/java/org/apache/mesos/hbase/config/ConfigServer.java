package org.apache.mesos.hbase.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hbase.state.IPersistentStateStore;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.mesos.hbase.util.HBaseConstants;

/**
 * This is the HTTP service which allows executors to fetch the configuration for hbase-site.xml.
 */
public class ConfigServer {

  private final Log log = LogFactory.getLog(ConfigServer.class);

  private Server server;
  private Engine engine;
  private HBaseFrameworkConfig hdfsFrameworkConfig;
  private IPersistentStateStore persistenceStore;

  @Inject
  public ConfigServer(HBaseFrameworkConfig hdfsFrameworkConfig,
      IPersistentStateStore persistenceStore) {
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
    this.persistenceStore = persistenceStore;
    engine = new Engine();
    server = new Server(hdfsFrameworkConfig.getConfigServerPort());
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(hdfsFrameworkConfig.getExecutorPath());
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{
        resourceHandler, new ServeHdfsConfigHandler()});
    server.setHandler(handlers);

    try {
      server.start();

    } catch (Exception e) {
      final String msg = "Unable to start jetty server";
      log.error(msg, e);
      throw new ConfigServerException(msg, e);
    }
  }

  public void stop() throws ConfigServerException {
    try {
      server.stop();
    } catch (Exception e) {
      final String msg = "Unable to stop the jetty service";
      log.error(msg, e);
      throw new ConfigServerException(msg, e);
    }
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {

    public synchronized void handle(String target, Request baseRequest, HttpServletRequest request,
      HttpServletResponse response) throws IOException {

      File confFile = new File(hdfsFrameworkConfig.getConfigPath());

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
          + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())), Charset.defaultCharset());

      Set<String> primaryNodes = new TreeSet<>();
      primaryNodes.addAll(persistenceStore.getPrimaryNodes().keySet());
      
      Map<String, Object> model = new HashMap<>();
      Iterator<String> iter = primaryNodes.iterator();
      
      if (iter.hasNext()) {
        model.put("primary1Hostname", iter.next());
      }
      
      if (iter.hasNext()) {
        model.put("primary2Hostname", iter.next());
      }
      
      model.put("hbase.rootdir", getHbaseRootDir());      
      
      model.put("frameworkName", hdfsFrameworkConfig.getFrameworkName());
      model.put("dataDir", hdfsFrameworkConfig.getDataDir());
      model.put("haZookeeperQuorum", hdfsFrameworkConfig.getHaZookeeperQuorum());

      content = engine.transform(content, model);

      response.setContentType("application/octet-stream;charset=utf-8");
      response.setHeader("Content-Disposition", "attachment; filename=\"" +
        HBaseConstants.HBASE_CONFIG_FILE_NAME + "\" ");
      response.setHeader("Content-Transfer-Encoding", "binary");
      response.setHeader("Content-Length", Integer.toString(content.length()));

      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().println(content);
    }

    private String getHbaseRootDir()
    {
        if(hdfsFrameworkConfig.usingMesosHdfs())
        {
            return "hdfs://"+hdfsFrameworkConfig.getDfsNameServices()+"/hbase";
        } else {
            return hdfsFrameworkConfig.getHbaseRootDir();
        }
    }
  }
}

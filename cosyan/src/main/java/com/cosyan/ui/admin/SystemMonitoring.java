package com.cosyan.ui.admin;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.SessionHandler.NoSessionExpression;

public class SystemMonitoring {

  private final SessionHandler sessionHandler;

  public SystemMonitoring(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  public JSONObject usage(String userToken) throws IOException, NoSessionExpression, AuthException {
    Session session = sessionHandler.session(userToken);
    MetaRepo metaRepo = session.metaRepo();
    JSONObject obj = new JSONObject();
    OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    obj.put("load", operatingSystemMXBean.getSystemLoadAverage());
    obj.put("freeMemory", Runtime.getRuntime().freeMemory());
    obj.put("totalMemory", Runtime.getRuntime().totalMemory());
    obj.put("maxMemory", Runtime.getRuntime().maxMemory());
    JSONArray uniqueIndexes = new JSONArray();
    for (Entry<String, ByteTrieStat> entry : metaRepo.uniqueIndexStats().entrySet()) {
      JSONObject index = new JSONObject();
      index.put("name", entry.getKey());
      index.put("indexFileSize", entry.getValue().getIndexFileSize());
      index.put("inMemNodes", entry.getValue().getInMemNodes());
      index.put("pendingNodes", entry.getValue().getPendingNodes());
      uniqueIndexes.put(index);
    }
    obj.put("uniqueIndexes", uniqueIndexes);
    JSONArray multiIndexes = new JSONArray();
    for (Entry<String, ByteMultiTrieStat> entry : metaRepo.multiIndexStats().entrySet()) {
      JSONObject index = new JSONObject();
      index.put("name", entry.getKey());
      index.put("trieFileSize", entry.getValue().getTrieFileSize());
      index.put("indexFileSize", entry.getValue().getIndexFileSize());
      index.put("trieInMemNodes", entry.getValue().getTrieInMemNodes());
      index.put("triePendingNodes", entry.getValue().getTriePendingNodes());
      index.put("pendingNodes", entry.getValue().getPendingNodes());
      uniqueIndexes.put(index);
    }
    obj.put("multiIndexes", multiIndexes);
    return obj;
  }
}

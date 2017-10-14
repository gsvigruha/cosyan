package com.cosyan.ui.admin;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.meta.MetaRepo;

public class SystemMonitoring {

  private final MetaRepo metaRepo;

  public SystemMonitoring(MetaRepo metaRepo) {
    this.metaRepo = metaRepo;
  }

  @SuppressWarnings("unchecked")
  public JSONObject usage() throws IOException {
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
      uniqueIndexes.add(index);
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
      uniqueIndexes.add(index);
    }
    obj.put("multiIndexes", multiIndexes);
    return obj;
  }
}

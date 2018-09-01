/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.ui.admin;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.TableStat;
import com.cosyan.db.session.Session;
import com.cosyan.ui.ParamServlet;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.ParamServlet.Servlet;

@Servlet(path = "monitoring", doc = "Returns system monitoring info.")
public class MonitoringServlet extends ParamServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;

  public MonitoringServlet(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Param(name = "token", doc = "User authentication token.")
  @Override
  protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      String token = req.getParameter("token");
      Session session = sessionHandler.session(token);
      MetaRepo metaRepo = session.metaRepo();
      metaRepo.metaRepoReadLock();
      try {
        JSONObject obj = new JSONObject();
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        obj.put("load", operatingSystemMXBean.getSystemLoadAverage());
        obj.put("nproc", operatingSystemMXBean.getAvailableProcessors());
        obj.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
        obj.put("freeMemory", Runtime.getRuntime().freeMemory());
        obj.put("totalMemory", Runtime.getRuntime().totalMemory());
        obj.put("maxMemory", Runtime.getRuntime().maxMemory());
        {
          JSONArray tables = new JSONArray();
          for (Entry<String, TableStat> entry : metaRepo.tableStats().entrySet()) {
            JSONObject table = new JSONObject();
            table.put("name", entry.getKey());
            table.put("fileSize", entry.getValue().getFileSize());
            tables.put(table);
          }
          obj.put("tables", tables);
        }
        {
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
        }
        {
          JSONArray multiIndexes = new JSONArray();
          for (Entry<String, ByteMultiTrieStat> entry : metaRepo.multiIndexStats().entrySet()) {
            JSONObject index = new JSONObject();
            index.put("name", entry.getKey());
            index.put("trieFileSize", entry.getValue().getTrieFileSize());
            index.put("indexFileSize", entry.getValue().getIndexFileSize());
            index.put("trieInMemNodes", entry.getValue().getTrieInMemNodes());
            index.put("triePendingNodes", entry.getValue().getTriePendingNodes());
            index.put("pendingNodes", entry.getValue().getPendingNodes());
            multiIndexes.put(index);
          }
          obj.put("multiIndexes", multiIndexes);
        }
        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println(obj);
      } finally {
        metaRepo.metaRepoReadUnlock();
      }
    } catch (Exception e) {
      JSONObject error = new JSONObject();
      error.put("error", e.getMessage());
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      resp.getWriter().println(error);
    }
  }
}

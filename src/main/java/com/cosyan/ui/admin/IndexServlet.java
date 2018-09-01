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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.cosyan.ui.ParamServlet;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.ParamServlet.Servlet;

@Servlet(path = "index", doc = "Looks up a key in an index and returns the corresponding values.")
public class IndexServlet extends ParamServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;

  public IndexServlet(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Param(name = "token", doc = "User authentication token.")
  @Param(name = "session", doc = "Session ID.")
  @Param(name = "index", mandatory = true, doc = "Full index name.")
  @Param(name = "key", mandatory = true, doc = "The value in the index to check.")
  @Override
  protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    sessionHandler.execute(req, resp, (Session session) -> {
      String id = req.getParameter("index");
      String key = req.getParameter("key");
      try {
        MetaRepo metaRepo = session.metaRepo();
        metaRepo.metaRepoReadLock();
        long[] pointers;
        try {
          IndexReader index = metaRepo.getIndex(id);
          pointers = index.get(index.keyDataType().fromString(key));
        } finally {
          metaRepo.metaRepoReadUnlock();
        }
        JSONObject result = new JSONObject();
        result.put("pointers", pointers);
        resp.setStatus(HttpStatus.OK_200);
        return result;
      } catch (AuthException | RuleException | IOException e) {
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        return new ErrorResult(e).toJSON();
      }
    });
  }
}

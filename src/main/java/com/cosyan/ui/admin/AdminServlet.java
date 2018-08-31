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

import com.cosyan.ui.ParamServlet;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.ParamServlet.Servlet;

@Servlet(path = "admin", doc = "Returns the metadata for all tables.")
public class AdminServlet extends ParamServlet {
  private static final long serialVersionUID = 1L;

  private final MetaRepoConnector metaRepoConnector;

  public AdminServlet(SessionHandler sessionHandler) {
    this.metaRepoConnector = new MetaRepoConnector(sessionHandler);
  }

  @Param(name = "token", doc = "User authentication token.")
  @Override
  protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      JSONObject obj = new JSONObject();
      obj.put("tables", metaRepoConnector.tables(req.getParameter("token")));
      resp.setStatus(HttpStatus.OK_200);
      resp.getWriter().println(obj);
    } catch (Exception e) {
      e.printStackTrace();
      JSONObject error = new JSONObject();
      error.put("error", e.getMessage());
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      resp.getWriter().println(error);
    }
  }
}

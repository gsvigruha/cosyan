package com.cosyan.ui.admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;

public class IndexServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;

  public IndexServlet(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
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
        return result;
      } catch (AuthException | RuleException | IOException e) {
        return new ErrorResult(e).toJSON();
      }
    });
  }
}

package com.cosyan.ui.sql;

import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.session.Session;

public class SQLConnector {

  private final DBApi dbApi;
  private Session session;

  public SQLConnector(DBApi dbApi) {
    this.dbApi = dbApi;
    session = dbApi.adminSession();
  }

  public JSONObject run(String sql) {
    Result result = session.execute(sql);
    return result.toJSON();
  }
}

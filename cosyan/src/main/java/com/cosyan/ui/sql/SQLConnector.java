package com.cosyan.ui.sql;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.session.Session;
import com.cosyan.db.sql.Result;
import com.cosyan.db.sql.Result.CrashResult;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.Result.TransactionResult;
import com.google.common.collect.ImmutableList;

public class SQLConnector {

  private final DBApi dbApi;
  private Session session;

  public SQLConnector(DBApi dbApi) {
    this.dbApi = dbApi;
    session = dbApi.getSession();
  }

  @SuppressWarnings("unchecked")
  public JSONObject run(String sql) {
    Result result = session.execute(sql);
    JSONObject obj = new JSONObject();
    if (result.isSuccess()) {
      TransactionResult transactionResult = (TransactionResult) result;
      JSONArray list = new JSONArray();
      for (Result partialResult : transactionResult.getResults()) {
        JSONObject partialResultObj = new JSONObject();
        if (partialResult instanceof QueryResult) {
          QueryResult queryResult = (QueryResult) partialResult;
          partialResultObj.put("type", "query");
          partialResultObj.put("header", ImmutableList.copyOf(queryResult.getHeader()));
          List<ImmutableList<Object>> valuess = new ArrayList<>();
          for (Object[] values : queryResult.getValues()) {
            valuess.add(ImmutableList.copyOf(values));
          }
          partialResultObj.put("values", valuess);
        } else if (partialResult instanceof StatementResult) {
          StatementResult statementResult = (StatementResult) partialResult;
          partialResultObj.put("type", "statement");
          partialResultObj.put("lines", statementResult.getAffectedLines());
        }
        list.add(partialResultObj);
      }
      obj.put("result", list);
    } else {
      if (result instanceof ErrorResult) {
        obj.put("error", ((ErrorResult) result).getError().getMessage());
      } else if (result instanceof CrashResult) {
        obj.put("error", ((CrashResult) result).getError().getMessage());
      }
    }
    return obj;
  }
}

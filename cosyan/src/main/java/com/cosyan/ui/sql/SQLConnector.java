package com.cosyan.ui.sql;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.session.Session;
import com.cosyan.db.sql.Result;
import com.cosyan.db.sql.Result.CrashResult;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.MetaStatementResult;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.Result.TransactionResult;

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
      if (result instanceof TransactionResult) {
        TransactionResult transactionResult = (TransactionResult) result;
        JSONArray list = new JSONArray();
        for (Result partialResult : transactionResult.getResults()) {
          JSONObject partialResultObj = new JSONObject();
          if (partialResult instanceof QueryResult) {
            QueryResult queryResult = (QueryResult) partialResult;
            partialResultObj.put("type", "query");
            partialResultObj.put("header", queryResult.getHeader());
            partialResultObj.put("values", queryResult.getValues());
          } else if (partialResult instanceof StatementResult) {
            StatementResult statementResult = (StatementResult) partialResult;
            partialResultObj.put("type", "statement");
            partialResultObj.put("lines", statementResult.getAffectedLines());
          }
          list.add(partialResultObj);
        }
        obj.put("result", list);
      } else if (result instanceof MetaStatementResult) {
        JSONArray list = new JSONArray();
        JSONObject partialResultObj = new JSONObject();
        partialResultObj.put("type", "statement");
        partialResultObj.put("lines", "1");
        list.add(partialResultObj);
        obj.put("result", list);
      }
      
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

package com.cosyan.ui.admin;

import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.TableRef;
import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.SessionHandler.NoSessionExpression;

public class MetaRepoConnector {

  private final SessionHandler sessionHandler;

  public MetaRepoConnector(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  public JSONObject tables(String userToken) throws ModelException, NoSessionExpression, AuthException, ConfigException {
    Session session = sessionHandler.session(userToken);
    MetaRepo metaRepo = session.metaRepo();
    JSONObject list = new JSONObject();
    for (Map.Entry<String, MaterializedTable> table : metaRepo.getTables(session.authToken()).entrySet()) {
      JSONObject tableObj = new JSONObject();
      tableObj.put("name", table.getKey());
      MaterializedTable tableMeta = table.getValue();
      JSONArray columns = new JSONArray();
      for (BasicColumn column : tableMeta.columns().values()) {
        JSONObject columnObj = new JSONObject();
        columnObj.put("name", column.getName());
        columnObj.put("type", column.getType().getName());
        columnObj.put("nullable", column.isNullable());
        columnObj.put("unique", column.isUnique());
        columnObj.put("indexed", column.isIndexed());
        columnObj.put("immutable", column.isImmutable());
        columns.put(columnObj);
      }
      tableObj.put("columns", columns);

      if (tableMeta.primaryKey().isPresent()) {
        tableObj.put("primaryKeys", tableMeta.primaryKey().get().getName().getString());
      }

      JSONArray foreignKeys = new JSONArray();
      for (ForeignKey foreignKey : tableMeta.foreignKeys().values()) {
        JSONObject fkObj = new JSONObject();
        fkObj.put("name", foreignKey.getName());
        fkObj.put("column", foreignKey.getColumn().getName());
        fkObj.put("revName", foreignKey.getRevName());
        fkObj.put("refTable", foreignKey.getRefTable().tableName());
        fkObj.put("refColumn", foreignKey.getRefColumn().getName());
        foreignKeys.put(fkObj);
      }
      tableObj.put("foreignKeys", foreignKeys);

      JSONArray reverseForeignKeys = new JSONArray();
      for (ReverseForeignKey foreignKey : tableMeta.reverseForeignKeys().values()) {
        JSONObject fkObj = new JSONObject();
        fkObj.put("name", foreignKey.getName());
        fkObj.put("column", foreignKey.getColumn().getName());
        fkObj.put("refTable", foreignKey.getRefTable().tableName());
        fkObj.put("refColumn", foreignKey.getRefColumn().getName());
        reverseForeignKeys.put(fkObj);
      }
      tableObj.put("reverseForeignKeys", reverseForeignKeys);

      JSONArray aggrefs = new JSONArray();
      for (TableRef aggref : tableMeta.refs().values()) {
        JSONObject aggrefsObj = new JSONObject();
        aggrefsObj.put("name", aggref.getName());
        aggrefsObj.put("expr", aggref.getExpr());
        aggrefs.put(aggrefsObj);
      }
      tableObj.put("aggRefs", aggrefs);
      
      JSONArray rules = new JSONArray();
      for (BooleanRule rule : tableMeta.rules().values()) {
        JSONObject ruleObj = new JSONObject();
        ruleObj.put("name", rule.getName());
        ruleObj.put("expr", rule.getExpr().print());
        rules.put(ruleObj);
      }
      tableObj.put("rules", rules);
      
      list.put(tableMeta.tableName(), tableObj);
    }
    return list;
  }
}

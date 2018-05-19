package com.cosyan.ui.admin;

import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.session.Session;

public class MetaRepoConnector {

  private final MetaRepo metaRepo;
  private Session session;

  public MetaRepoConnector(DBApi dbApi) {
    this.metaRepo = dbApi.getMetaRepo();
    this.session = dbApi.adminSession();
  }

  public JSONArray tables() throws ModelException {
    JSONArray list = new JSONArray();
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

      list.put(tableObj);
    }
    return list;
  }
}

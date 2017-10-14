package com.cosyan.ui.admin;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;

public class MetaRepoConnector {

  private final MetaRepo metaRepo;

  public MetaRepoConnector(MetaRepo metaRepo) {
    this.metaRepo = metaRepo;
  }

  @SuppressWarnings("unchecked")
  public JSONObject tables() throws ModelException {
    JSONObject obj = new JSONObject();
    JSONArray list = new JSONArray();
    for (Map.Entry<String, MaterializedTableMeta> table : metaRepo.getTables().entrySet()) {
      JSONObject tableObj = new JSONObject();
      tableObj.put("name", table.getKey());
      MaterializedTableMeta tableMeta = table.getValue();
      JSONArray columns = new JSONArray();
      for (BasicColumn column : tableMeta.columns().values()) {
        JSONObject columnObj = new JSONObject();
        columnObj.put("name", column.getName());
        columnObj.put("type", column.getType().getName());
        columnObj.put("nullable", column.isNullable());
        columnObj.put("unique", column.isUnique());
        columnObj.put("indexed", column.isIndexed());
        columns.add(columnObj);
      }
      tableObj.put("columns", columns);

      if (tableMeta.primaryKey().isPresent()) {
        tableObj.put("primaryKeys", tableMeta.primaryKey().get().getName());
      }

      JSONArray foreignKeys = new JSONArray();
      for (ForeignKey foreignKey : tableMeta.foreignKeys().values()) {
        JSONObject fkObj = new JSONObject();
        fkObj.put("name", foreignKey.getName());
        fkObj.put("column", foreignKey.getColumn().getName());
        fkObj.put("refTable", foreignKey.getRefTable().tableName());
        fkObj.put("refColumn", foreignKey.getRefColumn().getName());
        foreignKeys.add(fkObj);
      }
      tableObj.put("foreignKeys", foreignKeys);

      JSONArray reverseForeignKeys = new JSONArray();
      for (ForeignKey foreignKey : tableMeta.reverseForeignKeys().values()) {
        JSONObject fkObj = new JSONObject();
        fkObj.put("name", foreignKey.getName());
        fkObj.put("column", foreignKey.getColumn().getName());
        fkObj.put("refTable", foreignKey.getRefTable().tableName());
        fkObj.put("refColumn", foreignKey.getRefColumn().getName());
        reverseForeignKeys.add(fkObj);
      }
      tableObj.put("reverseForeignKeys", reverseForeignKeys);

      list.add(tableObj);
    }
    obj.put("tables", list);

    return obj;
  }

  @SuppressWarnings("unchecked")
  public JSONObject indexes() throws ModelException {
    JSONObject obj = new JSONObject();
    obj.put("uniqueIndexes", metaRepo.uniqueIndexNames());
    obj.put("multiIndexes", metaRepo.multiIndexNames());
    return obj;
  }
}

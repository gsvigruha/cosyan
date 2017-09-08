package com.cosyan.ui.admin;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;

public class MetaRepoConnector {

  private final MetaRepo metaRepo;

  public MetaRepoConnector(MetaRepo metaRepo) {
    this.metaRepo = metaRepo;
  }

  @SuppressWarnings("unchecked")
  public JSONObject tables() {
    JSONObject obj = new JSONObject();
    JSONArray list = new JSONArray();
    for (Map.Entry<String, MaterializedTableMeta> table : metaRepo.getTables().entrySet()) {
      JSONObject tableObj = new JSONObject();
      tableObj.put("name", table.getKey());
      MaterializedTableMeta tableMeta = table.getValue();
      JSONArray columns = new JSONArray();
      for (BasicColumn column : tableMeta.getColumns().values()) {
        JSONObject columnObj = new JSONObject();
        columnObj.put("name", column.getName());
        columnObj.put("type", column.getType().getName());
        columnObj.put("nullable", column.isNullable());
        columnObj.put("unique", column.isUnique());
        columns.add(columnObj);
      }
      tableObj.put("columns", columns);
      list.add(tableObj);
    }
    obj.put("tables", list);
    return obj;
  }
}

package com.cosyan.db.model;

import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;

import lombok.Data;

@Data
public class TableRef {
  
  private final String name;
  private final Select expr;
  protected final transient ExposedTableMeta tableMeta;

}

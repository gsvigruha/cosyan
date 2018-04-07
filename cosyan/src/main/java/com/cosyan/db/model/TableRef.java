package com.cosyan.db.model;

import com.cosyan.db.model.References.RefTableMeta;

import lombok.Data;

@Data
public class TableRef {
  
  private final String name;
  protected final transient RefTableMeta tableMeta;

}

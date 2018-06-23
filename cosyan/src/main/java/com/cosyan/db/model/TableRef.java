package com.cosyan.db.model;

import com.cosyan.db.model.References.AggRefTableMeta;

import lombok.Data;

@Data
public class TableRef {
  
  private final String name;
  private final String expr;
  protected final transient AggRefTableMeta tableMeta;

}

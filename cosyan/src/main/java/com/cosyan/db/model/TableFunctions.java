package com.cosyan.db.model;

import com.cosyan.db.model.BuiltinFunctions.TableFunction;
import com.google.common.collect.ImmutableMap;

public class TableFunctions {

  public static class SelectFunction extends TableFunction {

    public SelectFunction(String ident) {
      super(ident);      
    }

    @Override
    public TableMeta call(TableMeta tableMeta, ImmutableMap<String, ColumnMeta> argValues) {
      //return new DerivedTableMeta(tableMeta, argValues);
      return null;
    }
  }
}

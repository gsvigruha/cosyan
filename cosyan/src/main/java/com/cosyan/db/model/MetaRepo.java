package com.cosyan.db.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.conf.Config;
import com.cosyan.db.model.BuiltinFunctions.AggrFunction;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class MetaRepo {

  private final Config config;
  private final ConcurrentHashMap<String, ExposedTableMeta> tables;
  private final ConcurrentHashMap<String, SimpleFunction<?>> simpleFunctions;
  private final ConcurrentHashMap<String, AggrFunction> aggrFunctions;

  public MetaRepo(Config config) {
    this.config = config;
    this.tables = new ConcurrentHashMap<>();
    this.simpleFunctions = new ConcurrentHashMap<>();
    for (SimpleFunction<?> simpleFunction : BuiltinFunctions.SIMPLE) {
      simpleFunctions.put(simpleFunction.getIdent(), simpleFunction);
    }
    this.aggrFunctions = new ConcurrentHashMap<>();
    for (AggrFunction aggrFunction : BuiltinFunctions.AGGREGATIONS) {
      aggrFunctions.put(aggrFunction.getIdent(), aggrFunction);
    }
  }

  public ExposedTableMeta table(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid table identifier '" + ident.getString() + "'.");
    }
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table '" + ident.getString() + "' does not exist.");
    }
    return tables.get(ident.getString());
  }

  public InputStream open(MaterializedTableMeta table) throws ModelException {
    String path = config.dataDir() + File.separator + table.getTableName();
    try {
      return new FileInputStream(path);
    } catch (FileNotFoundException e) {
      throw new ModelException("Table file not found: " + path + ".");
    }
  }

  public void registerTable(String tableName, ExposedTableMeta tableMeta) {
    tables.put(tableName, tableMeta);
  }

  public OutputStream openForWrite(
      String tableName, ImmutableMap<String, DataType<?>> columns) throws ModelException, FileNotFoundException {
    ImmutableMap.Builder<String, BasicColumn> builder = ImmutableMap.builder();
    int i = 0;
    for (Map.Entry<String, DataType<?>> entry : columns.entrySet()) {
      builder.put(entry.getKey(), new BasicColumn(i++, entry.getValue()));
    }
    tables.put(tableName, new MaterializedTableMeta(tableName, builder.build(), this));
    return new FileOutputStream(config.dataDir() + File.separator + tableName);
  }

  public SimpleFunction<?> simpleFunction(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid function identifier " + ident.getString() + ".");
    }
    if (!simpleFunctions.containsKey(ident.getString())) {
      throw new ModelException("Function " + ident.getString() + " does not exist.");
    }
    return simpleFunctions.get(ident.getString());
  }

  public TypedAggrFunction<?> aggrFunction(Ident ident, DataType<?> argType) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid function identifier " + ident.getString() + ".");
    }
    if (!aggrFunctions.containsKey(ident.getString())) {
      throw new ModelException("Function " + ident.getString() + " does not exist.");
    }
    return aggrFunctions.get(ident.getString()).forType(argType);
  }

  public static class ModelException extends Exception {
    private static final long serialVersionUID = 1L;

    public ModelException(String msg) {
      super(msg);
    }
  }
}

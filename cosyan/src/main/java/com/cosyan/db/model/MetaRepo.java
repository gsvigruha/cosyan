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
import com.cosyan.db.model.BuiltinFunctions.Function;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class MetaRepo {

  private final Config config;
  private final ConcurrentHashMap<String, MaterializedTableMeta> tables;
  private final ConcurrentHashMap<String, Function<?>> builtinFunctions;

  public MetaRepo(Config config) {
    this.config = config;
    this.tables = new ConcurrentHashMap<>();
    this.builtinFunctions = new ConcurrentHashMap<>();
    for (Function<?> builtinFunction : BuiltinFunctions.ALL) {
      builtinFunctions.put(builtinFunction.getIdent(), builtinFunction);
    }
  }

  public MaterializedTableMeta table(Ident ident) throws ModelException {
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

  public OutputStream openForWrite(
      String tableName, ImmutableMap<String, DataType<?>> columns) throws ModelException, FileNotFoundException {
    ImmutableMap.Builder<String, BasicColumn> builder = ImmutableMap.builder();
    for (Map.Entry<String, DataType<?>> entry : columns.entrySet()) {
      builder.put(entry.getKey(), new BasicColumn(entry.getKey(), entry.getValue()));
    }
    tables.put(tableName, new MaterializedTableMeta(tableName, builder.build(), this));
    return new FileOutputStream(config.dataDir() + File.separator + tableName);
  }

  public Function<?> function(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid function identifier " + ident.getString() + ".");
    }
    if (!builtinFunctions.containsKey(ident.getString())) {
      throw new ModelException("Function " + ident.getString() + " does not exist.");
    }
    return builtinFunctions.get(ident.getString());
  }

  public static class ModelException extends Exception {
    public ModelException(String msg) {
      super(msg);
    }
  }
}

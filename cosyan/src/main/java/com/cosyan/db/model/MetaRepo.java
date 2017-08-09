package com.cosyan.db.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.conf.Config;
import com.cosyan.db.model.BuiltinFunctions.Function;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class MetaRepo {

  private final Config config;
  private final ConcurrentHashMap<String, MaterializedTableMeta> tables;
  private final ConcurrentHashMap<String, Function> builtinFunctions;

  public MetaRepo(Config config) {
    this.config = config;
    this.tables = new ConcurrentHashMap<>();
    this.builtinFunctions = new ConcurrentHashMap<>();
  }

  public MaterializedTableMeta table(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid table identifier " + ident.getString() + ".");
    }
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table " + ident.getString() + " does not exist.");
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

  public Function function(Ident ident) throws ModelException {
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

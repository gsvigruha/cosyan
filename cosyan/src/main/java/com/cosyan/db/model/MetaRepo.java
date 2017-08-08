package com.cosyan.db.model;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.cosyan.db.model.BuiltinFunctions.Function;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class MetaRepo {

  private static MetaRepo repo = new MetaRepo();

  public static MetaRepo instance() {
    return repo;
  }

  private ConcurrentHashMap<String, MaterializedTableMeta> tables;
  private HashMap<String, Function> builtinFunctions;

  public MaterializedTableMeta table(Ident ident) throws ModelException {
    if (ident.parts().length != 1) {
      throw new ModelException("Invalid table identifier " + ident.getString() + ".");
    }
    if (!tables.containsKey(ident.getString())) {
      throw new ModelException("Table " + ident.getString() + " does not exist.");
    }
    return tables.get(ident.getString());
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

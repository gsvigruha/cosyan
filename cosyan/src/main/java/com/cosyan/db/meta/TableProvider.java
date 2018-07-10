package com.cosyan.db.meta;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta;

public interface TableProvider {

  public TableMeta tableMeta(Ident ident) throws ModelException;
  
  public TableProvider tableProvider(Ident ident) throws ModelException;

}

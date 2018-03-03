package com.cosyan.db.meta;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;

public interface TableProvider {

  public ExposedTableMeta tableMeta(Ident ident) throws ModelException;
}

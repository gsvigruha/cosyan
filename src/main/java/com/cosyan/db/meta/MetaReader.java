package com.cosyan.db.meta;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;

public interface MetaReader extends TableProvider {

  public MaterializedTable table(Ident ident) throws ModelException;

}

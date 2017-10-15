package com.cosyan.db.meta;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;

public interface MetaRepoReader {

  public MaterializedTableMeta table(Ident ident) throws ModelException;
}

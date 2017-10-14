package com.cosyan.db.meta;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;

public interface MetaRepoReader {

  public MaterializedTableMeta table(Ident ident) throws ModelException;
}

package com.cosyan.db.sql;

import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.sql.SyntaxTree.Select;

public class Compiler {

  public TableMeta query(SyntaxTree tree) throws ModelException {
    MetaRepo metaRepo = MetaRepo.instance();
    if (!tree.isSelect()) {
      throw new ModelException("Expected select.");
    }
    return ((Select) tree.getRoot()).compile(metaRepo);
  }
}

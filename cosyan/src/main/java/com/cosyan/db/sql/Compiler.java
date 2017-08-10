package com.cosyan.db.sql;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.sql.SyntaxTree.Select;

import lombok.Data;

@Data
public class Compiler {

  private final MetaRepo metaRepo;

  public TableMeta query(SyntaxTree tree) throws ModelException, ConfigException {
    if (!tree.isSelect()) {
      throw new ModelException("Expected select.");
    }
    return ((Select) tree.getRoot()).compile(metaRepo);
  }
}

package com.cosyan.db.model;

import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;

import lombok.Data;

@Data
public class TableRef {

  private final String name;
  private final String expr;
  private final int index;
  private final boolean aggr;
  protected final transient TableMeta tableMeta;

  @Override
  public String toString() {
    return name + " [" + expr + "]";
  }

  public void reCompile(MaterializedTable tableMeta) throws ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      parser.parseSelect(lexer.tokenizeExpression(expr + ";")).getTable().compile(tableMeta.reader());
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }
}

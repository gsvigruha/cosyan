package com.cosyan.db.session;

import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.google.common.collect.PeekingIterator;

public interface IParser {

  public static class ParserException extends Exception {
    private static final long serialVersionUID = 1L;

    public ParserException(String msg) {
      super(msg);
    }
  }

  MetaStatement parseMetaStatement(PeekingIterator<Token> tokens) throws ParserException;

  Iterable<Statement> parseStatements(PeekingIterator<Token> tokens) throws ParserException;

  boolean isMeta(PeekingIterator<Token> tokens);

}

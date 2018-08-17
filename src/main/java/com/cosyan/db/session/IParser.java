/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.session;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Statements.MetaStatement;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;

public interface IParser {

  public static class ParserException extends Exception {
    private static final long serialVersionUID = 1L;

    private final Loc loc;

    public ParserException(String msg, Token token) {
      super(msg);
      this.loc = token.getLoc();
    }

    public ParserException(String msg, Loc loc) {
      super(msg);
      this.loc = loc;
    }

    @Override
    public String getMessage() {
      return loc.toString() + ": " + super.getMessage();
    }
  }

  MetaStatement parseMetaStatement(PeekingIterator<Token> tokens) throws ParserException;

  Iterable<Statement> parseStatements(PeekingIterator<Token> tokens) throws ParserException;

  boolean isMeta(PeekingIterator<Token> tokens);

  Select parseSelect(PeekingIterator<Token> tokens) throws ParserException;

  Expression parseExpression(PeekingIterator<Token> tokens) throws ParserException;

  ImmutableList<Expression> parseExpressions(PeekingIterator<Token> tokens) throws ParserException;
}

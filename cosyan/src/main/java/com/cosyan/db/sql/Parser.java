package com.cosyan.db.sql;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cosyan.db.sql.SyntaxTree.AsExpression;
import com.cosyan.db.sql.SyntaxTree.AsTable;
import com.cosyan.db.sql.SyntaxTree.AsteriskExpression;
import com.cosyan.db.sql.SyntaxTree.DoubleLiteral;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.FuncCallExpression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
import com.cosyan.db.sql.SyntaxTree.JoinExpr;
import com.cosyan.db.sql.SyntaxTree.LongLiteral;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Select;
import com.cosyan.db.sql.SyntaxTree.StringLiteral;
import com.cosyan.db.sql.SyntaxTree.Table;
import com.cosyan.db.sql.SyntaxTree.TableExpr;
import com.cosyan.db.sql.SyntaxTree.TableRef;
import com.cosyan.db.sql.SyntaxTree.UnaryExpression;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class Parser {

  private Lexer lexer = new Lexer();

  public SyntaxTree parse(String sql) throws ParserException {
    return parse(lexer.tokenize(sql));
  }

  Expression parseExpression(String sql) throws ParserException {
    return parseExpression(Iterators.peekingIterator(lexer.tokenize(sql).iterator()), 0);
  }

  public SyntaxTree parse(ImmutableList<Token> tokens) throws ParserException {
    return new SyntaxTree(parseTokens(Iterators.peekingIterator(tokens.iterator())));
  }

  private Node parseTokens(PeekingIterator<Token> tokens) throws ParserException {
    if (tokens.peek().is(Tokens.SELECT)) {
      return parseSelect(tokens);
    }
    throw new ParserException("Syntax error.");
  }

  private Select parseSelect(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.SELECT);
    ImmutableList<Expression> columns = parseExprs(tokens, true, Tokens.FROM);
    tokens.next();
    Table table = parseTable(tokens);
    if (tokens.peek().is(Tokens.INNER) || tokens.peek().is(Tokens.LEFT) || tokens.peek().is(Tokens.RIGHT)) {
      Token joinType = tokens.next();
      assertNext(tokens, Tokens.JOIN);
      Table rightTable = parseTable(tokens);
      assertNext(tokens, Tokens.ON);
      Expression onExpr = parseExpression(tokens, 0);
      table = new JoinExpr(joinType, table, rightTable, onExpr);
    }
    Optional<Expression> where;
    if (tokens.peek().is(Tokens.WHERE)) {
      tokens.next();
      where = Optional.of(parseExpression(tokens, 0));
    } else {
      where = Optional.empty();
    }
    Optional<ImmutableList<Expression>> groupBy;
    Optional<Expression> having;
    if (tokens.peek().is(Tokens.GROUP)) {
      tokens.next();
      assertNext(tokens, Tokens.BY);
      groupBy = Optional.of(parseExprs(tokens, true,
          String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED), Tokens.HAVING, Tokens.ORDER));
      if (tokens.peek().is(Tokens.HAVING)) {
        tokens.next();
        having = Optional.of(parseExpression(tokens, 0));
      } else {
        having = Optional.empty();
      }
    } else {
      groupBy = Optional.empty();
      having = Optional.empty();
    }
    Optional<ImmutableList<Expression>> orderBy;
    if (tokens.peek().is(Tokens.ORDER)) {
      tokens.next();
      assertNext(tokens, Tokens.BY);
      orderBy = Optional.of(parseExprs(tokens, true,
          String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED)));
    } else {
      orderBy = Optional.empty();
    }
    assertPeek(tokens, String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED));
    return new Select(columns, table, where, groupBy, having, orderBy);
  }

  private Expression parsePrimary(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.peek();
    if (token.is(Tokens.PARENT_OPEN)) {
      tokens.next();
      Expression expr = parseExpression(tokens, 0);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return expr;
    } else if (token.is(Tokens.ASTERISK)) {
      tokens.next();
      return new AsteriskExpression();
    } else if (token.getString().matches(Tokens.IDENT)) {
      Ident ident = new Ident(tokens.next().getString());
      if (tokens.peek().is(Tokens.PARENT_OPEN)) {
        tokens.next();
        ImmutableList<Expression> argExprs = parseExprs(tokens, false, String.valueOf(Tokens.PARENT_CLOSED));
        tokens.next();
        return new FuncCallExpression(ident, argExprs);
      } else {
        return new IdentExpression(ident);
      }
    } else if (token.getString().matches(Tokens.LONG_LITERAL)) {
      tokens.next();
      return new LongLiteral(Long.valueOf(token.getString()));
    } else if (token.getString().matches(Tokens.DOUBLE_LITERAL)) {
      tokens.next();
      return new DoubleLiteral(Double.valueOf(token.getString()));
    } else if (token.getString().matches(Tokens.STRING_LITERAL)) {
      tokens.next();
      return new StringLiteral(token.getString().substring(1, token.getString().length() - 1));
    } else {
      throw new ParserException("Expected literal but got " + token.getString() + ".");
    }
  }

  private ImmutableList<Expression> parseExprs(
      PeekingIterator<Token> tokens,
      boolean allowAlias,
      String... terminators) throws ParserException {
    ImmutableList.Builder<Expression> exprs = ImmutableList.builder();
    while (true) {
      Expression expr = parseExpression(tokens, 0);
      if (allowAlias && tokens.peek().is(Tokens.AS)) {
        tokens.next();
        Ident ident = parseIdent(tokens);
        exprs.add(new AsExpression(ident, expr));
      } else {
        exprs.add(expr);
      }
      if (tokens.peek().is(Tokens.COMMA)) {
        tokens.next();
      } else {
        assertPeek(tokens, terminators);
        break;
      }
    }
    return exprs.build();
  }

  private Ident parseIdent(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.next();
    if (token.getString().matches(Tokens.IDENT)) {
      return new Ident(token.getString());
    } else {
      throw new ParserException("Expected identifier but got " + token.getString() + ".");
    }
  }

  private Table parseTable(PeekingIterator<Token> tokens) throws ParserException {
    final Table table;
    if (tokens.peek().is(Tokens.PARENT_OPEN)) {
      tokens.next();
      Select select = parseSelect(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      table = new TableExpr(select);
    } else {
      table = new TableRef(parseIdent(tokens));
    }
    if (tokens.peek().is(Tokens.AS)) {
      tokens.next();
      return new AsTable(parseIdent(tokens), table);
    } else {
      return table;
    }
  }

  Expression parseExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    if (precedence >= Tokens.BINARY_OPERATORS_PRECEDENCE.size()) {
      return parsePrimary(tokens);
    } else if (tokens.peek().is(Tokens.NOT)
        && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.NOT)) {
      return new UnaryExpression(tokens.next(), parseExpression(tokens, precedence + 1));
    } else {
      Expression primary = parseExpression(tokens, precedence + 1);
      if (tokens.peek().is(Tokens.ASC) && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.ASC)) {
        return new UnaryExpression(tokens.next(), primary);
      } else if (tokens.peek().is(Tokens.DESC)
          && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.DESC)) {
        return new UnaryExpression(tokens.next(), primary);
      } else if (tokens.peek().is(Tokens.IS)
          && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.IS)) {
        tokens.next();
        boolean not;
        if (tokens.peek().is(Tokens.NOT)) {
          not = true;
          tokens.next();
        } else {
          not = false;
        }
        assertNext(tokens, Tokens.NULL);
        return new UnaryExpression(
            not ? Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL) : Token.concat(Tokens.IS, Tokens.NULL), primary);
      } else {
        return parseBinaryExpression(primary, tokens, precedence);
      }
    }
  }

  private Expression parseBinaryExpression(Expression left, PeekingIterator<Token> tokens, int precedence)
      throws ParserException {
    for (;;) {
      Token token = tokens.peek();
      if (!Tokens.BINARY_OPERATORS.contains(token.getString())) {
        return left;
      }
      if (!Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(token.getString())) {
        return left;
      }
      tokens.next();
      Expression right = parseExpression(tokens, precedence + 1);
      left = new BinaryExpression(new Ident(token.getString()), left, right);
    }
  }

  private void assertNext(PeekingIterator<Token> tokens, String... values) throws ParserException {
    String next = tokens.next().getString();
    if (!ImmutableSet.copyOf(values).contains(next)) {
      throw new ParserException("Expected " + join(values) + " but got " + next + ".");
    }
  }

  private void assertPeek(PeekingIterator<Token> tokens, String... values) throws ParserException {
    String next = tokens.peek().getString();
    if (!ImmutableSet.copyOf(values).contains(next)) {
      throw new ParserException("Expected " + join(values) + " but got " + next + ".");
    }
  }

  private String join(String[] values) {
    return Stream.of(values).collect(Collectors.joining(", "));
  }

  public static class ParserException extends Exception {
    private static final long serialVersionUID = 1L;

    public ParserException(String msg) {
      super(msg);
    }
  }
}

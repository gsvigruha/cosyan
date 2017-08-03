package com.cosyan.db.sql;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cosyan.db.sql.SyntaxTree.AllColumns;
import com.cosyan.db.sql.SyntaxTree.BinaryExpression;
import com.cosyan.db.sql.SyntaxTree.ColumnAggr;
import com.cosyan.db.sql.SyntaxTree.ColumnExpr;
import com.cosyan.db.sql.SyntaxTree.ColumnRef;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Select;
import com.cosyan.db.sql.SyntaxTree.Table;
import com.cosyan.db.sql.SyntaxTree.TableExpr;
import com.cosyan.db.sql.SyntaxTree.TableRef;
import com.cosyan.db.sql.SyntaxTree.UnaryExpression;
import com.cosyan.db.sql.SyntaxTree.Where;
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

  public SyntaxTree parse(ImmutableList<Token> tokens) throws ParserException {
    return new SyntaxTree(parseTokens(Iterators.peekingIterator(tokens.iterator())));
  }

  private Node parseTokens(PeekingIterator<Token> tokens) throws ParserException {
    if (tokens.peek().getString().equals(Tokens.SELECT)) {
      return parseSelect(tokens);
    }
    throw new ParserException("Syntax error.");
  }

  private Select parseSelect(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.SELECT);
    ImmutableList<ColumnExpr> columns = parseColumns(tokens);
    Table table = parseTable(tokens);
    Optional<Where> where;
    if (tokens.peek().getString().equals(Tokens.WHERE)) {
      where = Optional.of(parseWhere(tokens));
    } else {
      where = Optional.empty();
    }
    return new Select(columns, table, where);
  }

  private ImmutableList<ColumnExpr> parseColumns(PeekingIterator<Token> tokens)
      throws ParserException {
    ImmutableList.Builder<ColumnExpr> columns = ImmutableList.builder();
    while(true) {
      columns.add(parseColumn(tokens));
      if (tokens.peek().getString().equals(Tokens.COMMA)) {
        tokens.next();
      } else if (tokens.peek().getString().equals(Tokens.FROM)) {
        tokens.next();
        break;
      }
    }
    return columns.build();
  }

  private ColumnExpr parseColumn(PeekingIterator<Token> tokens) throws ParserException {
    System.out.println(tokens.peek().getString());
    if (tokens.peek().is(Tokens.ASTERISK)) {
      tokens.next();
      assertPeek(tokens, Tokens.FROM, String.valueOf(Tokens.COMMA));
      return new AllColumns();
    } else if (Tokens.AGGREGATORS.contains(tokens.peek().getString())) {
      String aggr = tokens.next().getString();
      Ident ident = parseIdent(tokens);
      assertPeek(tokens, Tokens.FROM, String.valueOf(Tokens.COMMA));
      return new ColumnAggr(aggr, ident);
    } else {
      Ident ident = parseIdent(tokens);
      assertPeek(tokens, Tokens.FROM, String.valueOf(Tokens.COMMA));
      return new ColumnRef(ident);
    }
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
    if (tokens.peek().getString().equals(Tokens.PARENT_OPEN)) {
      tokens.next();
      Select select = parseSelect(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new TableExpr(select);
    } else {
      return new TableRef(parseIdent(tokens));
    }
  }

  private Where parseWhere(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.WHERE);
    Expression condition = parseExpression(tokens, 0);
    return new Where(condition);
  }

  private Expression parseExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    if (tokens.peek().getString().equals(Tokens.PARENT_OPEN)) {
      Expression expr = parseExpression(tokens, 0);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return expr;
    } else if(tokens.peek().getString().equals(Tokens.NOT)) {
      return new UnaryExpression(tokens.next(), parseExpression(tokens, precedence + 1));
    } else {
      return parseBinaryExpression(tokens, precedence);
    }
  }
  
  private Expression parseBinaryExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    Expression left = parseExpression(tokens, precedence + 1);
    if (Tokens.BINARY_BOOL_OPERATORS.keySet().contains(tokens.peek().getString())) {
      Token op = tokens.next();
      Expression right = parseExpression(tokens, precedence + 1);
      return new BinaryExpression(op, left, right);
    } else {
      return left;
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
    public ParserException(String msg) {
      super(msg);
    }
  }
}

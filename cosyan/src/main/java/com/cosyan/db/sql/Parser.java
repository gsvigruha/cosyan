package com.cosyan.db.sql;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cosyan.db.sql.SyntaxTree.AsteriskExpression;
import com.cosyan.db.sql.SyntaxTree.BinaryExpression;
import com.cosyan.db.sql.SyntaxTree.DoubleLiteral;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.FuncCallExpression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
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
    ImmutableList<Expression> columns = parseExprs(tokens, Tokens.FROM);
    Table table = parseTable(tokens);
    Optional<Expression> where;
    if (tokens.peek().is(Tokens.WHERE)) {
      tokens.next();
      where = Optional.of(parseExpression(tokens, 0));
    } else {
      where = Optional.empty();
    }
    return new Select(columns, table, where);
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
        return new FuncCallExpression(
            ident, parseExprs(tokens, String.valueOf(Tokens.PARENT_CLOSED)));
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

  private ImmutableList<Expression> parseExprs(PeekingIterator<Token> tokens, String terminator)
      throws ParserException {
    ImmutableList.Builder<Expression> exprs = ImmutableList.builder();
    while (true) {
      exprs.add(parsePrimary(tokens));
      assertPeek(tokens, String.valueOf(Tokens.COMMA), terminator);
      if (tokens.peek().is(Tokens.COMMA)) {
        tokens.next();
      } else if (tokens.peek().is(terminator)) {
        tokens.next();
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
    if (tokens.peek().is(Tokens.PARENT_OPEN)) {
      tokens.next();
      Select select = parseSelect(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new TableExpr(select);
    } else {
      return new TableRef(parseIdent(tokens));
    }
  }

  Expression parseExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    if (precedence >= Tokens.BINARY_OPERATORS_PRECEDENCE.size()) {
      return parsePrimary(tokens);
    } else if (tokens.peek().is(Tokens.NOT) && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.NOT)) {
      return new UnaryExpression(new Ident(tokens.next().getString()), parseExpression(tokens, precedence + 1));
    } else {
      return parseBinaryExpression(tokens, precedence);
    }
  }

  private Expression parseBinaryExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    Expression left = parseExpression(tokens, precedence + 1);
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
    public ParserException(String msg) {
      super(msg);
    }
  }
}

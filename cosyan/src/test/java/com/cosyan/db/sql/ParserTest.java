package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.AsteriskExpression;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.FuncCallExpression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
import com.cosyan.db.sql.SyntaxTree.LongLiteral;
import com.cosyan.db.sql.SyntaxTree.Select;
import com.cosyan.db.sql.SyntaxTree.StringLiteral;
import com.cosyan.db.sql.SyntaxTree.TableRef;
import com.cosyan.db.sql.SyntaxTree.UnaryExpression;
import com.google.common.collect.ImmutableList;

public class ParserTest {

  private Parser parser = new Parser();

  @Test
  public void testSelect() throws ParserException {
    SyntaxTree tree = parser.parse("select * from table;");
    assertEquals(tree, new SyntaxTree(new Select(
        ImmutableList.of(new AsteriskExpression()),
        new TableRef(new Ident("table")),
        Optional.empty())));
  }

  @Test
  public void testSelectColmns() throws ParserException {
    SyntaxTree tree = parser.parse("select a, b from table;");
    assertEquals(tree, new SyntaxTree(new Select(
        ImmutableList.of(new IdentExpression(new Ident("a")), new IdentExpression(new Ident("b"))),
        new TableRef(new Ident("table")),
        Optional.empty())));
  }

  @Test
  public void testSelectAggr() throws ParserException {
    SyntaxTree tree = parser.parse("select sum(a) from table;");
    assertEquals(tree, new SyntaxTree(new Select(
        ImmutableList.of(new FuncCallExpression(
            new Ident("sum"),
            ImmutableList.of(new IdentExpression(new Ident("a"))))),
        new TableRef(new Ident("table")),
        Optional.empty())));
  }

  @Test
  public void testSelectWhere() throws ParserException {
    SyntaxTree tree = parser.parse("select * from table where a = 1;");
    assertEquals(tree, new SyntaxTree(new Select(
        ImmutableList.of(new AsteriskExpression()),
        new TableRef(new Ident("table")),
        Optional.of(new BinaryExpression(
            new Ident("="),
            new IdentExpression(new Ident("a")),
            new LongLiteral(1))))));
  }

  @Test
  public void testExpr() throws ParserException {
    Expression expr = parser.parseExpression("a = 1;");
    assertEquals(expr, new BinaryExpression(
        new Ident("="),
        new IdentExpression(new Ident("a")),
        new LongLiteral(1)));
  }

  @Test
  public void testExprPrecedence1() throws ParserException {
    Expression expr = parser.parseExpression("a and b or c;");
    assertEquals(expr, new BinaryExpression(
        new Ident("or"),
        new BinaryExpression(
            new Ident("and"),
            new IdentExpression(new Ident("a")),
            new IdentExpression(new Ident("b"))),
        new IdentExpression(new Ident("c"))));
  }

  @Test
  public void testExprPrecedence2() throws ParserException {
    Expression expr = parser.parseExpression("a or b and c;");
    assertEquals(expr, new BinaryExpression(
        new Ident("or"),
        new IdentExpression(new Ident("a")),
        new BinaryExpression(
            new Ident("and"),
            new IdentExpression(new Ident("b")),
            new IdentExpression(new Ident("c")))));
  }

  @Test
  public void testExprParentheses1() throws ParserException {
    Expression expr = parser.parseExpression("(a or b) and c;");
    assertEquals(expr, new BinaryExpression(
        new Ident("and"),
        new BinaryExpression(
            new Ident("or"),
            new IdentExpression(new Ident("a")),
            new IdentExpression(new Ident("b"))),
        new IdentExpression(new Ident("c"))));
  }

  @Test
  public void testExprParentheses2() throws ParserException {
    Expression expr = parser.parseExpression("a and (b or c);");
    assertEquals(expr, new BinaryExpression(
        new Ident("and"),
        new IdentExpression(new Ident("a")),
        new BinaryExpression(
            new Ident("or"),
            new IdentExpression(new Ident("b")),
            new IdentExpression(new Ident("c")))));
  }

  @Test
  public void testExprLogical() throws ParserException {
    Expression expr = parser.parseExpression("a > 1 or c;");
    assertEquals(expr, new BinaryExpression(
        new Ident("or"),
        new BinaryExpression(
            new Ident(">"),
            new IdentExpression(new Ident("a")),
            new LongLiteral(1)),
        new IdentExpression(new Ident("c"))));
  }

  @Test
  public void testExprFuncCall() throws ParserException {
    Expression expr = parser.parseExpression("a and f(b);");
    assertEquals(expr, new BinaryExpression(
        new Ident("and"),
        new IdentExpression(new Ident("a")),
        new FuncCallExpression(new Ident("f"), ImmutableList.of(new IdentExpression(new Ident("b"))))));
  }

  @Test
  public void testExprNot() throws ParserException {
    Expression expr = parser.parseExpression("not a;");
    assertEquals(expr, new UnaryExpression(
        new Ident("not"),
        new IdentExpression(new Ident("a"))));
  }

  @Test
  public void testExprNotInBinary() throws ParserException {
    Expression expr = parser.parseExpression("not a and not b;");
    assertEquals(expr, new BinaryExpression(
        new Ident("and"),
        new UnaryExpression(new Ident("not"), new IdentExpression(new Ident("a"))),
        new UnaryExpression(new Ident("not"), new IdentExpression(new Ident("b")))));
  }

  @Test
  public void testExprNotWithLogical() throws ParserException {
    Expression expr = parser.parseExpression("not a = 'x';");
    assertEquals(expr, new UnaryExpression(
        new Ident("not"),
        new BinaryExpression(
            new Ident("="),
            new IdentExpression(new Ident("a")),
            new StringLiteral("x"))));
  }
}

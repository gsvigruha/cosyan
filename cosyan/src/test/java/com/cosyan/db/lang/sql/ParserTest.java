package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Expression.UnaryExpression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.expr.Literals.DoubleLiteral;
import com.cosyan.db.lang.expr.Literals.LongLiteral;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.SelectStatement.AsteriskExpression;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.SelectStatement.TableRef;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.model.Ident;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;

public class ParserTest {

  private Lexer lexer = new Lexer();
  private Parser parser = new Parser();

  private ImmutableList<Statement> parse(String sql) throws ParserException {
    return parser.parseStatements(lexer.tokenize(sql));
  }

  @Test
  public void testSelect() throws ParserException {
    ImmutableList<Statement> tree = parse("select * from table;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(new AsteriskExpression()),
        new TableRef(new Ident("table")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  @Test
  public void testSelectColumns() throws ParserException {
    ImmutableList<Statement> tree = parse("select a, b from table;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(FuncCallExpression.of(new Ident("a")), FuncCallExpression.of(new Ident("b"))),
        new TableRef(new Ident("table")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  @Test
  public void testSelectAggr() throws ParserException {
    ImmutableList<Statement> tree = parse("select sum(a) from table;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(new FuncCallExpression(
            new Ident("sum"),
            null,
            ImmutableList.of(FuncCallExpression.of(new Ident("a"))))),
        new TableRef(new Ident("table")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  @Test
  public void testSelectWhere() throws ParserException {
    ImmutableList<Statement> tree = parse("select * from table where a = 1;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(new AsteriskExpression()),
        new TableRef(new Ident("table")),
        Optional.of(new BinaryExpression(
            new Token("="),
            FuncCallExpression.of(new Ident("a")),
            new LongLiteral(1L))),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  @Test
  public void testSelectGroupBy() throws ParserException {
    ImmutableList<Statement> tree = parse("select sum(b) from table group by a;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(new FuncCallExpression(
            new Ident("sum"),
            null,
            ImmutableList.of(FuncCallExpression.of(new Ident("b"))))),
        new TableRef(new Ident("table")),
        Optional.empty(),
        Optional.of(ImmutableList.of(FuncCallExpression.of(new Ident("a")))),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  @Test
  public void testSelectComplex() throws ParserException {
    ImmutableList<Statement> tree = parse("select a, b + 1, c * 2.0 > 3.0 from table;");
    assertEquals(tree, ImmutableList.of(new Select(
        ImmutableList.of(
            FuncCallExpression.of(new Ident("a")),
            new BinaryExpression(
                new Token("+"),
                FuncCallExpression.of(new Ident("b")),
                new LongLiteral(1L)),
            new BinaryExpression(
                new Token(">"),
                new BinaryExpression(
                    new Token("*"),
                    FuncCallExpression.of(new Ident("c")),
                    new DoubleLiteral(2.0)),
                new DoubleLiteral(3.0))),
        new TableRef(new Ident("table")),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        /* distinct= */false)));
  }

  private Expression parseExpression(String sql) throws ParserException {
    return parser.parseExpression(lexer.tokenize(sql));
  }

  @Test
  public void testExpr() throws ParserException {
    Expression expr = parseExpression("a = 1;");
    assertEquals(expr, new BinaryExpression(
        new Token("="),
        FuncCallExpression.of(new Ident("a")),
        new LongLiteral(1L)));
    assertEquals("(a = 1)", expr.print());
  }

  @Test
  public void testExprPrecedence1() throws ParserException {
    Expression expr = parseExpression("a and b or c;");
    assertEquals(expr, new BinaryExpression(
        new Token("or"),
        new BinaryExpression(
            new Token("and"),
            FuncCallExpression.of(new Ident("a")),
            FuncCallExpression.of(new Ident("b"))),
        FuncCallExpression.of(new Ident("c"))));
    assertEquals("((a and b) or c)", expr.print());
  }

  @Test
  public void testExprPrecedence2() throws ParserException {
    Expression expr = parseExpression("a or b and c;");
    assertEquals(expr, new BinaryExpression(
        new Token("or"),
        FuncCallExpression.of(new Ident("a")),
        new BinaryExpression(
            new Token("and"),
            FuncCallExpression.of(new Ident("b")),
            FuncCallExpression.of(new Ident("c")))));
    assertEquals("(a or (b and c))", expr.print());
  }

  @Test
  public void testExprParentheses1() throws ParserException {
    Expression expr = parseExpression("(a or b) and c;");
    assertEquals(expr, new BinaryExpression(
        new Token("and"),
        new BinaryExpression(
            new Token("or"),
            FuncCallExpression.of(new Ident("a")),
            FuncCallExpression.of(new Ident("b"))),
        FuncCallExpression.of(new Ident("c"))));
    assertEquals("((a or b) and c)", expr.print());
  }

  @Test
  public void testExprParentheses2() throws ParserException {
    Expression expr = parseExpression("a and (b or c);");
    assertEquals(expr, new BinaryExpression(
        new Token("and"),
        FuncCallExpression.of(new Ident("a")),
        new BinaryExpression(
            new Token("or"),
            FuncCallExpression.of(new Ident("b")),
            FuncCallExpression.of(new Ident("c")))));
    assertEquals("(a and (b or c))", expr.print());
  }

  @Test
  public void testExprLogical() throws ParserException {
    Expression expr = parseExpression("a > 1 or c;");
    assertEquals(expr, new BinaryExpression(
        new Token("or"),
        new BinaryExpression(
            new Token(">"),
            FuncCallExpression.of(new Ident("a")),
            new LongLiteral(1L)),
        FuncCallExpression.of(new Ident("c"))));
    assertEquals("((a > 1) or c)", expr.print());
  }

  @Test
  public void testExprFuncCall() throws ParserException {
    Expression expr = parseExpression("a and f(b);");
    assertEquals(expr, new BinaryExpression(
        new Token("and"),
        FuncCallExpression.of(new Ident("a")),
        new FuncCallExpression(new Ident("f"), null, ImmutableList.of(FuncCallExpression.of(new Ident("b"))))));
    assertEquals("(a and f(b))", expr.print());
  }

  @Test
  public void testExprFuncCallMultipleArgs() throws ParserException {
    Expression expr = parseExpression("f(a,b,c);");
    assertEquals(expr, new FuncCallExpression(new Ident("f"), null, ImmutableList.of(
        FuncCallExpression.of(new Ident("a")),
        FuncCallExpression.of(new Ident("b")),
        FuncCallExpression.of(new Ident("c")))));
    assertEquals("f(a, b, c)", expr.print());
  }

  @Test
  public void testExprAggr() throws ParserException {
    Expression expr = parseExpression("sum(a);");
    assertEquals(expr, new FuncCallExpression(new Ident("sum"), null, ImmutableList.of(
        FuncCallExpression.of(new Ident("a")))));
    assertEquals("sum(a)", expr.print());
  }

  @Test
  public void testExprNot() throws ParserException {
    Expression expr = parseExpression("not a;");
    assertEquals(expr, new UnaryExpression(
        new Token(Tokens.NOT),
        FuncCallExpression.of(new Ident("a"))));
    assertEquals("not a", expr.print());
  }

  @Test
  public void testExprNotInBinary() throws ParserException {
    Expression expr = parseExpression("not a and not b;");
    assertEquals(expr, new BinaryExpression(
        new Token("and"),
        new UnaryExpression(new Token(Tokens.NOT), FuncCallExpression.of(new Ident("a"))),
        new UnaryExpression(new Token(Tokens.NOT), FuncCallExpression.of(new Ident("b")))));
    assertEquals("(not a and not b)", expr.print());
  }

  @Test
  public void testExprNotWithLogical() throws ParserException {
    Expression expr = parseExpression("not a = 'x';");
    assertEquals(expr, new UnaryExpression(
        new Token(Tokens.NOT),
        new BinaryExpression(
            new Token("="),
            FuncCallExpression.of(new Ident("a")),
            new StringLiteral("x"))));
    assertEquals("not (a = 'x')", expr.print());
  }

  @Test
  public void testFuncCallOnFuncCall() throws ParserException {
    Expression expr = parseExpression("a.b.f().c.g();");
    assertEquals(expr, new FuncCallExpression(
        new Ident("g"),
        new FuncCallExpression(
            new Ident("c"),
            new FuncCallExpression(
                new Ident("f"),
                new FuncCallExpression(
                    new Ident("b"),
                    new FuncCallExpression(
                        new Ident("a"),
                        null,
                        ImmutableList.of()),
                    ImmutableList.of()),
                ImmutableList.of()),
            ImmutableList.of()),
        ImmutableList.of()));
    assertEquals("a.b.f.c.g", expr.print());
  }
}

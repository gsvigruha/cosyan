package com.cosyan.db.lang.sql;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.CaseExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Expression.UnaryExpression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.expr.Literals.BooleanLiteral;
import com.cosyan.db.lang.expr.Literals.DateLiteral;
import com.cosyan.db.lang.expr.Literals.DoubleLiteral;
import com.cosyan.db.lang.expr.Literals.Literal;
import com.cosyan.db.lang.expr.Literals.LongLiteral;
import com.cosyan.db.lang.expr.Literals.NullLiteral;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableAddColumn;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableAlterColumn;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableDropColumn;
import com.cosyan.db.lang.sql.AlterStatementConstraints.AlterTableAddConstraint;
import com.cosyan.db.lang.sql.AlterStatementRefs.AlterTableAddRef;
import com.cosyan.db.lang.sql.CreateStatement.ColumnDefinition;
import com.cosyan.db.lang.sql.CreateStatement.ConstraintDefinition;
import com.cosyan.db.lang.sql.CreateStatement.CreateIndex;
import com.cosyan.db.lang.sql.CreateStatement.CreateTable;
import com.cosyan.db.lang.sql.CreateStatement.ForeignKeyDefinition;
import com.cosyan.db.lang.sql.CreateStatement.PrimaryKeyDefinition;
import com.cosyan.db.lang.sql.CreateStatement.RefDefinition;
import com.cosyan.db.lang.sql.CreateStatement.RuleDefinition;
import com.cosyan.db.lang.sql.DeleteStatement.Delete;
import com.cosyan.db.lang.sql.DropStatement.DropIndex;
import com.cosyan.db.lang.sql.DropStatement.DropTable;
import com.cosyan.db.lang.sql.GrantStatement.Grant;
import com.cosyan.db.lang.sql.InsertIntoStatement.InsertInto;
import com.cosyan.db.lang.sql.SelectStatement.AsExpression;
import com.cosyan.db.lang.sql.SelectStatement.AsTable;
import com.cosyan.db.lang.sql.SelectStatement.AsteriskExpression;
import com.cosyan.db.lang.sql.SelectStatement.JoinExpr;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.SelectStatement.Table;
import com.cosyan.db.lang.sql.SelectStatement.TableExpr;
import com.cosyan.db.lang.sql.SelectStatement.TableRef;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.sql.UpdateStatement.SetExpression;
import com.cosyan.db.lang.sql.UpdateStatement.Update;
import com.cosyan.db.lang.sql.Users.CreateUser;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.DateFunctions;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.session.IParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.PeekingIterator;

public class Parser implements IParser {

  public boolean isMeta(PeekingIterator<Token> tokens) {
    if (tokens.peek().is(Tokens.CREATE) ||
        tokens.peek().is(Tokens.ALTER) ||
        tokens.peek().is(Tokens.DROP) ||
        tokens.peek().is(Tokens.GRANT)) {
      return true;
    }
    return false;
  }

  public ImmutableList<Statement> parseStatements(PeekingIterator<Token> tokens) throws ParserException {
    ImmutableList.Builder<Statement> roots = ImmutableList.builder();
    while (tokens.hasNext()) {
      Statement root = parseStatement(tokens);
      roots.add(root);
      assertNext(tokens, String.valueOf(Tokens.COMMA_COLON));
    }
    return roots.build();
  }

  public MetaStatement parseMetaStatement(PeekingIterator<Token> tokens) throws ParserException {
    if (tokens.peek().is(Tokens.CREATE)) {
      return parseCreate(tokens);
    } else if (tokens.peek().is(Tokens.DROP)) {
      return parseDrop(tokens);
    } else if (tokens.peek().is(Tokens.ALTER)) {
      return parseAlter(tokens);
    } else if (tokens.peek().is(Tokens.GRANT)) {
      return parseGrant(tokens);
    }
    throw new ParserException("Syntax error, expected create, drop or alter.");
  }

  private Statement parseStatement(PeekingIterator<Token> tokens) throws ParserException {
    if (tokens.peek().is(Tokens.SELECT)) {
      return parseSelect(tokens);
    } else if (tokens.peek().is(Tokens.INSERT)) {
      return parseInsert(tokens);
    } else if (tokens.peek().is(Tokens.DELETE)) {
      return parseDelete(tokens);
    } else if (tokens.peek().is(Tokens.UPDATE)) {
      return parseUpdate(tokens);
    }
    throw new ParserException("Syntax error, expected select, insert, delete or update.");
  }

  private Delete parseDelete(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.DELETE);
    assertNext(tokens, Tokens.FROM);
    Ident ident = parseIdent(tokens);
    assertNext(tokens, Tokens.WHERE);
    Expression where = parseExpression(tokens, 0);
    return new Delete(ident, where);
  }

  private InsertInto parseInsert(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.INSERT);
    assertNext(tokens, Tokens.INTO);
    Ident ident = parseIdent(tokens);
    Optional<ImmutableList<Ident>> columns;
    if (tokens.peek().is(Tokens.PARENT_OPEN)) {
      tokens.next();
      ImmutableList.Builder<Ident> builder = ImmutableList.builder();
      while (true) {
        builder.add(parseIdent(tokens));
        if (tokens.peek().is(Tokens.COMMA)) {
          tokens.next();
        } else {
          assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
          break;
        }
      }
      columns = Optional.of(builder.build());
    } else {
      columns = Optional.empty();
    }
    assertNext(tokens, Tokens.VALUES);

    ImmutableList.Builder<ImmutableList<Literal>> valuess = ImmutableList.builder();
    while (true) {
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      ImmutableList.Builder<Literal> values = ImmutableList.builder();
      while (true) {
        values.add(parseLiteral(tokens));
        if (tokens.peek().is(Tokens.COMMA)) {
          tokens.next();
        } else {
          assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
          break;
        }
      }
      valuess.add(values.build());
      if (tokens.peek().is(Tokens.COMMA)) {
        tokens.next();
      } else {
        break;
      }
    }
    return new InsertInto(ident, columns, valuess.build());
  }

  private Literal parseLiteral(PeekingIterator<Token> tokens) throws ParserException {
    Expression expr = parsePrimary(tokens);
    if (!(expr instanceof Literal)) {
      throw new ParserException("Expected literal but got '" + expr + "'.");
    }
    return (Literal) expr;
  }

  private Update parseUpdate(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.UPDATE);
    Ident ident = parseIdent(tokens);
    assertNext(tokens, Tokens.SET);
    ImmutableList.Builder<SetExpression> updates = ImmutableList.builder();
    while (true) {
      Ident columnIdent = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.EQ));
      Expression expr = parseExpression(tokens, 0);
      updates.add(new SetExpression(columnIdent, expr));
      if (tokens.peek().is(Tokens.COMMA)) {
        tokens.next();
      } else {
        break;
      }
    }

    Optional<Expression> where;
    if (tokens.peek().is(Tokens.WHERE)) {
      tokens.next();
      where = Optional.of(parseExpression(tokens, 0));
    } else {
      where = Optional.empty();
    }
    return new Update(ident, updates.build(), where);
  }

  private MetaStatement parseCreate(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.CREATE);
    MaterializedTableMeta.Type type = MaterializedTableMeta.Type.LOG;
    if (tokens.peek().is(Tokens.LOG)) {
      tokens.next();
      type = MaterializedTableMeta.Type.LOG;
    } else if (tokens.peek().is(Tokens.LOOKUP)) {
      tokens.next();
      type = MaterializedTableMeta.Type.LOOKUP;
    }
    assertPeek(tokens, Tokens.TABLE, Tokens.INDEX, Tokens.USER);
    if (tokens.peek().is(Tokens.TABLE)) {
      tokens.next();
      Ident ident = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      ImmutableList.Builder<ColumnDefinition> columns = ImmutableList.builder();
      ImmutableList.Builder<ConstraintDefinition> constraints = ImmutableList.builder();
      while (true) {
        if (tokens.peek().is(Tokens.CONSTRAINT)) {
          constraints.add(parseConstraint(tokens));
        } else {
          columns.add(parseColumnDefinition(tokens));
        }
        if (tokens.peek().is(Tokens.COMMA)) {
          tokens.next();
        } else {
          assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
          break;
        }
      }
      Optional<Expression> partitioning;
      if (tokens.peek().is(Tokens.PARTITION)) {
        tokens.next();
        assertNext(tokens, Tokens.BY);
        partitioning = Optional.of(parseExpression(tokens));
      } else {
        partitioning = Optional.empty();
      }
      return new CreateTable(ident.getString(), type, columns.build(), constraints.build(), partitioning);
    } else if (tokens.peek().is(Tokens.INDEX)) {
      assertNext(tokens, Tokens.INDEX);
      Ident table = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.DOT));
      Ident column = parseIdent(tokens);
      return new CreateIndex(table, column);
    } else {
      assertNext(tokens, Tokens.USER);
      Ident username = parseIdent(tokens);
      assertNext(tokens, Tokens.IDENTIFIED);
      assertNext(tokens, Tokens.BY);
      StringLiteral password = (StringLiteral) parseLiteral(tokens);
      return new CreateUser(username, password);
    }
  }

  private RefDefinition parseRef(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.REF);
    Ident ident = parseIdent(tokens);
    assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
    Select select = parseSelect(tokens);
    assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
    return new RefDefinition(ident.getString(), select);
  }

  private MetaStatement parseDrop(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.DROP);
    assertPeek(tokens, Tokens.TABLE, Tokens.INDEX);
    if (tokens.peek().is(Tokens.TABLE)) {
      tokens.next();
      Ident ident = parseIdent(tokens);
      return new DropTable(ident);
    } else {
      assertNext(tokens, Tokens.INDEX);
      Ident table = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.DOT));
      Ident column = parseIdent(tokens);
      return new DropIndex(table, column);
    }
  }

  private MetaStatement parseAlter(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.ALTER);
    assertNext(tokens, Tokens.TABLE);
    Ident ident = parseIdent(tokens);
    if (tokens.peek().is(Tokens.ADD)) {
      tokens.next();
      if (tokens.peek().is(Tokens.REF)) {
        RefDefinition ref = parseRef(tokens);
        return new AlterTableAddRef(ident, ref);
      } else if (tokens.peek().is(Tokens.CONSTRAINT)) {
        ConstraintDefinition constraint = parseConstraint(tokens);
        return new AlterTableAddConstraint(ident, constraint);
      } else {
        ColumnDefinition column = parseColumnDefinition(tokens);
        return new AlterTableAddColumn(ident, column);
      }
    } else if (tokens.peek().is(Tokens.DROP)) {
      tokens.next();
      Ident columnName = parseIdent(tokens);
      return new AlterTableDropColumn(ident, columnName);
    } else if (tokens.peek().is(Tokens.ALTER) || tokens.peek().is(Tokens.MODIFY)) {
      tokens.next();
      ColumnDefinition column = parseColumnDefinition(tokens);
      return new AlterTableAlterColumn(ident, column);
    } else {
      throw new ParserException("Unsupported alter operation '" + tokens.peek() + "'.");
    }
  }

  private MetaStatement parseGrant(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.GRANT);
    assertPeek(tokens, Tokens.SELECT, Tokens.INSERT, Tokens.UPDATE, Tokens.DELETE, Tokens.ALL);
    Ident method = parseIdent(tokens);
    assertNext(tokens, Tokens.ON);
    Ident table;
    if (tokens.peek().is(Tokens.ASTERISK)) {
      tokens.next();
      table = new Ident("*");
    } else {
      table = parseIdent(tokens);
    }
    assertNext(tokens, Tokens.TO);
    Ident username = parseIdent(tokens);
    boolean withGrantOption = false;
    if (tokens.peek().is(Tokens.WITH)) {
      tokens.next();
      assertNext(tokens, Tokens.GRANT);
      assertNext(tokens, Tokens.OPTION);
      withGrantOption = true;
    }
    return new Grant(username, table, method.getString(), withGrantOption);
  }

  private ConstraintDefinition parseConstraint(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.CONSTRAINT);
    Ident ident = parseIdent(tokens);
    if (tokens.peek().is(Tokens.CHECK)) {
      tokens.next();
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Expression expr = parseExpression(tokens, 0);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      boolean nullIsTrue = true;
      if (tokens.peek().is(Tokens.NOT)) {
        tokens.next();
        assertNext(tokens, Tokens.NULL);
        nullIsTrue = false;
      }
      return new RuleDefinition(ident.getString(), expr, nullIsTrue);
    } else if (tokens.peek().is(Tokens.PRIMARY)) {
      tokens.next();
      assertNext(tokens, Tokens.KEY);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Ident column = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new PrimaryKeyDefinition(ident.getString(), column);
    } else if (tokens.peek().is(Tokens.FOREIGN)) {
      tokens.next();
      assertNext(tokens, Tokens.KEY);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Ident column = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      assertNext(tokens, Tokens.REFERENCES);
      Ident refTable = parseIdent(tokens);
      Optional<Ident> refColumn = Optional.empty();
      if (tokens.peek().is(Tokens.PARENT_OPEN)) {
        tokens.next();
        refColumn = Optional.of(parseIdent(tokens));
        assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      }
      if (tokens.peek().is(Tokens.REVERSE)) {
        tokens.next();
        Ident reverseIdent = parseIdent(tokens);
        return new ForeignKeyDefinition(ident.getString(), reverseIdent.getString(), column, refTable, refColumn);
      } else {
        return new ForeignKeyDefinition(ident.getString(), "rev_" + ident.getString(), column, refTable, refColumn);
      }
    } else {
      throw new ParserException("Unsupported constraint '" + tokens.peek() + "'.");
    }
  }

  private ColumnDefinition parseColumnDefinition(PeekingIterator<Token> tokens) throws ParserException {
    Ident ident = parseIdent(tokens);
    DataType<?> type = parseDataType(tokens);
    boolean unique;
    if (tokens.peek().is(Tokens.UNIQUE)) {
      tokens.next();
      unique = true;
    } else {
      unique = false;
    }
    boolean nullable;
    if (tokens.peek().is(Tokens.NOT)) {
      tokens.next();
      assertNext(tokens, Tokens.NULL);
      nullable = false;
    } else {
      nullable = true;
    }
    boolean immutable;
    if (tokens.peek().is(Tokens.IMMUTABLE)) {
      tokens.next();
      immutable = true;
    } else {
      immutable = false;
    }
    return new ColumnDefinition(ident.getString(), type, nullable, unique, immutable);
  }

  private DataType<?> parseDataType(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.next();
    if (token.is(Tokens.VARCHAR)) {
      return DataTypes.StringType;
    } else if (token.is(Tokens.INTEGER)) {
      return DataTypes.LongType;
    } else if (token.is(Tokens.FLOAT)) {
      return DataTypes.DoubleType;
    } else if (token.is(Tokens.TIMESTAMP)) {
      return DataTypes.DateType;
    } else if (token.is(Tokens.BOOLEAN)) {
      return DataTypes.BoolType;
    } else if (token.is(Tokens.ID)) {
      return DataTypes.IDType;
    }
    throw new ParserException("Unknown data type '" + token + "'.");
  }

  private Select parseSelect(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.SELECT);
    boolean distinct = false;
    if (tokens.peek().is(Tokens.DISTINCT)) {
      tokens.next();
      distinct = true;
    }
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
    return new Select(columns, table, where, groupBy, having, orderBy, distinct);
  }

  private Expression parsePrimary(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.peek();
    Expression expr;
    if (token.is(Tokens.PARENT_OPEN)) {
      tokens.next();
      expr = parseExpression(tokens, 0);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return expr;
    } else if (token.is(Tokens.ASTERISK)) {
      tokens.next();
      expr = new AsteriskExpression();
    } else if (token.is(Tokens.NULL)) {
      tokens.next();
      expr = new NullLiteral();
    } else if (token.is(Tokens.CASE)) {
      expr = parseCase(tokens);
    } else if (token.is(Tokens.DT)) {
      tokens.next();
      Token value = tokens.next();
      Object date = DateFunctions.convert(value.getString());
      if (date == DataTypes.NULL) {
        throw new ParserException(String.format("Expected a valid date string but got '%s'.", value.getString()));
      }
      expr = new DateLiteral((java.util.Date) date);
    } else if (token.isIdent()) {
      Ident ident = new Ident(tokens.next().getString());
      expr = parseFuncCallExpression(ident, null, tokens);
    } else if (token.isInt()) {
      tokens.next();
      expr = new LongLiteral(Long.valueOf(token.getString()));
    } else if (token.isFloat()) {
      tokens.next();
      expr = new DoubleLiteral(Double.valueOf(token.getString()));
    } else if (token.isString()) {
      tokens.next();
      expr = new StringLiteral(token.getString());
    } else if (token.isBoolean()) {
      tokens.next();
      expr = new BooleanLiteral(Boolean.valueOf(token.getString()));
    } else {
      throw new ParserException("Expected literal but got " + token.getString() + ".");
    }
    if (tokens.peek().is(Tokens.DOT)) {
      tokens.next();
      Ident newIdent = parseIdent(tokens);
      return parseFuncCallExpression(newIdent, expr, tokens);
    } else {
      return expr;
    }
  }

  private Expression parseCase(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.CASE);
    ImmutableList.Builder<Expression> conditions = ImmutableList.builder();
    ImmutableList.Builder<Expression> values = ImmutableList.builder();
    while (tokens.peek().is(Tokens.WHEN)) {
      tokens.next();
      conditions.add(parseExpression(tokens));
      assertNext(tokens, Tokens.THEN);
      values.add(parseExpression(tokens));
    }
    assertNext(tokens, Tokens.ELSE);
    Expression elseValue = parseExpression(tokens);
    assertNext(tokens, Tokens.END);
    return new CaseExpression(conditions.build(), values.build(), elseValue);
  }

  private FuncCallExpression parseFuncCallExpression(Ident ident, Expression parent, PeekingIterator<Token> tokens)
      throws ParserException {
    FuncCallExpression expr;
    if (tokens.peek().is(Tokens.PARENT_OPEN)) {
      tokens.next();
      if (ident.is(Tokens.COUNT) && tokens.peek().is(Tokens.DISTINCT)) {
        tokens.next();
        ImmutableList<Expression> argExprs = parseExprs(tokens, false, String.valueOf(Tokens.PARENT_CLOSED));
        tokens.next();
        expr = new FuncCallExpression(new Ident("count$distinct"), parent, argExprs);
      } else {
        ImmutableList<Expression> argExprs = parseExprs(tokens, false, String.valueOf(Tokens.PARENT_CLOSED));
        tokens.next();
        expr = new FuncCallExpression(ident, parent, argExprs);
      }
    } else {
      expr = new FuncCallExpression(ident, parent, ImmutableList.of());
    }
    if (tokens.peek().is(Tokens.DOT)) {
      tokens.next();
      Ident newIdent = parseIdent(tokens);
      return parseFuncCallExpression(newIdent, expr, tokens);
    } else {
      return expr;
    }
  }

  private ImmutableList<Expression> parseExprs(
      PeekingIterator<Token> tokens,
      boolean allowAlias,
      String... terminators) throws ParserException {
    ImmutableList.Builder<Expression> exprs = ImmutableList.builder();
    while (true) {
      if (ImmutableSet.copyOf(terminators).contains(tokens.peek().getString())) {
        break;
      }
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
    if (token.isIdent()) {
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

  public Expression parseExpression(PeekingIterator<Token> tokens) throws ParserException {
    return parseExpression(tokens, 0);
  }

  private Expression parseExpression(PeekingIterator<Token> tokens, int precedence) throws ParserException {
    if (precedence >= Tokens.BINARY_OPERATORS_PRECEDENCE.size()) {
      return parsePrimary(tokens);
    } else if (tokens.peek().is(Tokens.NOT)
        && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.NOT)) {
      return new UnaryExpression(tokens.next(), parseExpression(tokens, precedence + 1));
    } else {
      Expression primary = parseExpression(tokens, precedence + 1);
      if (!tokens.hasNext()) {
        return primary;
      }
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
      left = new BinaryExpression(token, left, right);
    }
  }

  private void assertNext(PeekingIterator<Token> tokens, String... values) throws ParserException {
    String next = tokens.next().getString();
    if (!ImmutableSet.copyOf(values).contains(next)) {
      throw new ParserException("Expected '" + join(values) + "' but got '" + next + "'.");
    }
  }

  private void assertPeek(PeekingIterator<Token> tokens, String... values) throws ParserException {
    String next = tokens.peek().getString();
    if (!ImmutableSet.copyOf(values).contains(next)) {
      throw new ParserException("Expected '" + join(values) + "' but got '" + next + "'.");
    }
  }

  private String join(String[] values) {
    return Stream.of(values).collect(Collectors.joining(", "));
  }
}

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
package com.cosyan.db.lang.sql;

import java.util.ArrayList;
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
import com.cosyan.db.lang.expr.Statements.MetaStatement;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.expr.TableDefinition.ColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ConstraintDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ForeignKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.PrimaryKeyDefinition;
import com.cosyan.db.lang.expr.TableDefinition.RuleDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableColumnDefinition;
import com.cosyan.db.lang.expr.TableDefinition.TableWithOwnerDefinition;
import com.cosyan.db.lang.expr.TableDefinition.ViewDefinition;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableAddColumn;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableAlterColumn;
import com.cosyan.db.lang.sql.AlterStatementColumns.AlterTableDropColumn;
import com.cosyan.db.lang.sql.AlterStatementConstraints.AlterTableAddForeignKey;
import com.cosyan.db.lang.sql.AlterStatementConstraints.AlterTableAddRule;
import com.cosyan.db.lang.sql.AlterStatementConstraints.AlterTableDropConstraint;
import com.cosyan.db.lang.sql.AlterStatementConstraints.AlterViewAddRule;
import com.cosyan.db.lang.sql.AlterStatementRefs.AlterTableAddView;
import com.cosyan.db.lang.sql.AlterStatementRefs.AlterTableDropView;
import com.cosyan.db.lang.sql.CSVStatements.CSVExport;
import com.cosyan.db.lang.sql.CSVStatements.CSVImport;
import com.cosyan.db.lang.sql.CreateStatement.CreateIndex;
import com.cosyan.db.lang.sql.CreateStatement.CreateTable;
import com.cosyan.db.lang.sql.CreateStatement.CreateView;
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
import com.cosyan.db.lang.sql.SelectStatement.TableRefChain;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.sql.UpdateStatement.SetExpression;
import com.cosyan.db.lang.sql.UpdateStatement.Update;
import com.cosyan.db.lang.sql.Users.CreateUser;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.DateFunctions;
import com.cosyan.db.model.Ident;
import com.cosyan.db.session.IParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.PeekingIterator;

public class Parser implements IParser {

  public boolean isMeta(PeekingIterator<Token> tokens) {
    if (tokens.peek().is(Tokens.CREATE) || tokens.peek().is(Tokens.ALTER)
        || tokens.peek().is(Tokens.DROP) || tokens.peek().is(Tokens.GRANT)) {
      return true;
    }
    return false;
  }

  public ImmutableList<Statement> parseStatements(PeekingIterator<Token> tokens)
      throws ParserException {
    ImmutableList.Builder<Statement> roots = ImmutableList.builder();
    while (tokens.hasNext()) {
      Statement root = parseStatement(tokens);
      roots.add(root);
      assertNext(tokens, String.valueOf(Tokens.COMMA_COLON));
    }
    return roots.build();
  }

  public MetaStatement parseMetaStatement(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.peek();
    if (token.is(Tokens.CREATE)) {
      return parseCreate(tokens);
    } else if (token.is(Tokens.DROP)) {
      return parseDrop(tokens);
    } else if (token.is(Tokens.ALTER)) {
      return parseAlter(tokens);
    } else if (token.is(Tokens.GRANT)) {
      return parseGrant(tokens);
    }
    throw new ParserException("Syntax error, expected create, drop or alter.", token);
  }

  private Statement parseStatement(PeekingIterator<Token> tokens) throws ParserException {
    Token token = tokens.peek();
    if (token.is(Tokens.SELECT)) {
      return new SelectStatement(parseSelect(tokens));
    } else if (token.is(Tokens.INSERT)) {
      return parseInsert(tokens);
    } else if (token.is(Tokens.DELETE)) {
      return parseDelete(tokens);
    } else if (token.is(Tokens.UPDATE)) {
      return parseUpdate(tokens);
    } else if (token.is(Tokens.IMPORT)) {
      return parseImport(tokens);
    } else if (token.is(Tokens.EXPORT)) {
      return parseExport(tokens);
    } else if (token.is(Tokens.WAIT)) {
      return parseWait(tokens);
    }
    throw new ParserException("Syntax error, expected select, insert, delete or update.", token);
  }

  private Statement parseImport(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.IMPORT);
    assertNext(tokens, Tokens.FROM);
    if (tokens.peek().is(Tokens.CSV)) {
      tokens.next();
      StringLiteral fileName = (StringLiteral) parseLiteral(tokens);
      assertNext(tokens, Tokens.INTO);
      TableWithOwnerDefinition table = parseTableWithOwner(tokens);
      boolean withHeader = false;
      if (tokens.peek().is(Tokens.WITH)) {
        tokens.next();
        if (tokens.peek().is(Tokens.HEADER)) {
          tokens.next();
          withHeader = true;
        }
      }
      long commitAfterNRecords;
      if (tokens.peek().is(Tokens.COMMIT)) {
        tokens.next();
        commitAfterNRecords = parseLongLiteral(tokens).getValue();
      } else {
        commitAfterNRecords = 10000;
      }
      return new CSVImport(fileName, table, withHeader, commitAfterNRecords);
    } else {
      Token token = tokens.next();
      throw new ParserException(String.format("Invalid file format '%s'.", token), token);
    }
  }

  private Statement parseExport(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.EXPORT);
    assertNext(tokens, Tokens.INTO);
    if (tokens.peek().is(Tokens.CSV)) {
      tokens.next();
      StringLiteral fileName = (StringLiteral) parseLiteral(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Select select = parseSelect(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new CSVExport(fileName, select);
    } else {
      Token token = tokens.next();
      throw new ParserException(String.format("Invalid file format '%s'.", token), token);
    }
  }

  private Statement parseWait(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.WAIT);
    LongLiteral time = parseLongLiteral(tokens);
    Optional<String> tag = Optional.empty();
    if (!tokens.peek().is(Tokens.COMMA_COLON)) {
      tag = Optional.of(parseStringLiteral(tokens).getValue());
    }
    return new WaitStatement(time.getValue(), tag);
  }

  private Delete parseDelete(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.DELETE);
    assertNext(tokens, Tokens.FROM);
    TableWithOwnerDefinition table = parseTableWithOwner(tokens);
    assertNext(tokens, Tokens.WHERE);
    Expression where = parseExpression(tokens, 0);
    return new Delete(table, where);
  }

  private InsertInto parseInsert(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.INSERT);
    assertNext(tokens, Tokens.INTO);
    TableWithOwnerDefinition table = parseTableWithOwner(tokens);
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
    return new InsertInto(table, columns, valuess.build());
  }

  private Literal parseLiteral(PeekingIterator<Token> tokens) throws ParserException {
    Expression expr = parsePrimary(tokens);
    if (!(expr instanceof Literal)) {
      throw new ParserException("Expected literal but got '" + expr + "'.", expr.loc());
    }
    return (Literal) expr;
  }

  private LongLiteral parseLongLiteral(PeekingIterator<Token> tokens) throws ParserException {
    Expression expr = parsePrimary(tokens);
    if (!(expr instanceof LongLiteral)) {
      throw new ParserException("Expected integer literal but got '" + expr + "'.", expr.loc());
    }
    return (LongLiteral) expr;
  }

  private StringLiteral parseStringLiteral(PeekingIterator<Token> tokens) throws ParserException {
    Expression expr = parsePrimary(tokens);
    if (!(expr instanceof StringLiteral)) {
      throw new ParserException("Expected varchar literal but got '" + expr + "'.", expr.loc());
    }
    return (StringLiteral) expr;
  }

  private Update parseUpdate(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.UPDATE);
    TableWithOwnerDefinition table = parseTableWithOwner(tokens);
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
    return new Update(table, updates.build(), where);
  }

  private MetaStatement parseCreate(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.CREATE);
    MaterializedTable.Type type = MaterializedTable.Type.LOG;
    if (tokens.peek().is(Tokens.LOG)) {
      tokens.next();
      type = MaterializedTable.Type.LOG;
    } else if (tokens.peek().is(Tokens.LOOKUP)) {
      tokens.next();
      type = MaterializedTable.Type.LOOKUP;
    }
    assertPeek(tokens, Tokens.TABLE, Tokens.VIEW, Tokens.INDEX, Tokens.USER);
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
      return new CreateTable(ident, type, columns.build(), constraints.build(), partitioning);
    } else if (tokens.peek().is(Tokens.INDEX)) {
      assertNext(tokens, Tokens.INDEX);
      return new CreateIndex(parseTableColumn(tokens));
    } else if (tokens.peek().is(Tokens.USER)) {
      assertNext(tokens, Tokens.USER);
      Ident username = parseIdent(tokens);
      assertNext(tokens, Tokens.IDENTIFIED);
      assertNext(tokens, Tokens.BY);
      StringLiteral password = parseStringLiteral(tokens);
      return new CreateUser(username, password);
    } else {
      assertNext(tokens, Tokens.VIEW);
      return new CreateView(parseView(tokens));
    }
  }

  private ViewDefinition parseView(PeekingIterator<Token> tokens) throws ParserException {
    Ident ident = parseIdent(tokens);
    if (tokens.peek().is(Tokens.AS)) {
      tokens.next();
      return new ViewDefinition(ident, parseViewSelect(tokens));
    } else {
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Select select = parseViewSelect(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new ViewDefinition(ident, select);
    }
  }

  private MetaStatement parseDrop(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.DROP);
    assertPeek(tokens, Tokens.TABLE, Tokens.INDEX);
    if (tokens.peek().is(Tokens.TABLE)) {
      tokens.next();
      return new DropTable(parseTableWithOwner(tokens));
    } else {
      assertNext(tokens, Tokens.INDEX);
      TableColumnDefinition tableColumn = parseTableColumn(tokens);
      return new DropIndex(tableColumn);
    }
  }

  private TableColumnDefinition parseTableColumn(PeekingIterator<Token> tokens) throws ParserException {
    Ident ident1 = parseIdent(tokens);
    assertNext(tokens, String.valueOf(Tokens.DOT));
    Ident ident2 = parseIdent(tokens);
    TableWithOwnerDefinition table;
    Ident column;
    if (tokens.peek().is(Tokens.DOT)) {
      tokens.next();
      Ident ident3 = parseIdent(tokens);
      table = new TableWithOwnerDefinition(Optional.of(ident1), ident2);
      column = ident3;
    } else {
      table = new TableWithOwnerDefinition(Optional.empty(), ident1);
      column = ident2;
    }
    return new TableColumnDefinition(table, column);
  }

  private MetaStatement parseAlter(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.ALTER);
    if (tokens.peek().is(Tokens.VIEW)) {
      assertNext(tokens, Tokens.VIEW);
      TableWithOwnerDefinition view = parseTableWithOwner(tokens);
      assertNext(tokens, Tokens.ADD);
      ConstraintDefinition constraint = parseConstraint(tokens);
      if (constraint instanceof RuleDefinition) {
        return new AlterViewAddRule(view, (RuleDefinition) constraint);
      } else {
        throw new ParserException(
            String.format("Expected rule definition, got '%s'.", constraint),
            constraint.getName().getLoc());
      }
    } else {
      assertNext(tokens, Tokens.TABLE);
      TableWithOwnerDefinition table = parseTableWithOwner(tokens);
      if (tokens.peek().is(Tokens.ADD)) {
        tokens.next();
        if (tokens.peek().is(Tokens.VIEW)) {
          assertNext(tokens, Tokens.VIEW);
          return new AlterTableAddView(table, parseView(tokens));
        } else if (tokens.peek().is(Tokens.CONSTRAINT)) {
          ConstraintDefinition constraint = parseConstraint(tokens);
          if (constraint instanceof ForeignKeyDefinition) {
            return new AlterTableAddForeignKey(table, (ForeignKeyDefinition) constraint);
          } else if (constraint instanceof RuleDefinition) {
            return new AlterTableAddRule(table, (RuleDefinition) constraint);
          } else {
            throw new ParserException(
                String.format("Expected foreign key or rule definition, got '%s'.", constraint),
                constraint.getName().getLoc());
          }
        } else {
          ColumnDefinition column = parseColumnDefinition(tokens);
          return new AlterTableAddColumn(table, column);
        }
      } else if (tokens.peek().is(Tokens.DROP)) {
        tokens.next();
        if (tokens.peek().is(Tokens.CONSTRAINT)) {
          tokens.next();
          Ident constraint = parseIdent(tokens);
          return new AlterTableDropConstraint(table, constraint);
        } else if (tokens.peek().is(Tokens.VIEW)) {
          tokens.next();
          Ident constraint = parseIdent(tokens);
          return new AlterTableDropView(table, constraint);
        } else {
          Ident columnName = parseIdent(tokens);
          return new AlterTableDropColumn(table, columnName);
        }
      } else if (tokens.peek().is(Tokens.ALTER) || tokens.peek().is(Tokens.MODIFY)) {
        tokens.next();
        ColumnDefinition column = parseColumnDefinition(tokens);
        return new AlterTableAlterColumn(table, column);
      } else {
        Token token = tokens.peek();
        throw new ParserException("Unsupported alter operation '" + token + "'.", token);
      }
    }
  }

  private TableWithOwnerDefinition parseTableWithOwner(PeekingIterator<Token> tokens) throws ParserException {
    return parseTableWithOwner(tokens, /* acceptAsterisk= */false);
  }

  private TableWithOwnerDefinition parseTableWithOwner(PeekingIterator<Token> tokens, boolean acceptAsterisk) throws ParserException {
    Ident ident = parseIdent(tokens, acceptAsterisk);
    if (tokens.peek().is(Tokens.DOT)) {
      tokens.next();
      Ident table = parseIdent(tokens, acceptAsterisk);
      return new TableWithOwnerDefinition(Optional.of(ident), table);
    } else {
      return new TableWithOwnerDefinition(Optional.empty(), ident);
    }
  }

  private MetaStatement parseGrant(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.GRANT);
    assertPeek(tokens, Tokens.SELECT, Tokens.INSERT, Tokens.UPDATE, Tokens.DELETE, Tokens.ALL);
    Ident method = parseIdent(tokens);
    assertNext(tokens, Tokens.ON);
    TableWithOwnerDefinition table = parseTableWithOwner(tokens, /* acceptAsterisk= */true);
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

  private ConstraintDefinition parseConstraint(PeekingIterator<Token> tokens)
      throws ParserException {
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
      return new RuleDefinition(ident, expr, nullIsTrue);
    } else if (tokens.peek().is(Tokens.PRIMARY)) {
      tokens.next();
      assertNext(tokens, Tokens.KEY);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Ident column = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      return new PrimaryKeyDefinition(ident, column);
    } else if (tokens.peek().is(Tokens.FOREIGN)) {
      tokens.next();
      assertNext(tokens, Tokens.KEY);
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      Ident column = parseIdent(tokens);
      assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      assertNext(tokens, Tokens.REFERENCES);
      TableWithOwnerDefinition refTable = parseTableWithOwner(tokens);
      Optional<Ident> refColumn = Optional.empty();
      if (tokens.peek().is(Tokens.PARENT_OPEN)) {
        tokens.next();
        refColumn = Optional.of(parseIdent(tokens));
        assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      }
      if (tokens.peek().is(Tokens.REVERSE)) {
        tokens.next();
        Ident reverseIdent = parseIdent(tokens);
        return new ForeignKeyDefinition(ident, reverseIdent, column, refTable, refColumn);
      } else {
        return new ForeignKeyDefinition(ident, ident.map(i -> "rev_" + i), column, refTable,
            refColumn);
      }
    } else {
      Token token = tokens.peek();
      throw new ParserException("Unsupported constraint '" + token + "'.", token);
    }
  }

  private ColumnDefinition parseColumnDefinition(PeekingIterator<Token> tokens)
      throws ParserException {
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
    return new ColumnDefinition(ident, type, nullable, unique, immutable);
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
      String format = null;
      if (tokens.peek().is(Tokens.PARENT_OPEN)) {
        tokens.next();
        format = parseStringLiteral(tokens).getValue();
        assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
      }
      if (format == null) {
        return DataTypes.dateType();
      } else {
        return DataTypes.dateType(format);
      }
    } else if (token.is(Tokens.BOOLEAN)) {
      return DataTypes.BoolType;
    } else if (token.is(Tokens.ID)) {
      return DataTypes.IDType;
    } else if (token.is(Tokens.ENUM)) {
      assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
      ArrayList<String> list = new ArrayList<>();
      while (!tokens.peek().is(Tokens.PARENT_CLOSED)) {
        list.add(parseStringLiteral(tokens).getValue());
        if (tokens.peek().is(Tokens.COMMA)) {
          tokens.next();
        }
      }
      tokens.next();
      return DataTypes.enumType(list);
    }
    throw new ParserException("Unknown data type '" + token + "'.", token);
  }

  public Select parseSelect(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.SELECT);
    boolean distinct = false;
    if (tokens.peek().is(Tokens.DISTINCT)) {
      tokens.next();
      distinct = true;
    }
    ImmutableList<Expression> columns = parseExprs(tokens, true, Tokens.FROM);
    tokens.next();
    Table table = parseTable(tokens);
    if (tokens.peek().is(Tokens.INNER) || tokens.peek().is(Tokens.LEFT)
        || tokens.peek().is(Tokens.RIGHT)) {
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
      groupBy = Optional.of(parseExprs(tokens, true, String.valueOf(Tokens.COMMA_COLON),
          String.valueOf(Tokens.PARENT_CLOSED), Tokens.HAVING, Tokens.ORDER, Tokens.LIMIT));
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
      orderBy = Optional.of(parseExprs(tokens, true, String.valueOf(Tokens.COMMA_COLON),
          String.valueOf(Tokens.PARENT_CLOSED), Tokens.LIMIT));
    } else {
      orderBy = Optional.empty();
    }
    Optional<Long> limit;
    if (tokens.peek().is(Tokens.LIMIT)) {
      tokens.next();
      limit = Optional.of(parseLongLiteral(tokens).getValue());
    } else {
      limit = Optional.empty();
    }
    assertPeek(tokens, String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED));
    return new Select(columns, table, where, groupBy, having, orderBy, distinct, limit);
  }

  public Select parseViewSelect(PeekingIterator<Token> tokens) throws ParserException {
    assertNext(tokens, Tokens.SELECT);
    ImmutableList<Expression> columns = parseExprs(tokens, true, Tokens.FROM);
    tokens.next();
    Table table = parseTable(tokens);
    Optional<Expression> where;
    if (tokens.peek().is(Tokens.WHERE)) {
      tokens.next();
      where = Optional.of(parseExpression(tokens, 0));
    } else {
      where = Optional.empty();
    }
    Optional<ImmutableList<Expression>> groupBy;
    if (tokens.peek().is(Tokens.GROUP)) {
      tokens.next();
      assertNext(tokens, Tokens.BY);
      groupBy = Optional.of(parseExprs(tokens, true, String.valueOf(Tokens.COMMA_COLON),
          String.valueOf(Tokens.PARENT_CLOSED), Tokens.HAVING, Tokens.ORDER, Tokens.LIMIT));
    } else {
      groupBy = Optional.empty();
    }
    assertPeek(tokens, String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED));
    return new Select(columns, table, where, groupBy, Optional.empty(), Optional.empty(), /* distinct= */false, Optional.empty());
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
      Optional<ImmutableList<Ident>> exclude = Optional.empty();
      if (tokens.peek().is(Tokens.MINUS)) {
        tokens.next();
        assertNext(tokens, String.valueOf(Tokens.PARENT_OPEN));
        ImmutableList.Builder<Ident> idents = ImmutableList.builder();
        while (true) {
          idents.add(parseIdent(tokens));
          if (tokens.peek().is(Tokens.COMMA)) {
            tokens.next();
          } else {
            assertNext(tokens, String.valueOf(Tokens.PARENT_CLOSED));
            break;
          }
        }
        exclude = Optional.of(idents.build());
      }
      expr = new AsteriskExpression(token.getLoc(), exclude);
    } else if (token.is(Tokens.NULL)) {
      tokens.next();
      expr = new NullLiteral(token.getLoc());
    } else if (token.is(Tokens.CASE)) {
      expr = parseCase(tokens);
    } else if (token.is(Tokens.DT)) {
      tokens.next();
      Token value = tokens.next();
      Object date = DateFunctions.convert(value.getString());
      if (date == null) {
        throw new ParserException(
            String.format("Expected a valid date string but got '%s'.", value), value);
      }
      expr = new DateLiteral((java.util.Date) date, value.getLoc());
    } else if (token.isIdent()) {
      Ident ident = parseIdent(tokens);
      expr = parseFuncCallExpression(ident, null, tokens);
    } else if (token.isInt()) {
      tokens.next();
      expr = new LongLiteral(Long.valueOf(token.getString()), token.getLoc());
    } else if (token.isFloat()) {
      tokens.next();
      expr = new DoubleLiteral(Double.valueOf(token.getString()), token.getLoc());
    } else if (token.isString()) {
      tokens.next();
      expr = new StringLiteral(token.getString(), token.getLoc());
    } else if (token.isBoolean()) {
      tokens.next();
      expr = new BooleanLiteral(Boolean.valueOf(token.getString()), token.getLoc());
    } else {
      throw new ParserException("Expected literal but got " + token + ".", token);
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
    Token token = tokens.peek();
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
    return new CaseExpression(conditions.build(), values.build(), elseValue, token.getLoc());
  }

  private FuncCallExpression parseFuncCallExpression(Ident ident, Expression parent,
      PeekingIterator<Token> tokens) throws ParserException {
    FuncCallExpression expr;
    if (tokens.peek().is(Tokens.PARENT_OPEN)) {
      tokens.next();
      if (ident.is(Tokens.COUNT) && tokens.peek().is(Tokens.DISTINCT)) {
        tokens.next();
        ImmutableList<Expression> argExprs = parseExprs(tokens, false,
            String.valueOf(Tokens.PARENT_CLOSED));
        tokens.next();
        expr = new FuncCallExpression(new Ident("count$distinct", ident.getLoc()), parent,
            argExprs);
      } else {
        ImmutableList<Expression> argExprs = parseExprs(tokens, false,
            String.valueOf(Tokens.PARENT_CLOSED));
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

  @Override
  public ImmutableList<Expression> parseExpressions(PeekingIterator<Token> tokens) throws ParserException {
    return parseExprs(tokens, true, String.valueOf(Tokens.COMMA_COLON), String.valueOf(Tokens.PARENT_CLOSED));
  }

  private ImmutableList<Expression> parseExprs(PeekingIterator<Token> tokens, boolean allowAlias,
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
    return parseIdent(tokens, /* acceptAsterisk= */false);
  }

  private Ident parseIdent(PeekingIterator<Token> tokens, boolean acceptAsterisk) throws ParserException {
    Token token = tokens.next();
    if (token.isIdent()) {
      return new Ident(token.getString(), token.getLoc());
    } else if (token.is(Tokens.ASTERISK) && acceptAsterisk) {
      return new Ident("*", token.getLoc());
    } else {
      throw new ParserException("Expected identifier but got " + token + ".", token);
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
      Ident ident = parseIdent(tokens);
      if (tokens.peek().is(Tokens.DOT)) {
        tokens.next();
        table = new TableRefChain(ident, parseTable(tokens));
      } else {
        table = new TableRef(ident);
      }
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

  private Expression parseExpression(PeekingIterator<Token> tokens, int precedence)
      throws ParserException {
    if (precedence >= Tokens.BINARY_OPERATORS_PRECEDENCE.size()) {
      return parsePrimary(tokens);
    } else if (tokens.peek().is(Tokens.NOT)
        && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.NOT)) {
      Token token = tokens.next();
      return new UnaryExpression(UnaryExpression.Type.NOT, parseExpression(tokens, precedence + 1),
          token.getLoc());
    } else {
      Expression primary = parseExpression(tokens, precedence + 1);
      if (!tokens.hasNext()) {
        return primary;
      }
      Token token = tokens.peek();
      if (token.is(Tokens.ASC)
          && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.ASC)) {
        tokens.next();
        return new UnaryExpression(UnaryExpression.Type.ASC, primary, token.getLoc());
      } else if (token.is(Tokens.DESC)
          && Tokens.BINARY_OPERATORS_PRECEDENCE.get(precedence).contains(Tokens.DESC)) {
        tokens.next();
        return new UnaryExpression(UnaryExpression.Type.DESC, primary, token.getLoc());
      } else if (token.is(Tokens.IS)
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
            not ? UnaryExpression.Type.IS_NOT_NULL : UnaryExpression.Type.IS_NULL, primary,
            token.getLoc());
      } else {
        return parseBinaryExpression(primary, tokens, precedence);
      }
    }
  }

  private Expression parseBinaryExpression(Expression left, PeekingIterator<Token> tokens,
      int precedence) throws ParserException {
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

  private void assertNext(PeekingIterator<Token> tokens, String value) throws ParserException {
    Token next = tokens.next();
    if (!value.equals(next.getString())) {
      throw new ParserException("Expected '" + value + "' but got '" + next + "'.", next);
    }
  }

  private void assertPeek(PeekingIterator<Token> tokens, String... values) throws ParserException {
    Token next = tokens.peek();
    if (!ImmutableSet.copyOf(values).contains(next.getString())) {
      throw new ParserException("Expected '" + join(values) + "' but got '" + next + "'.", next);
    }
  }

  private String join(String[] values) {
    return Stream.of(values).collect(Collectors.joining(", "));
  }
}

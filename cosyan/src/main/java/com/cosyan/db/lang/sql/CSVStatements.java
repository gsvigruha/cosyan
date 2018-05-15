package com.cosyan.db.lang.sql;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.lang.transaction.Result.StatementResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CSVStatements {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CSVImport extends Node implements Statement {
    private final StringLiteral fileName;
    private final Ident table;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void cancel() {
      // TODO Auto-generated method stub

    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CSVExport extends Node implements Statement {
    private final StringLiteral fileName;
    private final Select select;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private ExposedTableMeta tableMeta;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = select.compileTable(metaRepo);
      return tableMeta.readResources();
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      BufferedWriter writer = new BufferedWriter(new FileWriter(fileName.getValue()));
      long lines = 0L;
      try {
        IterableTableReader reader = tableMeta.reader(resources);
        try {
          writer.write(QueryResult.prettyPrintHeader(tableMeta.columnNames()));
          Object[] values = null;
          while ((values = reader.next()) != null && !cancelled.get()) {
            writer.write(QueryResult.prettyPrint(values));
            lines++;
          }
        } finally {
          reader.close();
        }
      } finally {
        writer.close();
      }
      return new StatementResult(lines);
    }

    @Override
    public void cancel() {
      cancelled.set(true);
    }
  }
}

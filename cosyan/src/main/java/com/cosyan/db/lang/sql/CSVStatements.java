package com.cosyan.db.lang.sql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.lang.transaction.Result.StatementResult;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CSVStatements {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CSVImport extends Statement {
    private final StringLiteral fileName;
    private final Ident table;
    private final boolean withHeader;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private MaterializedTable tableMeta;
    private ImmutableList<BasicColumn> columns;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = metaRepo.table(table);
      columns = tableMeta.columns().values().asList();
      return MetaResources.insertIntoTable(tableMeta)
          .merge(tableMeta.ruleDependenciesReadResources())
          .merge(tableMeta.reverseRuleDependenciesReadResources());
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      TableWriter writer = resources.writer(tableMeta.tableName());
      BufferedReader reader = new BufferedReader(new FileReader(fileName.getValue()));
      CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator("\n");
      if (withHeader) {
        format = format.withFirstRecordAsHeader();
      }
      int lines = 0;
      CSVParser csvParser = new CSVParser(reader, format);
      try {
        int numCols = csvParser.getHeaderMap().size();
        for (CSVRecord csvRecord : csvParser) {
          Object[] values = new Object[numCols];
          for (int i = 0; i < numCols; i++) {
            values[i] = columns.get(i).getType().fromString(csvRecord.get(i));
          }
          writer.insert(resources, values, /* checkReferencingRules= */true);
          lines++;
          if (cancelled.get()) {
            break;
          }
        }
      } finally {
        csvParser.close();
      }
      tableMeta.insert(lines);
      return new StatementResult(lines);
    }

    @Override
    public void cancel() {
      cancelled.set(true);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class CSVExport extends Statement {
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
      CSVFormat format = CSVFormat.DEFAULT
          .withRecordSeparator("\n")
          .withHeader(tableMeta.columnNames().toArray(new String[] {}));
      CSVPrinter csvPrinter = new CSVPrinter(writer, format);
      try {
        IterableTableReader reader = tableMeta.reader(resources);
        try {
          Object[] values = null;
          while ((values = reader.next()) != null && !cancelled.get()) {
            csvPrinter.printRecord(QueryResult.prettyPrintToList(values));
            lines++;
          }
        } finally {
          reader.close();
        }
      } finally {
        csvPrinter.close();
      }
      return new StatementResult(lines);
    }

    @Override
    public void cancel() {
      cancelled.set(true);
    }
  }
}

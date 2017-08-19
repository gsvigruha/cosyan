package com.cosyan.db.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Optional;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.Compiler;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.SyntaxTree;
import com.google.common.collect.ImmutableMap;

public class CSVConverterTest {

  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Compiler compiler;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp");
    metaRepo = new MetaRepo(new Config(props));
    parser = new Parser();
    compiler = new Compiler(metaRepo);

  }

  @Test
  public void testReadFromCSV() throws Exception {
    CSVConverter csvConverter = new CSVConverter(metaRepo);
    csvConverter.convertWithSchema(
        "table",
        "target/test-classes/simple.csv",
        ImmutableMap.of("a", DataTypes.StringType, "b", DataTypes.LongType, "c", DataTypes.DoubleType),
        Optional.empty(),
        Optional.empty());
    SyntaxTree tree = parser.parse("select * from table;");
    ExposedTableMeta ExposedTableMeta = compiler.query(tree);
    ExposedTableReader reader = ExposedTableMeta.reader();
    assertEquals(ImmutableMap.of("a", "abc", "b", 1L, "c", 1.0), reader.readColumns());
  }
}

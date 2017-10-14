package com.cosyan.db.tools;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.cosyan.db.io.Serializer;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class CSVConverter {

  private final MetaRepo metaRepo;

  public void convertWithSchema(
      String tableName,
      String csvFile,
      ImmutableMap<String, BasicColumn> columns,
      Optional<Character> quoteChar,
      Optional<Character> separatorChar) throws IOException, ModelException, ParseException {

    CSVFormat format = CSVFormat.DEFAULT
        .withFirstRecordAsHeader()
        .withRecordSeparator("\n")
        .withDelimiter(separatorChar.orElse(','))
        .withQuote(quoteChar.orElse('"'));
    CSVParser parser = CSVParser.parse(new File(csvFile), Charset.defaultCharset(), format);
    OutputStream os = metaRepo.openForWrite(tableName, columns);
    for (CSVRecord csvRecord : parser) {
      Object[] values = new Object[columns.size()];
      int i = 0;
      for (Map.Entry<String, BasicColumn> entry : columns.entrySet()) {
        String stringValue = csvRecord.get(entry.getKey());
        Object value;
        DataType<?> dataType = entry.getValue().getType();
        if (dataType == DataTypes.BoolType) {
          value = Boolean.parseBoolean(stringValue);
        } else if (dataType == DataTypes.LongType) {
          value = Long.parseLong(stringValue);
        } else if (dataType == DataTypes.DoubleType) {
          value = Double.parseDouble(stringValue);
        } else if (dataType == DataTypes.StringType) {
          value = stringValue;
        } else if (dataType == DataTypes.DateType) {
          value = DateFormat.getInstance().parse(stringValue);
        } else {
          throw new UnsupportedOperationException();
        }
        values[i++] = value;
      }
      os.write(Serializer.serialize(values, columns.values().asList()));
    }
    parser.close();
    os.close();
  }
}

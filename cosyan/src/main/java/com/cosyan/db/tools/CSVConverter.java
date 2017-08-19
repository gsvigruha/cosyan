package com.cosyan.db.tools;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class CSVConverter {

  private final MetaRepo metaRepo;

  public void convertWithSchema(
      String tableName,
      String csvFile,
      ImmutableMap<String, DataType<?>> columns,
      Optional<Character> quoteChar,
      Optional<Character> separatorChar) throws IOException, ModelException, ParseException {

    CSVFormat format = CSVFormat.DEFAULT
        .withFirstRecordAsHeader()
        .withRecordSeparator("\n")
        .withDelimiter(separatorChar.orElse(','))
        .withQuote(quoteChar.orElse('"'));
    CSVParser parser = CSVParser.parse(new File(csvFile), Charset.defaultCharset(), format);
    DataOutputStream output = new DataOutputStream(metaRepo.openForWrite(tableName, columns));
    for (CSVRecord csvRecord : parser) {
      for (Map.Entry<String, DataType<?>> entry : columns.entrySet()) {
        String stringValue = csvRecord.get(entry.getKey());
        Object value;
        DataType<?> dataType = entry.getValue();
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
        TableWriter.writeColumn(value, dataType, output);
      }
    }
    parser.close();
    output.close();
  }
}

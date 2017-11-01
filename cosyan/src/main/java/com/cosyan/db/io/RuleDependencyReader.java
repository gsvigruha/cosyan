package com.cosyan.db.io;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Dependencies.ColumnReverseRuleDependencies;
import com.cosyan.db.model.Dependencies.ReverseRuleDependency;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.SourceValues.ReferencingSourceValues;
import com.cosyan.db.transaction.Resources;

public class RuleDependencyReader {

  private final Resources resources;
  private final ColumnReverseRuleDependencies reverseRules;
  private Object[] sourceValues;

  public RuleDependencyReader(Resources resources, ColumnReverseRuleDependencies reverseRules, Object[] sourceValues) {
    this.resources = resources;
    this.reverseRules = reverseRules;
    this.sourceValues = sourceValues;
  }

  public void checkReferencingRules()
      throws IOException, RuleException {
    HashMap<String, Object[]> referencedValues = new HashMap<>();
    for (Map<String, ReverseRuleDependency> values : reverseRules.getColumnDeps().values()) {
      checkReferencingRules(values.values(), sourceValues, referencedValues, "", "");
    }
  }

  private void checkReferencingRules(Collection<ReverseRuleDependency> collection, Object[] parentValues,
      HashMap<String, Object[]> referencedValues, String keyPrefix, String reverseKeySuffix)
      throws IOException, RuleException {
    for (ReverseRuleDependency dep : collection) {
      ReverseForeignKey reverseForeignKey = dep.getForeignKey();
      String key = keyPrefix.isEmpty() ? reverseForeignKey.getName() : keyPrefix + "." + reverseForeignKey.getName();
      String reverseKey = reverseKeySuffix.isEmpty() ? reverseForeignKey.getReverse().getName()
          : reverseForeignKey.getReverse().getName() + "." + reverseKeySuffix;

      Object[] newSourceValues = referencedValues.get(key);
      if (newSourceValues == null) {
        Object foreignKeyValue = parentValues[reverseForeignKey.getColumn().getIndex()];
        Ident table = new Ident(reverseForeignKey.getRefTable().tableName());
        SeekableTableReader reader = resources.createReader(table);
        IndexReader index = resources.getIndex(reverseForeignKey);
        long[] foreignKeyFilePointers = index.get(foreignKeyValue);
        for (long foreignKeyFilePointer : foreignKeyFilePointers) {
          reader.seek(foreignKeyFilePointer);
          newSourceValues = reader.read().toArray();
          HashMap<String, Object[]> newReferencedValues = new HashMap<>(referencedValues);
          newReferencedValues.put(keyPrefix, newSourceValues);
          for (BooleanRule rule : dep.getRules().values()) {
            HashMap<String, Object[]> initialValues = new HashMap<>();
            initialValues.put(reverseKey, sourceValues);
            if (!rule.check(ReferencingSourceValues.of(resources, newSourceValues, initialValues))) {
              throw new RuleException(String.format("Referencing constraint check %s.%s failed.",
                  dep.getForeignKey().getRefTable().tableName(), rule.getName()));
            }
          }
          checkReferencingRules(dep.getDeps().values(), newSourceValues, newReferencedValues, key, reverseKey);
        }
        reader.close();
      }
    }
  }
}

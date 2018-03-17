package com.cosyan.db.io;

import java.io.IOException;
import java.util.Collection;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Dependencies.ReverseRuleDependencies;
import com.cosyan.db.model.Dependencies.ReverseRuleDependency;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.transaction.Resources;

public class RuleDependencyReader {

  private final Resources resources;
  private final ReverseRuleDependencies reverseRules;

  public RuleDependencyReader(Resources resources, ReverseRuleDependencies reverseRules) {
    this.resources = resources;
    this.reverseRules = reverseRules;
  }

  public void checkReferencingRules(Record record)
      throws IOException, RuleException {
    checkReferencingRules(reverseRules.getDeps().values(), record);
  }

  private void checkReferencingRules(Collection<ReverseRuleDependency> collection, Record record)
      throws IOException, RuleException {
    for (ReverseRuleDependency dep : collection) {
      Ref ref = dep.getKey();

      Object[] newSourceValues = record.getValues();
      Object key = newSourceValues[ref.getColumn().getIndex()];
      IndexReader index = resources.getIndex(ref);
      long[] pointers = index.get(key);
      for (long pointer : pointers) {
        for (BooleanRule rule : dep.getRules().values()) {
          if (!rule.check(resources, pointer)) {
            throw new RuleException(
                String.format("Referencing constraint check %s.%s failed.",
                    rule.getTable().tableName(), rule.name()));
          }
        }
        SeekableTableReader reader = resources.reader(ref.getRefTable().tableName());
        checkReferencingRules(dep.getDeps().values(), reader.get(pointer));
      }
    }
  }
}

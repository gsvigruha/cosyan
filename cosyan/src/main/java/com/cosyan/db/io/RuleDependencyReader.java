package com.cosyan.db.io;

import java.io.IOException;
import java.util.Collection;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Dependencies.ColumnReverseRuleDependencies;
import com.cosyan.db.model.Dependencies.ReverseRuleDependency;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.transaction.Resources;

public class RuleDependencyReader {

  private final Resources resources;
  private final ColumnReverseRuleDependencies reverseRules;

  public RuleDependencyReader(Resources resources, ColumnReverseRuleDependencies reverseRules) {
    this.resources = resources;
    this.reverseRules = reverseRules;
  }

  public void checkReferencingRules(long fileIndex)
      throws IOException, RuleException {
    checkReferencingRules(reverseRules.allReverseRuleDepenencies(), fileIndex);
  }

  private void checkReferencingRules(Collection<ReverseRuleDependency> collection, long fileIndex)
      throws IOException, RuleException {
    for (ReverseRuleDependency dep : collection) {
      Ref reverseForeignKey = dep.getForeignKey();

      SeekableTableReader reader = resources.reader(reverseForeignKey.getTable().tableName());
      Object[] newSourceValues = reader.get(fileIndex).getValues();
      Object key = newSourceValues[reverseForeignKey.getColumn().getIndex()];
      IndexReader index = resources.getIndex(reverseForeignKey);
      long[] pointers = index.get(key);
      for (long pointer : pointers) {
        for (BooleanRule rule : dep.getRules().values()) {
          if (!rule.check(resources, pointer)) {
            throw new RuleException(
                String.format("Referencing constraint check %s.%s failed.", rule.getTable().tableName(), rule.name()));
          }
        }
        checkReferencingRules(dep.getDeps().values(), pointer);
      }
    }
  }
}

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
package com.cosyan.db.io;

import java.io.IOException;
import java.util.Collection;

import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependencies;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependency;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.Rule.BooleanViewRule;
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
      long[] pointers = ref.resolve(record.getValues(), resources);
      for (long pointer : pointers) {
        for (BooleanRule rule : dep.rules()) {
          if (!rule.check(resources, pointer)) {
            throw new RuleException(
                String.format("Referencing constraint check %s.%s failed.",
                    rule.getTable().tableName(), rule.name()));
          }
        }
        if (!dep.getDeps().isEmpty()) {
          SeekableTableReader reader = resources.reader(ref.getRefTable().fullName());
          checkReferencingRules(dep.getDeps().values(), reader.get(pointer));
        }
      }
      for (BooleanViewRule rule : dep.viewRules()) {
        if (!rule.checkRecord(resources, record.getValues())) {
          throw new RuleException(
              String.format("Referencing constraint check %s.%s failed.",
                  rule.getView().name(), rule.name()));
        }
      }
    }
  }
}

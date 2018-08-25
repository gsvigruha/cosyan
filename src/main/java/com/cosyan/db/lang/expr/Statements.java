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
package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaReader;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

public class Statements {
  public static abstract class Statement {

    public abstract MetaResources compile(MetaReader metaRepo, AuthToken authToken) throws ModelException;

    public abstract Result execute(Resources resources) throws RuleException, IOException;

    public abstract void cancel();
  }

  public static abstract class MetaStatement {

  }

  public static abstract class AlterStatement extends MetaStatement {

    public abstract MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, GrantException, IOException;

    public abstract Result executeData(MetaRepoExecutor metaRepo, Resources resources)
        throws RuleException, IOException;

    public abstract void cancel();
  }

  public static abstract class GlobalStatement extends MetaStatement {

    public abstract Result execute(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, GrantException, IOException;
  }
}

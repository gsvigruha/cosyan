package com.cosyan.db.meta;

import java.io.IOException;

import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.model.BasicColumn;

public interface MetaRepoExecutor {

  IndexWriter registerIndex(MaterializedTable meta, BasicColumn basicColumn) throws IOException;

  void syncMeta(MaterializedTable tableMeta);

  int numRefs();

}

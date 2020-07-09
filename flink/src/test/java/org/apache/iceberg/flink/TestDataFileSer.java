package org.apache.iceberg.flink;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by yexianxun@corp.netease.com on 2020/7/9.
 */
public class TestDataFileSer {
  private static final Logger LOG = LoggerFactory.getLogger(TestDataFileSer.class);
  @Test
  public void testSet() {

    String location = "/Users/yexianxun/dev/iceberg-data";
    Table table = new HadoopTables().load(location);
    Assert.assertNotNull(table);
    DataFile dataFile = DataFiles.fromManifest(table.currentSnapshot().manifests().get(0));
    Set set = new HashSet();
    set.add(dataFile);
    Assert.assertEquals(1, set.size());


    CloseableIterable<FileScanTask> iterable = table.newScan().planFiles();
    Set<DataFile> currentDataFiles = new HashSet<>();

    iterable.forEach(f -> currentDataFiles.add(f.file()));
  }
}

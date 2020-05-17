/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.KerberosLoginUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.security.auth.Subject;

import static junit.framework.TestCase.assertNotNull;

public class TestFlinkIcebergSinkKerberos extends AbstractTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tableLocation;

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
  }

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "word", Types.StringType.get()),
      Types.NestedField.optional(2, "count", Types.IntegerType.get())
  );

  private Table createTestIcebergTable() {
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();
    Tables table = new HadoopTables();
    return table.create(SCHEMA, spec, tableLocation);
  }

  @Test
  public void testNonHadoopACC() throws PrivilegedActionException {
    Subject nonHadoopSubject = new Subject();
    Subject.doAs(nonHadoopSubject, (PrivilegedExceptionAction<Void>) () -> {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      assertNotNull(ugi);
      return null;
    });
  }

  @Test
  public void testKerberos() {
    String tableLocationPre = "hdfs://bdms-test/user/sloth/iceberg/";
    String hdfsLocation = "/Users/yexianxun/dev/env/mammut-test-hive/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/mammut-test-hive/core-site.xml";

    Configuration conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
    initKrbConf(conf);
    //    conf.set(KerberosLoginUtil.classPathKey, "/Users/yexianxun/dev/env/mammut-test-hive");
    String location = tableLocationPre + System.currentTimeMillis();
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();

    Tables tables = new HadoopTables(conf);
    Table table = tables.create(SCHEMA, spec, location);
    table.currentSnapshot();
  }

  @Test
  public void testDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(1);

    RowTypeInfo flinkSchema = new RowTypeInfo(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.INT
    );
    TupleTypeInfo<Tuple2<Boolean, Row>> tupleTypeInfo = new TupleTypeInfo<>(
        org.apache.flink.api.common.typeinfo.Types.BOOLEAN, flinkSchema);

    List<Tuple2<Boolean, Row>> rows = Lists.newArrayList(
        Tuple2.of(true, Row.of("hello", 2)),
        Tuple2.of(true, Row.of("world", 2)),
        Tuple2.of(true, Row.of("word", 1))
    );

    DataStream<Tuple2<Boolean, Row>> dataStream = env.addSource(new FiniteTestSource<>(rows), tupleTypeInfo);

    Table table = createTestIcebergTable();
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    dataStream.addSink(new IcebergSinkFunction(tableLocation, flinkSchema));

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    table.refresh();
    Iterable<Record> results = IcebergGenerics.read(table).build();
    List<Record> records = Lists.newArrayList(results);
    // The stream will produce (hello,2),(world,2),(word,1),(hello,2),(world,2),(word,1) actually,
    // because the FiniteTestSource will produce double row list.
    Assert.assertEquals(6, records.size());

    // The hash set will remove the duplicated rows.
    Set<Record> real = Sets.newHashSet(records);
    Record record = GenericRecord.create(SCHEMA);
    Set<Record> expected = Sets.newHashSet(
        record.copy(ImmutableMap.of("word", "hello", "count", 2)),
        record.copy(ImmutableMap.of("word", "word", "count", 1)),
        record.copy(ImmutableMap.of("word", "world", "count", 2))
    );
    Assert.assertEquals("Should produce the expected record", expected, real);
  }

  private void initKrbConf(Configuration conf) {
    conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, true);
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KRB_CONF_NAME, "krb5.conf");
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KEYTAB_NAME, "sloth.keytab");
//    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KEYTAB_NAME, "yexianxun.keytab");
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_PRINCIPAL, "sloth/dev@BDMS.163.COM");
//    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_PRINCIPAL, "bdms_yexianxun/dev@BDMS.163.COM");
  }

  @Test
  public void testHdfsIcebergSink() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(2);
//    String hdfsLocation = "/Users/yexianxun/dev/env/sloth-test/hdfs-site.xml";
//    String coreLocation = "/Users/yexianxun/dev/env/sloth-test/core-site.xml";
//    String tableLocationPre = "hdfs://slothTest/user/sloth/iceberg/sink-test/";
    String hdfsLocation = "/Users/yexianxun/dev/env/mammut-test-hive/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/mammut-test-hive/core-site.xml";
    String tableLocationPre = "hdfs://bdms-test/user/sloth/iceberg/";
//    String tableLocationPre = "hdfs://bdms-test/user/sloth/yxx_iceberg/";

    RowTypeInfo flinkSchema = new RowTypeInfo(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.INT
    );
    TupleTypeInfo<Tuple2<Boolean, Row>> tupleTypeInfo = new TupleTypeInfo<>(
        org.apache.flink.api.common.typeinfo.Types.BOOLEAN, flinkSchema);

    List<Tuple2<Boolean, Row>> rows = Lists.newArrayList(
        Tuple2.of(true, Row.of("hello", 2)),
        Tuple2.of(true, Row.of("world", 2)),
        Tuple2.of(true, Row.of("word", 1))
    );

    DataStream<Tuple2<Boolean, Row>> dataStream = env.addSource(new FiniteTestSource<>(rows), tupleTypeInfo);

    Configuration conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
    initKrbConf(conf);
//    System.setProperty("HADOOP_USER_NAME", "sloth");

    String location = tableLocationPre + System.currentTimeMillis();
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();
    Tables tables = new HadoopTables(conf);
    Table table = tables.create(SCHEMA, spec, location);
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    dataStream.addSink(new IcebergSinkFunction(location, flinkSchema, conf));

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    table.refresh();
    Iterable<Record> results = IcebergGenerics.read(table).build();
    List<Record> records = Lists.newArrayList(results);
    // The stream will produce (hello,2),(world,2),(word,1),(hello,2),(world,2),(word,1) actually,
    // because the FiniteTestSource will produce double row list.
    Assert.assertEquals(6, records.size());

    // The hash set will remove the duplicated rows.
    Set<Record> real = Sets.newHashSet(records);
    Record record = GenericRecord.create(SCHEMA);
    Set<Record> expected = Sets.newHashSet(
        record.copy(ImmutableMap.of("word", "hello", "count", 2)),
        record.copy(ImmutableMap.of("word", "word", "count", 1)),
        record.copy(ImmutableMap.of("word", "world", "count", 2))
    );
    Assert.assertEquals("Should produce the expected record", expected, real);
  }
}

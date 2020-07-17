package org.apache.iceberg.flink.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.KerberosLoginUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.IOException;

import static org.apache.iceberg.hadoop.KerberosLoginUtil.classPathKey;

/**
 * Created by yexianxun@corp.netease.com on 2020/5/18.
 */
public class TestMusicABTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestMusicABTest.class);
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private Configuration conf;

  private static final org.apache.iceberg.Schema SCHEMA = new org.apache.iceberg.Schema(
      Types.NestedField.optional(1, "flink_process_time", Types.TimestampType.withoutZone()),
      Types.NestedField.optional(2, "deviceid", Types.StringType.get()),
      Types.NestedField.optional(3, "userid", Types.StringType.get()),
      Types.NestedField.optional(4, "ip", Types.StringType.get()),
      Types.NestedField.optional(5, "appver", Types.StringType.get()),
      Types.NestedField.optional(6, "os", Types.StringType.get()),
      Types.NestedField.optional(7, "osver", Types.StringType.get()),
      Types.NestedField.optional(8, "logtime", Types.LongType.get()),
      Types.NestedField.optional(9, "action", Types.StringType.get()),
      Types.NestedField.optional(10, "props", Types.MapType.ofOptional(13, 14, Types.StringType.get(), Types.StringType.get())),
      Types.NestedField.optional(11, "ds_timestamp", Types.LongType.get()),
      Types.NestedField.optional(12, "ds_fields", Types.MapType.ofOptional(15, 16, Types.StringType.get(), Types.StringType.get()))
      );

  private static final org.apache.iceberg.Schema SCHEMA_TZ = new org.apache.iceberg.Schema(
          Types.NestedField.optional(1, "flink_process_time", Types.TimestampType.withZone()),
          Types.NestedField.optional(2, "deviceid", Types.StringType.get()),
          Types.NestedField.optional(3, "userid", Types.StringType.get()),
          Types.NestedField.optional(4, "ip", Types.StringType.get()),
          Types.NestedField.optional(5, "appver", Types.StringType.get()),
          Types.NestedField.optional(6, "os", Types.StringType.get()),
          Types.NestedField.optional(7, "osver", Types.StringType.get()),
          Types.NestedField.optional(8, "logtime", Types.LongType.get()),
          Types.NestedField.optional(9, "action", Types.StringType.get()),
          Types.NestedField.optional(10, "props", Types.MapType.ofOptional(13, 14, Types.StringType.get(), Types.StringType.get())),
          Types.NestedField.optional(11, "ds_timestamp", Types.LongType.get()),
          Types.NestedField.optional(12, "ds_fields", Types.MapType.ofOptional(15, 16, Types.StringType.get(), Types.StringType.get()))
  );

  @Before
  public void createConfiguration() {
    String hdfsLocation = "/Users/yexianxun/dev/env/mammut-test-hive/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/mammut-test-hive/core-site.xml";

    conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
    conf.set(classPathKey, "/Users/yexianxun/dev/env/mammut-test-hive");
  }

  private static void initKrbConf(Configuration conf) {
    conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, true);
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KRB_CONF_NAME, "krb5.conf");
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KEYTAB_NAME, "sloth.keytab");
    String principal = "sloth/dev@BDMS.163.COM";
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_PRINCIPAL, principal);
  }

//  @Test
  public void createUnPartitionTable() throws IOException, KrbException {
    String tableLocation = "hdfs://bdms-test/user/sloth/iceberg/music_user_action_abtest_un_partition";
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .build();
    HadoopTables hadoopTables = new HadoopTables(conf);
    try {
      initKrbConf(conf);
      Table table = hadoopTables.load(tableLocation);
      Assert.assertNotNull(table);
    } catch (NoSuchTableException e) {
      e.printStackTrace();
      Table table = hadoopTables.create(SCHEMA, spec, tableLocation);
      Assert.assertNotNull(table);
    }
  }

  @Test
  public void createPartitionTableLocal() throws IOException {
    File folder = tempFolder.newFolder();
    String tableLocation = folder.getAbsolutePath();    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .hour("flink_process_time")
        .build();
    HadoopTables hadoopTables = new HadoopTables(new Configuration());
    try {
      Table table = new HadoopTables().create(SCHEMA, spec, tableLocation);
      Assert.assertNotNull(table);
    } catch (NoSuchTableException e) {
      LOG.error("failed:", e);
      Table table = hadoopTables.create(SCHEMA, spec, tableLocation);
      Assert.assertNotNull(table);
    }
  }

//  @Test
  public void createPartitionTable() throws IOException {
    String tableLocation = "hdfs://bdms-test/user/sloth/iceberg/music_user_action_abtest_partition_2";
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .hour("flink_process_time")
        .build();
    HadoopTables hadoopTables = new HadoopTables(conf);
    try {
      initKrbConf(conf);
      Table table = hadoopTables.load(tableLocation);
      Assert.assertNotNull(table);
    } catch (NoSuchTableException e) {
      Table table = hadoopTables.create(SCHEMA, spec, tableLocation);
      Assert.assertNotNull(table);
    }
  }

//  @Test
  public void createPartitionTableJD() throws IOException {
    String tableLocation = "hdfs://sloth-jd-pub/user/sloth/iceberg/music_user_action_abtest_partition";
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .hour("flink_process_time")
        .build();
    String hdfsLocation = "/Users/yexianxun/dev/env/hz_online/conf_sloth_jd_pub/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/hz_online/conf_sloth_jd_pub/core-site.xml";

    System.setProperty("HADOOP_USER_NAME", "sloth");

    Configuration conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
    HadoopTables hadoopTables = new HadoopTables(conf);
    try {
      conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, false);
      Table table = hadoopTables.load(tableLocation);
      Assert.assertNotNull(table);
    } catch (NoSuchTableException e) {
      Table table = hadoopTables.create(SCHEMA, spec, tableLocation);
      Assert.assertNotNull(table);
    }
  }

//  @Test
  public void createPartitionTableJDTZ() throws IOException {
    String tableLocation = "hdfs://sloth-jd-pub/user/sloth/iceberg/music_user_action_abtest_partition_tz";
    PartitionSpec spec = PartitionSpec
            .builderFor(SCHEMA_TZ)
            .hour("flink_process_time")
            .build();
    String hdfsLocation = "/Users/yexianxun/dev/env/hz_online/conf_sloth_jd_pub/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/hz_online/conf_sloth_jd_pub/core-site.xml";

    System.setProperty("HADOOP_USER_NAME", "sloth");

    Configuration conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
    HadoopTables hadoopTables = new HadoopTables(conf);
    try {
      conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, false);
      Table table = hadoopTables.load(tableLocation);
      Assert.assertNotNull(table);
    } catch (NoSuchTableException e) {
      Table table = hadoopTables.create(SCHEMA_TZ, spec, tableLocation);
      Assert.assertNotNull(table);
    }
  }
}

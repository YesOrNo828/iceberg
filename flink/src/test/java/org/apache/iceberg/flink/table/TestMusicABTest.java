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
import org.junit.Test;
import sun.security.krb5.KrbException;

import java.io.IOException;

/**
 * Created by yexianxun@corp.netease.com on 2020/5/18.
 */
public class TestMusicABTest {
  private Configuration conf;

  private static final org.apache.iceberg.Schema SCHEMA = new org.apache.iceberg.Schema(
      Types.NestedField.optional(1, "deviceid", Types.StringType.get()),
      Types.NestedField.optional(2, "userid", Types.StringType.get()),
      Types.NestedField.optional(3, "ip", Types.StringType.get()),
      Types.NestedField.optional(4, "appver", Types.StringType.get()),
      Types.NestedField.optional(5, "os", Types.StringType.get()),
      Types.NestedField.optional(6, "osver", Types.StringType.get()),
      Types.NestedField.optional(7, "logtime", Types.LongType.get()),
      Types.NestedField.optional(8, "action", Types.StringType.get()),
      Types.NestedField.optional(9, "props", Types.MapType.ofOptional(12, 13, Types.StringType.get(), Types.StringType.get())),
      Types.NestedField.optional(10, "ds_timestamp", Types.LongType.get()),
      Types.NestedField.optional(11, "ds_fields", Types.MapType.ofOptional(14, 15, Types.StringType.get(), Types.StringType.get()))
  );

  @Before
  public void createConfiguration() {
    String hdfsLocation = "/Users/yexianxun/dev/env/mammut-test-hive/hdfs-site.xml";
    String coreLocation = "/Users/yexianxun/dev/env/mammut-test-hive/core-site.xml";

    conf = new Configuration(false);
    conf.addResource(new Path(hdfsLocation));
    conf.addResource(new Path(coreLocation));
  }

  private static void initKrbConf(Configuration conf) {
    conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, true);
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KRB_CONF_NAME, "krb5.conf");
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KEYTAB_NAME, "sloth.keytab");
    String principal = "sloth/dev@BDMS.163.COM";
    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_PRINCIPAL, principal);
  }

  @Test
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
}

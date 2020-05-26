//package org.apache.iceberg.flink;
//
//import com.google.common.collect.Lists;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.api.java.typeutils.TupleTypeInfo;
//import org.apache.flink.runtime.state.CheckpointListener;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.types.Row;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.iceberg.PartitionSpec;
//import org.apache.iceberg.Schema;
//import org.apache.iceberg.Table;
//import org.apache.iceberg.Tables;
//import org.apache.iceberg.hadoop.HadoopTables;
//import org.apache.iceberg.hadoop.KerberosLoginUtil;
//import org.apache.iceberg.types.Types;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.List;
//
///**
// * Created by yexianxun@corp.netease.com on 2020/4/29.
// */
//public class KrbSinkExample2 {
//  private static final Logger LOG = LoggerFactory.getLogger(KrbSinkExample2.class);
//
//  private static final Schema SCHEMA = new Schema(
//      Types.NestedField.optional(1, "word", Types.StringType.get()),
//      Types.NestedField.optional(2, "count", Types.IntegerType.get())
//  );
//
//  public static void main(String[] args) throws Exception {
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    // Enable the checkpoint.
//    env.enableCheckpointing(100);
//    env.setParallelism(2);
//    String classpathInJar = "zz_sloth";
//    String confDir = KerberosLoginUtil.copyFiles(classpathInJar, "sloth-hdfs-site.xml", "sloth-core-site.xml");
//
//    String tableLocationPre = "hdfs://slothTest/user/sloth/iceberg/sink-test/";
//
//    RowTypeInfo flinkSchema = new RowTypeInfo(
//        org.apache.flink.api.common.typeinfo.Types.STRING,
//        org.apache.flink.api.common.typeinfo.Types.INT
//    );
//    TupleTypeInfo<Tuple2<Boolean, Row>> tupleTypeInfo = new TupleTypeInfo<>(
//        org.apache.flink.api.common.typeinfo.Types.BOOLEAN, flinkSchema);
//
//    List<Tuple2<Boolean, Row>> rows = Lists.newArrayList(
//        Tuple2.of(true, Row.of("hello", 2)),
//        Tuple2.of(true, Row.of("world", 2)),
//        Tuple2.of(true, Row.of("word", 1))
//    );
//
//    DataStream<Tuple2<Boolean, Row>> dataStream = env.addSource(new SourceDemo<>(rows), tupleTypeInfo);
//
//    Configuration conf = new Configuration(false);
//    conf.addResource(new Path(confDir, "sloth-hdfs-site.xml"));
//    conf.addResource(new Path(confDir, "sloth-core-site.xml"));
//    conf.set(KerberosLoginUtil.classPathKey, classpathInJar);
//    initKrbConf(conf);
////    System.setProperty("HADOOP_USER_NAME", "sloth");
//
//    String location = tableLocationPre + System.currentTimeMillis();
//    PartitionSpec spec = PartitionSpec
//        .builderFor(SCHEMA)
//        .identity("word")
//        .build();
//    Tables tables = new HadoopTables(conf);
//    Table table = tables.create(SCHEMA, spec, location);
//
//    // Output the data stream to stdout.
//    dataStream.addSink(new IcebergSinkFunction(location, flinkSchema, conf));
//
//    // Execute the program.
//    env.execute("Test Iceberg DataStream");
//  }
//
//  private static void initKrbConf(Configuration conf) {
//    conf.setBoolean(KerberosLoginUtil.KERBEROS_ENABLED, false);
////    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KRB_CONF_NAME, "krb5.conf");
////    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_KEYTAB_NAME, "sloth.keytab");
////    String principal = "sloth/dev@BDMS.163.COM";
////    conf.set(KerberosLoginUtil.KERBEROS_LOGIN_PRINCIPAL, principal);
////    SlothContext.setKrbPrincipal(principal);
//  }
//
//  private static class SourceDemo<T> implements SourceFunction<T>, CheckpointListener {
//    private final Iterable<T> elements;
//
//    private volatile boolean running = true;
//
//    private transient int numCheckpointsComplete;
//
//    public SourceDemo(Iterable<T> elements) {
//      this.elements = elements;
//    }
//
//    @Override
//    public void run(SourceContext<T> ctx) throws Exception {
//      // first round of sending the elements and waiting for the checkpoints
//      emitElementsAndWaitForCheckpoints(ctx, 2);
//
//      // second round of the same
//      emitElementsAndWaitForCheckpoints(ctx, 2);
//    }
//
//    private void emitElementsAndWaitForCheckpoints(SourceContext<T> ctx, int checkpointsToWaitFor) throws InterruptedException {
//      final Object lock = ctx.getCheckpointLock();
//
//      final int checkpointToAwait;
//      synchronized (lock) {
//        checkpointToAwait = numCheckpointsComplete + checkpointsToWaitFor;
//        for (T t : elements) {
//          ctx.collect(t);
//        }
//      }
//
//      synchronized (lock) {
//        while (running && numCheckpointsComplete < checkpointToAwait) {
//          lock.wait(1);
//        }
//      }
//    }
//
//    @Override
//    public void cancel() {
//      running = false;
//    }
//
//    @Override
//    public void notifyCheckpointComplete(long checkpointId) throws Exception {
//      numCheckpointsComplete++;
//    }
//  }
//}

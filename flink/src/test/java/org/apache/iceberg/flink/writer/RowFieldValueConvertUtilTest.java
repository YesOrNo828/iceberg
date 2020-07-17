package org.apache.iceberg.flink.writer;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

/**
 * Created by yexianxun@corp.netease.com on 2020/7/17.
 */
public class RowFieldValueConvertUtilTest {

  @Test
  public void testConvert() {
    String dateFormat = "2019-11-11T10:00:01Z";
    Instant instant = Instant.parse(dateFormat);
    Row row = Row.of(instant);
    RowFieldValueConvertUtil.convertValue(Collections.singletonList(0), row);
    Assert.assertEquals(instant.toString(), row.getField(0).toString());

    dateFormat = "2019-11-11T10:00:00Z";
    instant = Instant.parse(dateFormat);
    row = Row.of(instant);
    RowFieldValueConvertUtil.convertValue(Collections.singletonList(0), row);
    OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
    OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    long actual = ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) row.getField(0));
    long expected = ChronoUnit.MICROS.between(EPOCH, offsetDateTime);
    Assert.assertEquals(expected, actual);

    instant = Instant.now();
    row = Row.of(instant);
    RowFieldValueConvertUtil.convertValue(Collections.singletonList(0), row);
    Assert.assertEquals(instant.toString(), row.getField(0).toString());
  }
}

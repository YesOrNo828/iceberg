package org.apache.iceberg.flink.writer;

import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Created by yexianxun@corp.netease.com on 2020/7/17.
 */
public class RowFieldValueConvertUtil {

    /**
     * convert Row.Instant to Row.OffsetDateTime
     * @param localTimeZoneFieldIndexes LocalTimeZone(Instant) field type in row indices
     * @param row Row
     * @return Row
     */
    public static Row convertValue(List<Integer> localTimeZoneFieldIndexes, Row row) {
        localTimeZoneFieldIndexes.forEach(index -> {
            if (row.getField(index) instanceof Instant) {
                Instant instant = (Instant) row.getField(index);
                row.setField(index, instant.atOffset(ZoneOffset.UTC));
            }
        });
        return row;
    }
}

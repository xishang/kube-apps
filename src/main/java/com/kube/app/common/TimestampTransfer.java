package com.kube.app.common;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class TimestampTransfer extends ScalarFunction {

    public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(@DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {
        return timestamp.truncatedTo(ChronoUnit.MINUTES);
    }
}

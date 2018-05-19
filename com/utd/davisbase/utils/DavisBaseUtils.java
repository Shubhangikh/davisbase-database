package com.utd.davisbase.utils;

import java.util.HashSet;
import java.util.Set;

public class DavisBaseUtils {
    public final int PAGE_SIZE = 512;
    public final byte PAGE_TYPE_OFFSET = 0;
    public final byte PAGE_TYPE_LEAF = 0x0D;
    public final byte PAGE_TYPE_INTERIOR = 0x05;
    public final byte RECORD_COUNT_OFFSET = 1;
    public final short START_OF_CONTENT_OFFSET = 2;
    public final int RIGHT_PAGE_OFFSET = 4;
    public final short RECORDS_ARRAY_OFFSET = 8;
    public final byte COLUMNS_COUNT_SIZE = 1;
    public final byte ROW_ID_SIZE = 4;
    public final byte PAYLOAD_SIZE = 1;
    public final byte LEAF_PAGE_CELL_HEADER_SIZE = 6;
    public final byte INTERIOR_PAGE_CELL_HEADER_SIZE = 8;
    public final byte CELL_ROW_ID_OFFSET = 2;
    public final byte COLUMNS_COUNT_OFFSET = 7;
    public final byte DAVISBASE_TABLES_COLUMNS_COUNT = 4;
    public final byte DAVISBASE_COLUMNS_COLUMNS_COUNT = 7;

    private Set<String> dataTypes;
    private Set<String> operators;

    public DavisBaseUtils() {
        dataTypes = new HashSet<>();
        dataTypes.add("tinyint");
        dataTypes.add("smallint");
        dataTypes.add("int");
        dataTypes.add("bigint");
        dataTypes.add("real");
        dataTypes.add("double");
        dataTypes.add("datetime");
        dataTypes.add("date");
        dataTypes.add("text");

        operators = new HashSet<>();
        operators.add("=");
        operators.add("!=");
        operators.add("like");
        operators.add("is null");
        operators.add("is not null");
        operators.add("<");
        operators.add("<=");
        operators.add(">");
        operators.add(">=");
    }

    public Set<String> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(Set<String> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public Set<String> getOperators() {
        return operators;
    }

    public void setOperators(Set<String> operators) {
        this.operators = operators;
    }
}

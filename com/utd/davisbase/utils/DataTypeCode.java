package com.utd.davisbase.utils;

public final class DataTypeCode {
    public static final byte TINYINT_NULL = 0x00;
    public static final byte SMALLINT_NULL = 0x01;
    public static final byte INT_OR_REAL_NULL = 0x02;
    public static final byte DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL = 0x03;
    public static final byte TINYINT = 0x04;
    public static final byte SMALLINT = 0x05;
    public static final byte INT = 0x06;
    public static final byte BIGINT = 0x07;
    public static final byte REAL = 0x08;
    public static final byte DOUBLE = 0x09;
    public static final byte DATETIME = 0x0A;
    public static final byte DATE = 0x0B;
    public static final byte TEXT = 0x0C;
}

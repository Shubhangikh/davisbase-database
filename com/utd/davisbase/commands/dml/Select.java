package com.utd.davisbase.commands.dml;

import com.utd.davisbase.mode.DavisBasePrompt;
import com.utd.davisbase.utils.*;
import org.apache.log4j.Logger;

import java.io.RandomAccessFile;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.out;

public class Select {
    private final static Logger LOGGER = Logger.getLogger(Select.class.getName());

    private final static String SHOW_TABLES = "SELECT TABLE_NAME FROM DAVISBASE_TABLES";
    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();
    private final static String COL_SEPARATOR = "-";
    private final static String ROW_SEPARATOR = "|";
    private final static String COL_JOIN = "+";

    public static int parseSelectQuery(String selectQuery, boolean printResult) {
        // selectQuery = "SELECT * FROM <table-name> WHERE TABLE_NAME operator 'x'";
        // get all tables from davisbase_tables and check if the table given in the query exist. if yes, get all the columns
        // of that table and check if where clause has valid column name. In place of *, a column or comma separated list of
        // columns can be provided. Validate operator value. Read all data from that table's file and search for x.
        if (selectQuery.equals(SHOW_TABLES)) {
            List<String> columnNames = new ArrayList<>();
            columnNames.add("table_name");
            return parseShowTables(columnNames);
        }
        List<List<String>> finalRecords;
        Pattern pattern = Pattern.compile("(SELECT|select)[\\s]+([\\w\\s,*]+)[\\s]+(FROM|from)[\\s]+([\\w-]+)[\\s]*(WHERE|where)?([\\s\\w\\=!><']*)");
        Matcher m = pattern.matcher(selectQuery);
        if (!m.matches()) {
            LOGGER.error("Invalid SELECT query. Please try again...");
            return -1;
        } else {
            int groupCount = m.groupCount();
            if (groupCount < 4) {
                LOGGER.error("Invalid SELECT query. Please try again...");
                return -1;
            } else {
                String selectTableName = m.group(4);
                if (!checkTableExistence(selectTableName)) {
                    LOGGER.error("Table does not exist");
                    out.println("Table does not exist");
                    return -1;
                }
                List<List<String>> tableDetails = fetchTableColumns(selectTableName);
                List<String> columnHeaders;
                String columns = m.group(2).toLowerCase().trim();
                List<List<String>> records = fetchAllRecords(selectTableName);
                List<List<String>> matchRecordsByWhereClause = new ArrayList<>();
                String whereClauseColumn = null;
                String whereClauseValue = "";
                String whereOperator = "=";
                int whereClauseColumnIndex = 0;
                if (m.group(5) != null && m.group(6) == null) {
                    LOGGER.error("WHERE condition missing");
                    out.println("WHERE condition missing");
                    return -1;
                } else if(m.group(5) == null && (!m.group(6).equals(""))) {
                    LOGGER.error("WHERE keyword missing");
                    out.println("WHERE keyword missing");
                    return -1;
                }
                if(m.group(6) != null && !m.group(6).equals("")) {
                    String whereCondition = m.group(6).trim().toLowerCase();
                    String[] conditionTokens = m.group(6).toLowerCase().trim().split(" ");
                    if(conditionTokens.length < 3) {
                        LOGGER.error("Invalid where condition");
                        out.println("Invalid where condition");
                        return -1;
                    }
                    whereClauseColumn = conditionTokens[0];
                    if(tableDetails.get(0).indexOf(whereClauseColumn) == -1) {
                        LOGGER.error("Invalid column name in where clause");
                        out.println("Invalid column name in where clause");
                        return -1;
                    } else {
                        whereClauseColumnIndex = tableDetails.get(0).indexOf(whereClauseColumn);
                    }
                    if(whereCondition.contains("not null")) {
                        if(conditionTokens.length != 3) {
                            LOGGER.error("Invalid use of \"is null\" operator");
                            out.println("Invalid use of \"is null\" operator");
                            return -1;
                        }
                        whereOperator = "not null";
                    }
                    if(whereCondition.contains("is not null")) {
                        if(conditionTokens.length != 4) {
                            LOGGER.error("Invalid use of \"is not null\" operator");
                            out.println("Invalid use of \"is not null\" operator");
                            return -1;
                        }
                        whereOperator = "is not null";
                    }
                    if(conditionTokens.length == 3 && !whereCondition.contains("not null")
                            && !whereCondition.contains("is not null")) {
                        if(!davisBaseUtils.getOperators().contains(conditionTokens[1])) {
                            LOGGER.error("Invalid condition operator");
                            out.println("Invalid condition operator");
                            return -1;
                        }
                        whereOperator = conditionTokens[1].trim();
                        whereClauseValue = conditionTokens[2].trim();
                        if(whereClauseValue.indexOf("'") == 0 && whereClauseValue.lastIndexOf("'") == whereClauseValue.length() - 1) {
                            whereClauseValue = whereClauseValue.substring(1, whereClauseValue.length() - 1);
                        }
                    }
                    int colIndexForDataType = tableDetails.get(0).indexOf(whereClauseColumn);
                    String whereColumnDataType = tableDetails.get(1).get(colIndexForDataType);
                    try {
                        for (List<String> record : records) {
                            switch (whereOperator) {
                                case Operator.EQUALS:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) == Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) == Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) == Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) == Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) == Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) == Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) == 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) == 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.NOT_EQUALS:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) != Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) != Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) != Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) != Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) != Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) != Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) != 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) != 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if (!(record.get(whereClauseColumnIndex).equalsIgnoreCase(whereClauseValue))) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.GREATER_THAN:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) > Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) > Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) > Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) > Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) > Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) > Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) > 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) > 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if ((record.get(whereClauseColumnIndex).compareTo(whereClauseValue)) > 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.GREATER_THAN_EQUALS:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) >= Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) >= Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) >= Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) >= Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) >= Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) >= Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) >= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) >= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if ((record.get(whereClauseColumnIndex).compareTo(whereClauseValue)) >= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.LESS_THAN:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) < Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) < Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) < Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) < Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) < Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) < Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) < 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) < 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if ((record.get(whereClauseColumnIndex).compareTo(whereClauseValue)) < 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.LESS_THAN_EQUALS:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (Byte.parseByte(record.get(whereClauseColumnIndex)) <= Byte.parseByte(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (Short.parseShort(record.get(whereClauseColumnIndex)) <= Short.parseShort(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (Integer.parseInt(record.get(whereClauseColumnIndex)) <= Integer.parseInt(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (Long.parseLong(record.get(whereClauseColumnIndex)) <= Long.parseLong(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (Float.parseFloat(record.get(whereClauseColumnIndex)) <= Float.parseFloat(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (Double.parseDouble(record.get(whereClauseColumnIndex)) <= Double.parseDouble(whereClauseValue)) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                            Date savedDateValue = dateTimeFormat.parse(record.get(whereClauseColumnIndex));
                                            Date inputDateTime = dateTimeFormat.parse(whereClauseValue);
                                            if (savedDateValue.compareTo(inputDateTime) <= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            Date savedValue = dateFormat.parse(record.get(whereClauseColumnIndex));
                                            Date input = dateFormat.parse(whereClauseValue);
                                            if (savedValue.compareTo(input) <= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if ((record.get(whereClauseColumnIndex).compareTo(whereClauseValue)) <= 0) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.IS_NULL:
                                    switch (whereColumnDataType.toUpperCase()) {
                                        case DataTypeNames.TINYINT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.SMALLINT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.INT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.BIGINT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.REAL:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DOUBLE:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATETIME:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.DATE:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                            break;
                                        case DataTypeNames.TEXT:
                                            if (record.get(whereClauseColumnIndex).equalsIgnoreCase("null")) {
                                                matchRecordsByWhereClause.add(record);
                                            }
                                    }
                                    break;
                                case Operator.IS_NOT_NULL:
                                    if (!(record.get(whereClauseColumnIndex).equalsIgnoreCase("null"))) {
                                        matchRecordsByWhereClause.add(record);
                                    }
                                    break;
                            }
                        }
                    } catch(NumberFormatException nfe) {
                        LOGGER.error("Incorrect format of 1 or more input values");
                        out.println("Incorrect format of 1 or more input values");
                        return -1;
                    }
                    catch(ParseException pe) {
                        LOGGER.error("Incorrect date/datetime format");
                        out.println("Incorrect date/datetime format");
                        return -1;
                    }

                    // TO-DO Handle all operators!!!
                    //  TO-DO include is null and is not null conditions and other operators and other operators
                    /*for(int i = 0; i < records.size(); i++) {
                        if(records.get(i).get(whereClauseColumnIndex).equalsIgnoreCase(whereClauseValue)) {
                            matchRecordsByWhereClause.add(records.get(i));
                        }
                    }*/
                    if(matchRecordsByWhereClause.size() == 0) {
                        return 0;
                    }
                }
                Set<Integer> ordinalPositions = new HashSet<>();
                if(columns.equals("*")) {
                    columnHeaders = new ArrayList<>(tableDetails.get(0));
                    LOGGER.debug("Printing all columns");
                } else if(columns.trim().split(",").length == 1 && columns.trim().split(" ").length == 1) {
                    if(!tableDetails.get(0).contains(columns)) {
                        LOGGER.error("Invalid columns list");
                        out.println("Invalid columns list");
                        return -1;
                    }
                    int ordinalPosition = tableDetails.get(0).indexOf(columns);
                    ordinalPositions.add(ordinalPosition);
                    columnHeaders = new ArrayList<>();
                    columnHeaders.add(columns.trim());
                } else {
                    columnHeaders = new ArrayList<>();
                    for(String column: columns.split(",")) {
                        if(column.trim().split(" ").length > 1) {
                            LOGGER.error("Invalid columns list");
                            out.println("Invalid columns list");
                            return -1;
                        }
                        if(!tableDetails.get(0).contains(column.trim())) {
                            LOGGER.error("Invalid columns list");
                            out.println("Invalid columns list");
                            return -1;
                        }
                        ordinalPositions.add(tableDetails.get(0).indexOf(column.trim()));
                        columnHeaders.add(column.trim());
                    }
                }

                if(matchRecordsByWhereClause.size() != 0) {
                    finalRecords = getRecordsByColumns(ordinalPositions, matchRecordsByWhereClause);
                    if (printResult && finalRecords.size() > 0) {
                        printSelectResult(columnHeaders, finalRecords);
                    } else {
                        return Integer.parseInt(finalRecords.get(0).get(0));
                    }
                } else {
                    finalRecords = getRecordsByColumns(ordinalPositions,records);
                    if (printResult && finalRecords.size() > 0) {
                        printSelectResult(columnHeaders, finalRecords);
                    }
                }
            }
        }
        return finalRecords.size();
    }

    private static List<List<String>> getRecordsByColumns(Set<Integer> ordinalPositions, List<List<String>> tableRecords) {
        List<List<String>> records = new ArrayList<>();
        if(ordinalPositions.size() == 0) {
            return tableRecords;
        }
        for(List<String> record : tableRecords) {
            List<String> row = new ArrayList<>();
            for(int position : ordinalPositions) {
                row.add(record.get(position));
            }
            records.add(row);
        }
        return records;
    }

    public static List<List<String>> fetchAllRecords(String selectTableName) {
        List<List<String>> records = new ArrayList<>();
        RandomAccessFile table;
        List<Integer> recordStartPositions;
        try {
            if(selectTableName.equalsIgnoreCase("davisbase_columns") || selectTableName.equalsIgnoreCase("davisbase_tables")) {
                table = new RandomAccessFile("data/catalog/" + selectTableName + ".tbl", "rw");
            } else {
                table = new RandomAccessFile("data/user_data/" + selectTableName + ".tbl", "rw");
            }
            long pageCount = table.length()/davisBaseUtils.PAGE_SIZE;
            table.seek(davisBaseUtils.RECORD_COUNT_OFFSET);
            recordStartPositions = new ArrayList<>();

            int position;
            for(int i = 1; i <= pageCount; i++) {
                table.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.PAGE_TYPE_OFFSET);
                if(table.readByte() == davisBaseUtils.PAGE_TYPE_LEAF) {
                    table.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                    while ((position = (int) table.readShort()) != 0) {
                        recordStartPositions.add(position);
                    }
                }
            }
            for (Integer recordStartPosition : recordStartPositions) {
                int[] size;
                List<String> record = new ArrayList<>();
                table.seek(recordStartPosition + davisBaseUtils.CELL_ROW_ID_OFFSET);
                record.add(String.valueOf(table.readInt()));
                int columnCount = table.readByte();
                size = new int[columnCount];
                for (int k = 0; k < columnCount; k++) {
                    byte contentSize = table.readByte();
                    switch (contentSize) {
                        case DataTypeCode.TINYINT_NULL:
                            size[k] = DataTypeCode.TINYINT_NULL;
                            break;
                        case DataTypeCode.SMALLINT_NULL:
                            size[k] = DataTypeCode.SMALLINT_NULL;
                            break;
                        case DataTypeCode.INT_OR_REAL_NULL:
                            size[k] = DataTypeCode.INT_OR_REAL_NULL;
                            break;
                        case DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL:
                            size[k] = DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL;
                            break;
                        case DataTypeCode.TINYINT:
                            size[k] = DataTypeCode.TINYINT;
                            break;
                        case DataTypeCode.SMALLINT:
                            size[k] = DataTypeCode.SMALLINT;
                            break;
                        case DataTypeCode.INT:
                            size[k] = DataTypeCode.INT;
                            break;
                        case DataTypeCode.BIGINT:
                            size[k] = DataTypeCode.BIGINT;
                            break;
                        case DataTypeCode.REAL:
                            size[k] = DataTypeCode.REAL;
                            break;
                        case DataTypeCode.DOUBLE:
                            size[k] = DataTypeCode.DOUBLE;
                            break;
                        case DataTypeCode.DATETIME:
                            size[k] = DataTypeCode.DATETIME;
                            break;
                        case DataTypeCode.DATE:
                            size[k] = DataTypeCode.DATE;
                            break;
                        default:
                            size[k] = contentSize;
                    }
                }
                int payloadStart = recordStartPosition + davisBaseUtils.COLUMNS_COUNT_OFFSET + columnCount;
                table.seek(payloadStart);
                for (int aSize : size) {
                    switch (aSize) {
                        case DataTypeCode.TINYINT_NULL:
                            table.readByte();
                            record.add(DataTypeNames.TINYINT_NULL);
                            break;
                        case DataTypeCode.SMALLINT_NULL:
                            table.readShort();
                            record.add(DataTypeNames.SMALLINT_NULL);
                            break;
                        case DataTypeCode.INT_OR_REAL_NULL:
                            table.readInt();
                            record.add(DataTypeNames.INT_OR_REAL_NULL);
                            break;
                        case DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL:
                            table.readLong();
                            record.add(DataTypeNames.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL);
                            break;
                        case DataTypeCode.TINYINT:
                            record.add(String.valueOf(table.readByte()));
                            break;
                        case DataTypeCode.SMALLINT:
                            record.add(String.valueOf(table.readShort()));
                            break;
                        case DataTypeCode.INT:
                            record.add(String.valueOf(table.readInt()));
                            break;
                        case DataTypeCode.BIGINT:
                            record.add(String.valueOf(table.readLong()));
                            break;
                        case DataTypeCode.REAL:
                            record.add(String.valueOf(table.readFloat()));
                            break;
                        case DataTypeCode.DOUBLE:
                            record.add(String.valueOf(table.readDouble()));
                            break;
                        case DataTypeCode.DATETIME:
                            Date dateTime = new Date(table.readLong());
                            Format dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                            record.add(dateTimeFormat.format(dateTime));
                            break;
                        case DataTypeCode.DATE:
                            Date date = new Date(table.readLong());
                            Format format = new SimpleDateFormat("yyyy-MM-dd");
                            record.add(format.format(date));
                            break;
                        default:
                            byte[] text = new byte[aSize - DataTypeCode.TEXT];
                            for (int ix = 0; ix < text.length; ix++) {
                                text[ix] = table.readByte();
                            }
                            if (aSize - DataTypeCode.TEXT > 0) {
                                record.add(new String(text));
                            } else {
                                record.add("null");
                            }
                    }

                }
                records.add(record);
            }
            table.close();
        } catch (Exception e) {
            LOGGER.error("Error reading " + selectTableName + ".tbl file");
        }
        return records;
    }

    public static List<List<String>> fetchTableColumns(String selectTableName) {
        List<List<String>> tableDetails = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<String> dataTypes = new ArrayList<>();
        List<String> constraints = new ArrayList<>();
        List<Integer> recordStartPositions;
        RandomAccessFile davisBaseColumns;
        try {
            davisBaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
            long pageCount = davisBaseColumns.length()/davisBaseUtils.PAGE_SIZE;
            davisBaseColumns.seek(davisBaseUtils.RECORD_COUNT_OFFSET);
            recordStartPositions = new ArrayList<>();

            int position;
            for(int i = 1; i <= pageCount; i++) {
                davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                while((position = (int)davisBaseColumns.readShort()) != 0) {
                    recordStartPositions.add(position);
                }
            }

            for (Integer recordStartPosition : recordStartPositions) {
                davisBaseColumns.seek(recordStartPosition + davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE);
                int columnCount = davisBaseColumns.readByte();
                int tableNameLength = davisBaseColumns.readByte() - DataTypeCode.TEXT;
                int columnNameLength = davisBaseColumns.readByte() - DataTypeCode.TEXT;
                int dataTypeLength = davisBaseColumns.readByte() - DataTypeCode.TEXT;
                int ordinalPositionSize = davisBaseColumns.readByte();
                int constraintLength = davisBaseColumns.readByte() - DataTypeCode.TEXT;
                ;
                byte[] tableNameBytes = new byte[tableNameLength];
                davisBaseColumns.seek(davisBaseColumns.getFilePointer() + 1);
                for (int k = 0; k < tableNameLength; k++) {
                    tableNameBytes[k] = davisBaseColumns.readByte();
                }
                if (new String(tableNameBytes).equalsIgnoreCase(selectTableName)) {
                    byte[] columnNameBytes = new byte[columnNameLength];
                    byte[] dataTypeBytes = new byte[dataTypeLength];
                    byte[] constraintBytes = new byte[constraintLength];
                    for (int k = 0; k < columnNameLength; k++) {
                        columnNameBytes[k] = davisBaseColumns.readByte();
                    }
                    columns.add(new String(columnNameBytes));

                    for (int k = 0; k < dataTypeLength; k++) {
                        dataTypeBytes[k] = davisBaseColumns.readByte();
                    }
                    dataTypes.add(new String(dataTypeBytes));
                    davisBaseColumns.seek(davisBaseColumns.getFilePointer() + DataTypeContentSize.TINYINT);
                    for (int k = 0; k < constraintLength; k++) {
                        constraintBytes[k] = davisBaseColumns.readByte();
                    }
                    constraints.add(new String(constraintBytes));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading davisbase_columns table.", e);
        }

        tableDetails.add(columns);
        tableDetails.add(dataTypes);
        tableDetails.add(constraints);
        return tableDetails;
    }

    private static int parseShowTables(List<String> columnNames) {
        String tableName = "davisbase_tables";
        if (!checkTableExistence(tableName)) {
            LOGGER.error("Table does not exist.");
            DavisBasePrompt.print("Table does not exist");
            return 0;
        }

        List<List<String>> columnValues = readAllTables();
        if (columnValues.size() > 0) {
            printSelectResult(columnNames, columnValues);
        }
        return columnValues.size();
    }

    private static void printSelectResult(List<String> columnNames, List<List<String>> columnValues) {
        int[] columnWidths = new int[columnNames.size()];
        for (int i = 0; i < columnWidths.length; i++) {
            int maxLength = columnNames.get(i).length();
            for (int j = 0; j < columnValues.size(); j++) {
                String value = columnValues.get(j).get(i);
                if (value.length() > maxLength) {
                    maxLength = value.length();
                }
            }
            columnWidths[i] = maxLength;
        }
        for (int width : columnWidths) {
            out.print(COL_JOIN + line(COL_SEPARATOR, width + 2));
        }
        out.println(COL_JOIN);
        out.print(ROW_SEPARATOR);
        for (int i = 0; i < columnNames.size(); i++) {
            out.format(" %1$-" + columnWidths[i] + "s " + ROW_SEPARATOR, columnNames.get(i).toUpperCase());
        }
        out.print("\n");
        for (int width : columnWidths) {
            out.print(COL_JOIN + line(COL_SEPARATOR, width + 2));
        }
        out.println(COL_JOIN);
        int rowCount = columnValues.size();
        for (int i = 0; i < rowCount; i++) {
            out.print(ROW_SEPARATOR);
            for (int j = 0; j < columnWidths.length; j++) {
                out.format(" %1$-" + columnWidths[j] + "s " + ROW_SEPARATOR, columnValues.get(i).get(j));
            }
            out.print("\n");
        }
        for (int width : columnWidths) {
            out.print(COL_JOIN + line(COL_SEPARATOR, width + 2));
        }
        out.println(COL_JOIN);
    }

    private static String line(String s, int num) {
        StringBuilder a = new StringBuilder();
        for (int i = 0; i < num; i++) {
            a.append(s);
        }
        return a.toString();
    }

    private static List<List<String>> readAllTables() {
        List<List<String>> records = new ArrayList<>();
        List<String> tableNames;
        int[] recordStartPositions;
        try (RandomAccessFile davisBaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw")) {

            davisBaseTables.seek(davisBaseUtils.RECORD_COUNT_OFFSET);
            int recordCount = davisBaseTables.readByte();
            recordStartPositions = new int[recordCount];

            davisBaseTables.seek(davisBaseUtils.RECORDS_ARRAY_OFFSET);
            for (int i = 0; i < recordCount; i++) {
                recordStartPositions[i] = davisBaseTables.readShort();
            }
            for (int j = 0; j < recordStartPositions.length; j++) {
                tableNames = new ArrayList<>();
                davisBaseTables.seek(recordStartPositions[j] + davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE);
                int columnCount = davisBaseTables.readByte();
                int tableNameLength = davisBaseTables.readByte() - DataTypeCode.TEXT;
                byte[] tableNameBytes = new byte[tableNameLength];
                davisBaseTables.seek(davisBaseTables.getFilePointer() + columnCount - 1);
                for (int k = 0; k < tableNameLength; k++) {
                    tableNameBytes[k] = davisBaseTables.readByte();
                }
                tableNames.add(new String(tableNameBytes));
                records.add(tableNames);
            }
        } catch (Exception e) {
            LOGGER.error("Error reading davisbase_tables.tbl file.");
        }
        return records;
    }

    public static boolean checkTableExistence(String tableName) {
        boolean exists = false;
        int[] recordStartPositions;
        RandomAccessFile davisBaseTables;
        try {
            davisBaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            davisBaseTables.seek(davisBaseUtils.RECORD_COUNT_OFFSET);
            int recordCount = davisBaseTables.readByte();
            recordStartPositions = new int[recordCount];

            davisBaseTables.seek(davisBaseUtils.RECORDS_ARRAY_OFFSET);
            for (int i = 0; i < recordCount; i++) {
                recordStartPositions[i] = davisBaseTables.readShort();
            }
            for (int j = 0; j < recordStartPositions.length; j++) {
                davisBaseTables.seek(recordStartPositions[j] + davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE);
                int columnCount = davisBaseTables.readByte();
                int tableNameLength = davisBaseTables.readByte() - 0x0C;
                byte[] tableNameBytes = new byte[tableNameLength];
                davisBaseTables.seek(davisBaseTables.getFilePointer() + columnCount - 1);
                for (int k = 0; k < tableNameLength; k++) {
                    tableNameBytes[k] = davisBaseTables.readByte();
                }
                String tableNameRetrieved = new String(tableNameBytes);
                if (tableName.equalsIgnoreCase(tableNameRetrieved)) {
                    exists = true;
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading davisbase_tables.tbl file.");
        }
        return exists;
    }
}

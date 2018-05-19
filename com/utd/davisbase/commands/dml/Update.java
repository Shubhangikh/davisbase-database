package com.utd.davisbase.commands.dml;

import com.utd.davisbase.utils.*;
import org.apache.log4j.Logger;

import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.out;

public class Update {
    private final static Logger LOGGER = Logger.getLogger(Update.class.getName());
    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();
    public static String parseUpdateQuery(String updateQuery) {
        String message = "";
        String tableName = "";
        int updateColumnIndex = -1;
        int whereColumnIndex = -1;
        int nullCode = -1;
        String updateColumn = "";
        String whereColumn = "";
        String updateColumnDataType = "";
        String whereColumnDataType = "";
        String updateColumnIsNullable = "";
        String whereColumnIsNullable = "";
        String setValue = "";
        String conditionValue = "";
        String whereOperator = "";
        List<List<String>> tableDetails = new ArrayList<>();
        Pattern pattern = Pattern.compile("(UPDATE|update)[\\s]+([\\w-]+)[\\s]+(SET|set)[\\s]+([\\s\\w-\\=']+(?=WHERE|where)?)[\\s]*(WHERE|where)?[\\s]*([\\s\\w\\=\\-'><!]*)");
        Matcher m = pattern.matcher(updateQuery);
        if(m.matches()) {
            if(m.group(2) != null) {
                tableName = m.group(2).trim();
                if(!Select.checkTableExistence(tableName)) {
                    LOGGER.error("Table does not exist");
                    message = "Table does not exist";
                    return message;
                }
                tableDetails = Select.fetchTableColumns(tableName);
            }
            if(m.group(4) != null) {
                String[] tokens = m.group(4).split("=");
                if(tokens.length < 2) {
                    LOGGER.error("Invalid SET parameters");
                    message = "Invalid SET parameters";
                    return message;
                } else if(tokens.length > 2) {
                    LOGGER.error("Invalid SET parameters. If setting column to a text type value, please enclose within single quotes");
                    message = "Invalid SET parameters. If setting column to a text type value, please enclose within single quotes";
                    return message;
                }
                updateColumn = tokens[0].trim().toLowerCase();
                if(!tableDetails.get(0).contains(updateColumn)) {
                    LOGGER.error("Invalid column name");
                    message = "Invalid column name";
                    return message;
                }
                if(updateColumn.trim().equalsIgnoreCase("ROW_ID")) {
                    LOGGER.error("ROW_ID cannot be updated.");
                    message = "ROW_ID cannot be updated";
                    return message;
                }
                updateColumnIndex = tableDetails.get(0).indexOf(updateColumn);
                updateColumnDataType = tableDetails.get(1).get(updateColumnIndex);
                updateColumnIsNullable = tableDetails.get(2).get(updateColumnIndex);
                setValue = tokens[1].trim();
                if(setValue.equalsIgnoreCase("null") && updateColumnIsNullable.equalsIgnoreCase("no")) {
                    LOGGER.error("Column is not nullable. Cannot set null value");
                    message = "Column is not nullable. Cannot set null value";
                    return message;
                }
                try {
                    switch(updateColumnDataType.toUpperCase()) {
                        case DataTypeNames.TINYINT:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.TINYINT_NULL;
                            } else {
                                Byte.parseByte(setValue);
                            }
                            break;
                        case DataTypeNames.SMALLINT:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.SMALLINT_NULL;
                            } else {
                                Short.parseShort(setValue);
                            }
                            break;
                        case DataTypeNames.INT:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.INT_OR_REAL_NULL;
                            } else {
                                Integer.parseInt(setValue);
                            }
                            break;
                        case DataTypeNames.BIGINT:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL;
                            } else {
                                Long.parseLong(setValue);
                            }
                            break;
                        case DataTypeNames.REAL:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.INT_OR_REAL_NULL;
                            } else {
                                Float.parseFloat(setValue);
                            }
                            break;
                        case DataTypeNames.DOUBLE:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL;
                            } else {
                                Double.parseDouble(setValue);
                            }
                            break;
                        case DataTypeNames.DATETIME:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL;
                            } else {
                                SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                dateTimeFormat.parse(setValue);
                            }
                            break;
                        case DataTypeNames.DATE:
                            if(setValue.equalsIgnoreCase("null")) {
                                if(updateColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Column to be updated is not nullable. Cannot set null value");
                                    message = "Column to be updated is not nullable. Cannot set null value";
                                    return message;
                                }
                                nullCode = DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL;
                            } else {
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                dateFormat.parse(setValue);
                            }
                            break;
                        case DataTypeNames.TEXT:
                            /*Pattern textPattern = Pattern.compile("'(.*)'");
                            Matcher testMatcher = textPattern.matcher(setValue);
                            if(!testMatcher.matches()) {
                                LOGGER.error("Enclose text type value within single quotes");
                                message = "Enclose text type value within single quotes";
                                return message;
                            }*/
                            LOGGER.debug("Text update has not been handled");
                            message = "Text update has not been handled";
                            return message;
                    }
                } catch(NumberFormatException nfe) {
                    LOGGER.error("Incorrect format", nfe);
                    message = "Incorrect format of column value";
                    return message;
                } catch(ParseException pe) {
                    LOGGER.error("Incorrect date/datetime format", pe);
                    message = "Incorrect date/datetime format";
                    return message;
                }
            }
            if (m.group(5) != null && m.group(6) == null) {
                LOGGER.error("WHERE condition missing");
                message = "WHERE condition missing";
                return message;
            } else if(m.group(5) == null && (!m.group(6).equals(""))) {
                LOGGER.error("WHERE keyword missing");
                message = "WHERE keyword missing";
                return message;
            }
            if(m.group(6) != null && !m.group(6).equals("")) {
                String tokens[] = m.group(6).split("[\\s]{1}(?=(?:[^\']*\'[^\']*\')*[^\']*$)", -1);
                whereColumn = tokens[0].trim();
                whereColumnIndex = tableDetails.get(0).indexOf(whereColumn);
                whereColumnDataType = tableDetails.get(1).get(whereColumnIndex);
                whereColumnIsNullable = tableDetails.get(2).get(whereColumnIndex);
                if(tokens.length < 3) {
                    LOGGER.error("Invalid WHERE condition");
                    message = "Invalid WHERE condition";
                    return message;
                }
                if(tokens.length == 4) {
                    if(!m.group(6).trim().contains("is not null")) {
                        LOGGER.error("Invalid WHERE condition");
                        message = "Invalid WHERE condition";
                        return message;
                    } else {
                        whereOperator = "is not null";
                    }
                }
                if(tokens.length > 4) {
                    LOGGER.error("Invalid WHERE condition");
                    message = "Invalid WHERE condition";
                    return message;
                }
                if(m.group(6).contains("is null") && tokens.length != 3) {
                    LOGGER.error("Invalid WHERE condition");
                    message = "Invalid WHERE condition";
                    return message;
                } else if(m.group(6).contains("is null") && tokens.length == 3) {
                    whereOperator = "is null";
                }
                if(tokens.length == 3 && !m.group(6).contains("is null") && !m.group(6).contains("is not null")) {
                    whereOperator = tokens[1].trim();
                    conditionValue = tokens[2].trim();
                }
                try {
                    switch (whereColumnDataType.toUpperCase()) {
                        case DataTypeNames.TINYINT:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Byte.parseByte(conditionValue);
                            }
                            break;
                        case DataTypeNames.SMALLINT:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Short.parseShort(conditionValue);
                            }
                            break;
                        case DataTypeNames.INT:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Integer.parseInt(conditionValue);
                            }
                            break;
                        case DataTypeNames.BIGINT:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Long.parseLong(conditionValue);
                            }
                            break;
                        case DataTypeNames.REAL:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Float.parseFloat(conditionValue);
                            }
                            break;
                        case DataTypeNames.DOUBLE:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Double.parseDouble(conditionValue);
                            }
                            break;
                        case DataTypeNames.DATETIME:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                dateTimeFormat.parse(conditionValue);
                            }
                            break;
                        case DataTypeNames.DATE:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                dateFormat.parse(conditionValue);
                            }
                            break;
                        case DataTypeNames.TEXT:
                            if(conditionValue.equalsIgnoreCase("null")) {
                                if (whereColumnIsNullable.equalsIgnoreCase("no")) {
                                    LOGGER.error("Value of condition column cannot be null. It is not nullable");
                                    message = "Value of condition column cannot be null. It is not nullable";
                                    return message;
                                }
                            } else {
                                Pattern textPattern = Pattern.compile("'(.*)'");
                                Matcher testMatcher = textPattern.matcher(conditionValue);
                                if (!testMatcher.matches()) {
                                    LOGGER.error("Enclose text type value within single quotes");
                                    message = "Enclose text type value within single quotes";
                                    return message;
                                }
                            }
                    }
                } catch(NumberFormatException nfe) {
                    LOGGER.error("Incorrect format", nfe);
                    message = "Incorrect format of column value";
                    return message;
                } catch(ParseException pe) {
                    LOGGER.error("Incorrect date/datetime format", pe);
                    message = "Incorrect date/datetime format";
                    return message;
                }

            }
            List<List<String>> records = Select.fetchAllRecords(tableName);
            if(whereColumnIndex == -1) {
                for(List<String> record: records) {
                    record.set(updateColumnIndex, setValue);
                }
            } else {
                for(List<String> record: records){
                    try {
                        switch (whereOperator.toUpperCase()) {
                            case Operator.EQUALS:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) == Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) == Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) == Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) == Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) == Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) == Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) == 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) == 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.NOT_EQUALS:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) != Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) != Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) != Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) != Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) != Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) != Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) != 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) != 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if (!(record.get(whereColumnIndex).equalsIgnoreCase(conditionValue))) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.GREATER_THAN:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) > Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) > Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) > Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) > Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) > Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) > Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) > 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) > 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if ((record.get(whereColumnIndex).compareTo(conditionValue)) > 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.GREATER_THAN_EQUALS:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) >= Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) >= Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) >= Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) >= Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) >= Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) >= Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) >= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) >= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if ((record.get(whereColumnIndex).compareTo(conditionValue)) >= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.LESS_THAN:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) < Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) < Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) < Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) < Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) < Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) < Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) < 0) {
                                            record.set(updateColumnIndex, setValue);;
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) < 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if ((record.get(whereColumnIndex).compareTo(conditionValue)) < 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.LESS_THAN_EQUALS:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (Byte.parseByte(record.get(whereColumnIndex)) <= Byte.parseByte(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);;
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (Short.parseShort(record.get(whereColumnIndex)) <= Short.parseShort(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);;
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (Integer.parseInt(record.get(whereColumnIndex)) <= Integer.parseInt(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (Long.parseLong(record.get(whereColumnIndex)) <= Long.parseLong(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (Float.parseFloat(record.get(whereColumnIndex)) <= Float.parseFloat(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (Double.parseDouble(record.get(whereColumnIndex)) <= Double.parseDouble(conditionValue)) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                        Date savedDateValue = dateTimeFormat.parse(record.get(whereColumnIndex));
                                        Date inputDateTime = dateTimeFormat.parse(conditionValue);
                                        if (savedDateValue.compareTo(inputDateTime) <= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        Date savedValue = dateFormat.parse(record.get(whereColumnIndex));
                                        Date input = dateFormat.parse(conditionValue);
                                        if (savedValue.compareTo(input) <= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if ((record.get(whereColumnIndex).compareTo(conditionValue)) <= 0) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.IS_NULL:
                                switch (whereColumnDataType.toUpperCase()) {
                                    case DataTypeNames.TINYINT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.SMALLINT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.INT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.BIGINT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.REAL:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DOUBLE:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATETIME:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.DATE:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                        break;
                                    case DataTypeNames.TEXT:
                                        if (record.get(whereColumnIndex).equalsIgnoreCase("null")) {
                                            record.set(updateColumnIndex, setValue);
                                        }
                                }
                                break;
                            case Operator.IS_NOT_NULL:
                                if (!(record.get(whereColumnIndex).equalsIgnoreCase("null"))) {
                                    record.set(updateColumnIndex, setValue);
                                }
                                break;
                        }
                    } catch (ParseException pe) {
                        LOGGER.error("Invalid date/datetime formats");
                        message = "Invalid date/datetime formats";
                        return message;
                    }
                }
            }
            message = updateData(tableName, records, tableDetails, updateColumnIndex, nullCode);
            return message;
        } else {
            LOGGER.error("Invalid UPDATE query");
            message = "Invalid UPDATE query";
            return message;
        }
    }

    private static String updateData(String tableName, List<List<String>> records, List<List<String>> tableDetails, int updateColumnIndex, int nullCode) {
        String message = "";
        RandomAccessFile table;
        List<Integer> recordStartPositions = new ArrayList<>();
        try {
            table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
            long pageCount = table.length()/davisBaseUtils.PAGE_SIZE;
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
            for(int pos = 0; pos < recordStartPositions.size(); pos++) {
                List<String> record = records.get(pos);
                if(nullCode != -1) {
                    table.seek(recordStartPositions.get(pos) + davisBaseUtils.COLUMNS_COUNT_OFFSET + updateColumnIndex - 1);
                    table.writeByte(nullCode);
                }
                table.seek(recordStartPositions.get(pos) + davisBaseUtils.COLUMNS_COUNT_OFFSET + record.size() - 1);
                for(int j = 1; j < tableDetails.get(1).size(); j++) {
                    switch (tableDetails.get(1).get(j).toUpperCase()) {
                        case DataTypeNames.TINYINT:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeByte(0x00);
                            } else {
                                table.writeByte(Byte.parseByte(record.get(j)));
                            }
                            break;
                        case DataTypeNames.SMALLINT:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeShort(0x00);
                            } else {
                                table.writeShort(Short.parseShort(record.get(j)));
                            }
                            break;
                        case DataTypeNames.INT:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeInt(0x00);
                            } else {
                                table.writeInt(Integer.parseInt(record.get(j)));
                            }
                            break;
                        case DataTypeNames.BIGINT:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeLong(0x00);
                            } else {
                                table.writeLong(Long.parseLong(record.get(j)));
                            }
                            break;
                        case DataTypeNames.REAL:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeFloat(0x00);
                            } else {
                                table.writeFloat(Float.parseFloat(record.get(j)));
                            }
                            break;
                        case DataTypeNames.DOUBLE:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeDouble(0x00);
                            } else {
                                table.writeDouble(Double.parseDouble(record.get(j)));
                            }
                            break;
                        case DataTypeNames.DATE:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeLong(0x00);
                            } else {
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                                Date date = format.parse(record.get(j));
                                table.writeLong(date.getTime());
                            }
                            break;
                        case DataTypeNames.DATETIME:
                            if(record.get(j).equalsIgnoreCase("null")) {
                                table.writeLong(0x00);
                            } else {
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                                Date dateTime = format.parse(record.get(j));
                                table.writeLong(dateTime.getTime());
                            }
                            break;
                        case DataTypeNames.TEXT:
                            if(!record.get(j).equalsIgnoreCase("null")) {
                                table.writeBytes(record.get(j));
                            }
                            break;
                    }
                }
            }
            message = "Record updated successfully";
        } catch (Exception e) {
            LOGGER.error("Error updating table " + tableName, e);
            message = "Error updating table " + tableName;
            return message;
        }
        return message;
    }

    public static void updateRecordCount(String tableName, int recordCount) {
        List<Integer> recordStartPositions = new ArrayList<>();
        RandomAccessFile davisBaseTables;
        try {
            davisBaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            long pageCount = davisBaseTables.length()/davisBaseUtils.PAGE_SIZE;
            int position;
            for(int i = 1; i <= pageCount; i++) {
                davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.PAGE_TYPE_OFFSET);
                if(davisBaseTables.readByte() == davisBaseUtils.PAGE_TYPE_LEAF) {
                    davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                    while ((position = (int) davisBaseTables.readShort()) != 0) {
                        recordStartPositions.add(position);
                    }
                }
            }
            for (int recordStartPosition : recordStartPositions) {
                davisBaseTables.seek(recordStartPosition + davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE);
                int columnCount = davisBaseTables.readByte();
                int tableNameLength = davisBaseTables.readByte() - 0x0C;
                byte[] tableNameBytes = new byte[tableNameLength];
                davisBaseTables.seek(davisBaseTables.getFilePointer() + columnCount - 1);
                for (int k = 0; k < tableNameLength; k++) {
                    tableNameBytes[k] = davisBaseTables.readByte();
                }
                String tableNameRetrieved = new String(tableNameBytes);
                if (tableName.equalsIgnoreCase(tableNameRetrieved)) {
                    davisBaseTables.writeInt(recordCount);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading davisbase_tables.tbl file.");
        }
    }
    public static void updateRootPage(String tableName, int rootPage) {
        List<Integer> recordStartPositions = new ArrayList<>();
        RandomAccessFile davisBaseTables;
        try {
            davisBaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            long pageCount = davisBaseTables.length()/davisBaseUtils.PAGE_SIZE;
            int position;
            for(int i = 1; i <= pageCount; i++) {
                long pageTypeOffset = davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.PAGE_TYPE_OFFSET;
                davisBaseTables.seek(pageTypeOffset);
                if(davisBaseTables.readByte() == davisBaseUtils.PAGE_TYPE_LEAF) {
                    davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                    while ((position = (int) davisBaseTables.readShort()) != 0) {
                        recordStartPositions.add(position);
                    }
                }
            }
            for (int recordStartPosition : recordStartPositions) {
                davisBaseTables.seek(recordStartPosition + davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE);
                int columnCount = davisBaseTables.readByte();
                int tableNameLength = davisBaseTables.readByte() - 0x0C;
                byte[] tableNameBytes = new byte[tableNameLength];
                davisBaseTables.seek(davisBaseTables.getFilePointer() + columnCount - 1);
                for (int k = 0; k < tableNameLength; k++) {
                    tableNameBytes[k] = davisBaseTables.readByte();
                }
                String tableNameRetrieved = new String(tableNameBytes);
                if (tableName.equalsIgnoreCase(tableNameRetrieved)) {
                    davisBaseTables.readShort();
                    davisBaseTables.writeInt(rootPage);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading davisbase_tables.tbl file.");
        }
    }

}

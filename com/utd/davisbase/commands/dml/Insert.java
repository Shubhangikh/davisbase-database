package com.utd.davisbase.commands.dml;

import com.utd.davisbase.table.BPlusTreeHandler;
import com.utd.davisbase.utils.DataTypeCode;
import com.utd.davisbase.utils.DataTypeContentSize;
import com.utd.davisbase.utils.DataTypeNames;
import com.utd.davisbase.utils.DavisBaseUtils;
import org.apache.log4j.Logger;

import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Insert {
    private final static Logger LOGGER = Logger.getLogger(Insert.class.getName());

    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();
    private static int rowId = 0;
    private static long startOfContent = 0;

    public static String parseInsertQuery(String insertQuery) {
        String message = "";
        String tableName = "";
        List<String> tableColumns;
        List<List<String>> tableDetails = new ArrayList<>();
        Map<String, Integer> ordinalPositions = new LinkedHashMap<>();
        List<String> valuesToMapInDB = new ArrayList<>();
        Pattern pattern = Pattern.compile("(INSERT|insert)[\\s]+(INTO|into)[\\s]+([\\w-]+)[\\s]*\\(([\\w\\s,-]+)\\)[\\s]+(VALUES|values)[\\s]*\\(([\\w\\s',-:]+)\\)");
        Matcher m = pattern.matcher(insertQuery);
        try {
            if (m.matches()) {
                if (m.group(3) != null) {
                    tableName = m.group(3);
                    if (!Select.checkTableExistence(tableName)) {
                        LOGGER.error("Table does not exist");
                        message = "Table does not exist";
                        return message;
                    }
                }
                if (m.group(4) != null) {
                    tableDetails = Select.fetchTableColumns(tableName);
                    tableColumns = tableDetails.get(0);
                    String[] columnNames = m.group(4).trim().replaceAll(" ", "").split(",");
                    // Check if columns count is valid
                    if (columnNames.length != tableColumns.size()) {
                        LOGGER.error("Column names list is missing few columns");
                        message = "Column names list is missing few columns";
                        return message;
                    }
                    // Check for validity of column names
                    List<String> columnsSet = new ArrayList<>(Arrays.asList(columnNames));
                    if (!columnsSet.containsAll(tableColumns)) {
                        LOGGER.error("Column names list is missing few columns");
                        message = "Column names list is missing few columns";
                        return message;
                    }
                    for (int i = 0; i < tableColumns.size(); i++) {
                        ordinalPositions.put(tableColumns.get(i), columnsSet.indexOf(tableColumns.get(i)));
                    }
                }
                if (m.group(6) != null) {
                    List<String> dataTypes = tableDetails.get(1);
                    List<String> isNullable = tableDetails.get(2);
                    String[] columnValues = m.group(6).split(",(?=(?:[^\']*\'[^\']*\')*[^\']*$)", -1);
                    if (columnValues.length != ordinalPositions.size()) {
                        LOGGER.error("Not enough column values to map");
                        message = "Not enough column values to map";
                        return message;
                    }
                    int index = 0;
                    for (Map.Entry<String, Integer> entry : ordinalPositions.entrySet()) {
                        int colIndex = entry.getValue();
                        String userInput = columnValues[colIndex].trim();
                        switch(dataTypes.get(index).toUpperCase()) {
                            case DataTypeNames.TINYINT: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Byte.parseByte(userInput);
                                }
                                break;
                            case DataTypeNames.SMALLINT: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Short.parseShort(userInput);
                                }
                                break;
                            case DataTypeNames.INT: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Integer.parseInt(userInput);
                                }
                                break;
                            case DataTypeNames.BIGINT: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Long.parseLong(userInput);
                                }
                                break;
                            case DataTypeNames.REAL: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Float.parseFloat(userInput);
                                }
                                break;
                            case DataTypeNames.DOUBLE: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    Double.parseDouble(userInput);
                                }
                                break;
                            case DataTypeNames.DATE: if(userInput.equalsIgnoreCase("null")) {
                                    if(isNullable.get(index).equalsIgnoreCase("no")) {
                                        LOGGER.error("NULL value for a column which cannot be null");
                                        message = "NULL value for a column which cannot be null";
                                        return message;
                                    }
                                } else {
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                    dateFormat.parse(userInput);
                                }
                                break;
                            case DataTypeNames.DATETIME: if(userInput.equalsIgnoreCase("null")) {
                                if(isNullable.get(index).equalsIgnoreCase("no")) {
                                    LOGGER.error("NULL value for a column which cannot be null");
                                    message = "NULL value for a column which cannot be null";
                                    return message;
                                }
                            } else {
                                SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd_HH:mm:ss");
                                dateFormat.parse(userInput);
                            }
                            break;
                            case DataTypeNames.TEXT: if(userInput.equalsIgnoreCase("null")) {
                                if(isNullable.get(index).equalsIgnoreCase("no")) {
                                    LOGGER.error("NULL value for a column which cannot be null");
                                    message = "NULL value for a column which cannot be null";
                                    return message;
                                }
                            } else {
                                Pattern textPattern = Pattern.compile("'(.*)'");
                                Matcher matcher = textPattern.matcher(userInput);
                                if(!matcher.matches()) {
                                    LOGGER.error("String type data must be enclosed within single quotes(ex. 'data')");
                                    message = "String type data must be enclosed within single quotes(ex. 'data')";
                                    return message;
                                }
                                if(matcher.group(1) != null) {
                                    userInput = matcher.group(1);
                                }
                            }
                            break;
                        }
                        valuesToMapInDB.add(userInput);
                        index++;
                    }
                }
                message = insertRecord(tableName, tableDetails, valuesToMapInDB);
            } else {
                LOGGER.error("Invalid INSERT query");
                message = "Invalid INSERT query";
                return message;
            }
        } catch(ClassCastException e) {
            LOGGER.error("Invalid data type of one or more column values");
            message = "Invalid data type of one or more column values";
            return message;
        } catch (ParseException pe) {
            LOGGER.error("Invalid date format");
            message = "Invalid date format";
            return message;
        }
        return message;
    }

    private static String insertRecord(String tableName, List<List<String>> tableDetails, List<String> valuesToMapInDB) {
        RandomAccessFile table;
        BPlusTreeHandler treeHandler = new BPlusTreeHandler();
        List<String> columnNames = tableDetails.get(0);
        List<String> columnDataTypes = tableDetails.get(1);
        try {
            table = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
            int pageNumber = treeHandler.getLeafPageNumber("davisbase_tables", table);
            int rowId = Select.parseSelectQuery("SELECT RECORD_COUNT FROM DAVISBASE_TABLES WHERE TABLE_NAME = '" + tableName + "'", false) + 1;
            int start = davisBaseUtils.PAGE_SIZE * (pageNumber - 1);
            table.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
            int cellCount = table.readByte();
            int cellContentStartPosition = table.readShort();
            int payloadWriteStart = 0;
            int payloadSize = davisBaseUtils.COLUMNS_COUNT_SIZE + columnNames.size() - 1;
            for(int k = 1; k < columnDataTypes.size(); k++) {
                switch (columnDataTypes.get(k).toUpperCase()) {
                    case DataTypeNames.TINYINT:
                        payloadSize += DataTypeContentSize.TINYINT;
                        break;
                    case DataTypeNames.SMALLINT:
                        payloadSize += DataTypeContentSize.SMALLINT;
                        break;
                    case DataTypeNames.INT:
                        payloadSize += DataTypeContentSize.INT;
                        break;
                    case DataTypeNames.BIGINT:
                        payloadSize += DataTypeContentSize.BIGINT;
                        break;
                    case DataTypeNames.REAL:
                        payloadSize += DataTypeContentSize.REAL;
                        break;
                    case DataTypeNames.DOUBLE:
                        payloadSize += DataTypeContentSize.DOUBLE;
                        break;
                    case DataTypeNames.DATETIME:
                        payloadSize += DataTypeContentSize.DATETIME;
                        break;
                    case DataTypeNames.DATE:
                        payloadSize += DataTypeContentSize.DATE;
                        break;
                    case DataTypeNames.TEXT:
                        payloadSize += DataTypeContentSize.TEXT + valuesToMapInDB.get(k).length();
                }
            }
            if(cellContentStartPosition != 0) {
                int availableSpace = cellContentStartPosition - (start + davisBaseUtils.RECORDS_ARRAY_OFFSET + (cellCount*2));
                if(availableSpace < payloadSize) {
                    Map<String, Integer> parameters = treeHandler.extendTree(tableName, table, (int)pageNumber, rowId - 1);
                    start = davisBaseUtils.PAGE_SIZE * (parameters.get("NEW-LEAF-PAGE-NMBR") - 1);
                    table.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
                    table.writeByte(0x01);
                    cellCount = parameters.get("NEW-PAGE-CELL-COUNT");
                    pageNumber = parameters.get("NEW-LEAF-PAGE-NMBR");
                    payloadWriteStart = davisBaseUtils.PAGE_SIZE * pageNumber  - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize);

                } else {
                    payloadWriteStart = cellContentStartPosition - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize);
                }
            } else {
                payloadWriteStart = davisBaseUtils.PAGE_SIZE * (pageNumber) - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize);
            }
            table.seek(payloadWriteStart);
            table.writeShort(payloadSize);
            table.writeInt(rowId);
            table.writeByte(columnNames.size() - 1);
            for(int i = 1; i < valuesToMapInDB.size(); i++) {
                switch(columnDataTypes.get(i).toUpperCase()) {
                    case DataTypeNames.TINYINT:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.TINYINT_NULL);
                        } else {
                            table.writeByte(DataTypeCode.TINYINT);
                        }
                        break;
                    case DataTypeNames.SMALLINT:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.SMALLINT_NULL);
                        } else {
                            table.writeByte(DataTypeCode.SMALLINT);
                        }
                        break;
                    case DataTypeNames.INT:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.INT_OR_REAL_NULL);
                        } else {
                            table.writeByte(DataTypeCode.INT);
                        }
                        break;
                    case DataTypeNames.BIGINT:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL);
                        } else {
                            table.writeByte(DataTypeCode.BIGINT);
                        }
                        break;
                    case DataTypeNames.REAL:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.INT_OR_REAL_NULL);
                        } else {
                            table.writeByte(DataTypeCode.REAL);
                        }
                        break;
                    case DataTypeNames.DOUBLE:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL);
                        } else {
                            table.writeByte(DataTypeCode.DOUBLE);
                        }
                        break;
                    case DataTypeNames.DATE:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL);
                        } else {
                            table.writeByte(DataTypeCode.DATE);
                        }
                        break;
                    case DataTypeNames.DATETIME:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.DBL_OR_BIGINT_OR_DATE_OR_DATE_NULL);
                        } else {
                            table.writeByte(DataTypeCode.DATETIME);
                        }
                        break;
                    case DataTypeNames.TEXT:
                        if(valuesToMapInDB.get(i).equalsIgnoreCase("null")) {
                            table.writeByte(DataTypeCode.TEXT);
                        } else {
                            table.writeByte(DataTypeCode.TEXT + valuesToMapInDB.get(i).length());
                        }
                        break;
                }
            }
            for(int j = 1; j < valuesToMapInDB.size(); j++) {
                switch(columnDataTypes.get(j).toUpperCase()) {
                    case DataTypeNames.TINYINT:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeByte(0x00);
                        } else {
                            table.writeByte(Byte.parseByte(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.SMALLINT:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeShort(0x00);
                        } else {
                            table.writeShort(Short.parseShort(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.INT:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeInt(0x00);
                        } else {
                            table.writeInt(Integer.parseInt(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.BIGINT:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeLong(0x00);
                        } else {
                            table.writeLong(Long.parseLong(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.REAL:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeFloat(0x00);
                        } else {
                            table.writeFloat(Float.parseFloat(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.DOUBLE:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeDouble(0x00);
                        } else {
                            table.writeDouble(Double.parseDouble(valuesToMapInDB.get(j)));
                        }
                        break;
                    case DataTypeNames.DATE:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeLong(0x00);
                        } else {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                            Date date = format.parse(valuesToMapInDB.get(j));
                            table.writeLong(date.getTime());
                        }
                        break;
                    case DataTypeNames.DATETIME:
                        if(valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeLong(0x00);
                        } else {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                            Date dateTime = format.parse(valuesToMapInDB.get(j));
                            table.writeLong(dateTime.getTime());
                        }
                        break;
                    case DataTypeNames.TEXT:
                        if(!valuesToMapInDB.get(j).equalsIgnoreCase("null")) {
                            table.writeBytes(valuesToMapInDB.get(j));
                        }
                        break;
                }
            }
            table.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
            table.writeByte(cellCount + 1);
            // Set file pointer to content start position offset
            table.seek(start + davisBaseUtils.START_OF_CONTENT_OFFSET);
            // Write content start position
            table.writeShort(payloadWriteStart);
            // Page number of leaf to the right, -1 (0xFFFFFFFF) if rightmost
            table.writeInt(0xFFFFFFFF);
            // Write current record location to the array
            table.seek(start + davisBaseUtils.RECORDS_ARRAY_OFFSET + cellCount * 2);
            // Write start of content position for current record
            table.writeShort(payloadWriteStart);
            table.close();
            Update.updateRecordCount(tableName, rowId);
        } catch (Exception e) {
            LOGGER.error("Failed to insert the record", e);
            System.out.println("Failed to insert the record");
        }
        return "1 row inserted";
    }

    public static int insertRecordIntoDavisBaseTables(String tableName) {
        RandomAccessFile davisBaseTablesCatalog;
        BPlusTreeHandler treeHandler = new BPlusTreeHandler();
        rowId = 0;
        if(Select.checkTableExistence("davisbase_tables") && !tableName.equalsIgnoreCase("davisbase_columns")) {
            rowId = Select.parseSelectQuery("SELECT RECORD_COUNT FROM DAVISBASE_TABLES WHERE TABLE_NAME = 'DAVISBASE_TABLES'", false) + 1;
        } else {
            if(tableName.equalsIgnoreCase("davisbase_columns")) {
                rowId = 2;
            } else {
                rowId = 1;
            }
        }
        int columnsCount = davisBaseUtils.DAVISBASE_TABLES_COLUMNS_COUNT - 1;
        try {
            davisBaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
            long pageNumber = treeHandler.getLeafPageNumber("davisbase_tables", davisBaseTablesCatalog);
            long start = davisBaseUtils.PAGE_SIZE * (pageNumber - 1);
            davisBaseTablesCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
            int cellCount = davisBaseTablesCatalog.readByte();
            int recordCount = rowId;
            davisBaseTablesCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
            davisBaseTablesCatalog.writeByte(cellCount + 1);
            // Calculate the payload size and write data into file
            int payloadSize = davisBaseUtils.COLUMNS_COUNT_SIZE + columnsCount;
            payloadSize += tableName.length() + DataTypeContentSize.INT + DataTypeContentSize.SMALLINT;

            if(cellCount == 0) {
                davisBaseTablesCatalog.seek(pageNumber * davisBaseUtils.PAGE_SIZE - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                startOfContent = davisBaseTablesCatalog.getFilePointer();
            } else {
                davisBaseTablesCatalog.seek(start + davisBaseUtils.START_OF_CONTENT_OFFSET);
                int contentStartPosition = davisBaseTablesCatalog.readShort();
                int availableSpace = contentStartPosition - ((int)start + davisBaseUtils.RECORDS_ARRAY_OFFSET + (cellCount*2));
                // check if empty space equivalent to payload is available on the page, add another page if not
                if(availableSpace < payloadSize) {
                    //pageCount = davisBaseTablesCatalog.length()/davisBaseUtils.PAGE_SIZE;
                    Map<String, Integer> parameters = treeHandler.extendTree("davisbase_tables", davisBaseTablesCatalog, (int)pageNumber, rowId - 1);
                    start = davisBaseUtils.PAGE_SIZE * (parameters.get("NEW-LEAF-PAGE-NMBR") - 1);
                    davisBaseTablesCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
                    davisBaseTablesCatalog.writeByte(0x01);
                    cellCount = parameters.get("NEW-PAGE-CELL-COUNT");
                    davisBaseTablesCatalog.seek(davisBaseUtils.PAGE_SIZE*parameters.get("NEW-LEAF-PAGE-NMBR") - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                    startOfContent = davisBaseTablesCatalog.getFilePointer();
                } else {
                    davisBaseTablesCatalog.seek(contentStartPosition - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                    startOfContent = davisBaseTablesCatalog.getFilePointer();
                }
            }
            // Write size of payload (excluding cell header size of 6 bytes) to the file at current file pointer
            davisBaseTablesCatalog.writeShort(payloadSize);
            // Write row_id
            davisBaseTablesCatalog.writeInt(rowId);
            // Write count of columns excluding row_id
            davisBaseTablesCatalog.writeByte(columnsCount);
            // Write table_name column's value size (0x0C + length of table_name string)
            davisBaseTablesCatalog.writeByte(DataTypeCode.TEXT + tableName.length());
            // Write size of record_count column value
            davisBaseTablesCatalog.writeByte(DataTypeCode.INT);
            // Write root page size
            davisBaseTablesCatalog.writeByte(DataTypeCode.SMALLINT);
            // Write table name
            davisBaseTablesCatalog.writeBytes(tableName);
            // Write record_count
            if(tableName.equalsIgnoreCase("davisbase_columns")) {
                davisBaseTablesCatalog.writeInt(4);
            } else if(tableName.equalsIgnoreCase("davisbase_tables")){
                davisBaseTablesCatalog.writeInt(recordCount);
            } else {
                davisBaseTablesCatalog.writeInt(0);
            }

            // Write root page number
            davisBaseTablesCatalog.writeShort(0x01);

            // Set file pointer to content start position offset
            davisBaseTablesCatalog.seek(start + davisBaseUtils.START_OF_CONTENT_OFFSET);
            // Write content start position
            davisBaseTablesCatalog.writeShort((int)startOfContent);
            // Page number of leaf to the right, -1 (0xFFFFFFFF) if rightmost
            davisBaseTablesCatalog.writeInt(0xFFFFFFFF);
            // Write current record location to the array
            davisBaseTablesCatalog.seek(start + davisBaseUtils.RECORDS_ARRAY_OFFSET + cellCount * 2);
            // Write start of content position for current record
            davisBaseTablesCatalog.writeShort((int)startOfContent);
            davisBaseTablesCatalog.close();
            Update.updateRecordCount("davisbase_tables", rowId);
        }
        catch (Exception e) {
            LOGGER.error("Unable to insert record to database_tables file ", e);
        }
        return 1;
    }

    public static int insertRecordIntoDavisBaseColumns(String tableName, List<String> columnNamesList, List<String> columnDataTypeList,
                                                      List<String> columnConstraintList) {
        rowId = 0;
        int numOfRecordsInserted = 0;
        int columnsCount = davisBaseUtils.DAVISBASE_COLUMNS_COLUMNS_COUNT - 1;
        RandomAccessFile davisBaseColumnsCatalog;
        BPlusTreeHandler treeHandler = new BPlusTreeHandler();
        try {
            davisBaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
            long pageNumber;
            if(tableName.equalsIgnoreCase("davisbase_columns") || tableName.equalsIgnoreCase("davisbase_tables")) {
                pageNumber = 1;
            } else {
                pageNumber = treeHandler.getLeafPageNumber("davisbase_columns", davisBaseColumnsCatalog);
            }

            long start = davisBaseUtils.PAGE_SIZE * (pageNumber - 1);
            for(int index = 0; index < columnDataTypeList.size(); index++) {
                if(Select.checkTableExistence("davisbase_columns")) {
                    rowId = Select.parseSelectQuery("SELECT RECORD_COUNT FROM DAVISBASE_TABLES WHERE TABLE_NAME = 'DAVISBASE_COLUMNS'", false) + 1;
                } else {
                    rowId++;
                }
                String columnName = columnNamesList.get(index).toLowerCase();
                String dataType = columnDataTypeList.get(index).toLowerCase();
                String constraint = columnConstraintList.get(index).toLowerCase();
                davisBaseColumnsCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
                int cellCount = davisBaseColumnsCatalog.readByte();
                // Calculate the payload size and write data into file
                int payloadSize = davisBaseUtils.COLUMNS_COUNT_SIZE + columnsCount;
                payloadSize += tableName.toLowerCase().length() + columnName.length() +
                        dataType.length() + DataTypeContentSize.TINYINT +
                        constraint.length();
                if(index == 0) {
                    payloadSize += 3;   // Adding 3 to payloadSize for "PRI" for PRIMARY KEY, 0 (NULL) otherwise as COLUMN_KEY column's value
                }

                if(cellCount != 0) {
                    davisBaseColumnsCatalog.seek(start + davisBaseUtils.START_OF_CONTENT_OFFSET);
                    int contentStartPosition = davisBaseColumnsCatalog.readShort();
                    int availableSpace = contentStartPosition - ((int)start + davisBaseUtils.RECORDS_ARRAY_OFFSET + (cellCount*2));
                    // check if empty space equivalent to payload is available on the page, add another page if not
                    if(availableSpace < payloadSize) {
                        // pageCount = davisBaseColumnsCatalog.length()/davisBaseUtils.PAGE_SIZE;
                        Map<String, Integer> parameters = treeHandler.extendTree("davisbase_columns", davisBaseColumnsCatalog, (int)pageNumber, rowId - 1);
                        start = davisBaseUtils.PAGE_SIZE * (parameters.get("NEW-LEAF-PAGE-NMBR") - 1);
                        davisBaseColumnsCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
                        davisBaseColumnsCatalog.writeByte(0x01);
                        cellCount = parameters.get("NEW-PAGE-CELL-COUNT");
                        davisBaseColumnsCatalog.seek(davisBaseUtils.PAGE_SIZE*parameters.get("NEW-LEAF-PAGE-NMBR") - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                        startOfContent = davisBaseColumnsCatalog.getFilePointer();
                    } else {
                        davisBaseColumnsCatalog.seek(contentStartPosition - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                        startOfContent = davisBaseColumnsCatalog.getFilePointer();
                    }
                } else {
                    davisBaseColumnsCatalog.seek(pageNumber * davisBaseUtils.PAGE_SIZE - (davisBaseUtils.LEAF_PAGE_CELL_HEADER_SIZE + payloadSize));
                    startOfContent = davisBaseColumnsCatalog.getFilePointer();
                }
                davisBaseColumnsCatalog.writeShort(payloadSize);
                davisBaseColumnsCatalog.writeInt(rowId);
                davisBaseColumnsCatalog.writeByte(columnsCount);
                davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT + tableName.length());
                davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT + columnName.length());
                davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT + dataType.length());
                davisBaseColumnsCatalog.writeByte(DataTypeCode.TINYINT);
                davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT + constraint.length());
                if(index == 0) {
                    davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT + 3);
                } else {
                    davisBaseColumnsCatalog.writeByte(DataTypeCode.TEXT);
                }
                davisBaseColumnsCatalog.writeBytes(tableName);
                davisBaseColumnsCatalog.writeBytes(columnName);
                davisBaseColumnsCatalog.writeBytes(dataType);
                davisBaseColumnsCatalog.writeByte(index + 1);
                davisBaseColumnsCatalog.writeBytes(constraint);
                if(index == 0) {
                    davisBaseColumnsCatalog.writeBytes("PRI");
                }

                davisBaseColumnsCatalog.seek(start + davisBaseUtils.RECORD_COUNT_OFFSET);
                davisBaseColumnsCatalog.writeByte(cellCount + 1);
                // Set file pointer to content start position offset
                davisBaseColumnsCatalog.seek(start + davisBaseUtils.START_OF_CONTENT_OFFSET);
                // Write content start position
                davisBaseColumnsCatalog.writeShort((int)startOfContent);
                // Page number of leaf to the right, -1 (0xFFFFFFFF) if rightmost
                davisBaseColumnsCatalog.writeInt(0xFFFFFFFF);
                // Write current record location to the array
                davisBaseColumnsCatalog.seek(start + davisBaseUtils.RECORDS_ARRAY_OFFSET + cellCount * 2);
                // Write start of content position for current record
                davisBaseColumnsCatalog.writeShort((int)startOfContent);
                if(Select.checkTableExistence("davisbase_columns")) {
                    Update.updateRecordCount("davisbase_columns", rowId);
                }
                numOfRecordsInserted++;
            }
            davisBaseColumnsCatalog.close();
        } catch(Exception e) {
            LOGGER.error("Unable to insert record to database_columns file ", e);
        }
        return numOfRecordsInserted;
    }
}

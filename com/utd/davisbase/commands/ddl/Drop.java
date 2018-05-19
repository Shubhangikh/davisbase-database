package com.utd.davisbase.commands.ddl;

import com.utd.davisbase.commands.dml.Select;
import com.utd.davisbase.utils.DavisBaseUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Drop {
    private final static Logger LOGGER = Logger.getLogger(Drop.class.getName());
    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();

    public static String parseDropTableQuery(String dropQuery) {
        String message = "";
        String tableName = "";
        RandomAccessFile davisBaseTables;
        RandomAccessFile davisBaseColumns;
        List<Integer> recordStartPositions;
        File fileToDelete;
        Pattern pattern = Pattern.compile("(DROP|drop)[\\s]+(TABLE|table)[\\s]+([\\s\\w-']+)");
        Matcher m = pattern.matcher(dropQuery);
        if(m.matches()) {
            if(m.group(3) != null) {
                tableName = m.group(3).trim();
                if(!Select.checkTableExistence(tableName)) {
                    LOGGER.error("Unknown table " + tableName);
                    message = "Unknown table " + tableName;
                    return message;
                }
            }
            try {
                // Remove record from davisbase_tables where table_name = <table-name>
                davisBaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
                long pageCount = davisBaseTables.length()/davisBaseUtils.PAGE_SIZE;
                int position;
                for(int i = 1; i <= pageCount; i++) {
                    recordStartPositions = new ArrayList<>();
                    davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORD_COUNT_OFFSET);
                    int count = davisBaseTables.readByte();
                    int columnsCount = davisBaseUtils.DAVISBASE_TABLES_COLUMNS_COUNT - 1;
                    davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.PAGE_TYPE_OFFSET);
                    if(davisBaseTables.readByte() == davisBaseUtils.PAGE_TYPE_LEAF) {
                        davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                        while ((position = (int) davisBaseTables.readShort()) != 0) {
                            recordStartPositions.add(position);
                        }
                        for (int j = 0; j < recordStartPositions.size(); j++) {
                            davisBaseTables.seek(recordStartPositions.get(j) + davisBaseUtils.COLUMNS_COUNT_OFFSET);
                            int tableNameLength = davisBaseTables.readByte() - 0x0C;
                            byte[] tableNameBytes = new byte[tableNameLength];
                            davisBaseTables.seek(davisBaseTables.getFilePointer() + columnsCount - 1);
                            for (int k = 0; k < tableNameLength; k++) {
                                tableNameBytes[k] = davisBaseTables.readByte();
                            }
                            String tableNameRetrieved = new String(tableNameBytes);
                            if (tableName.equalsIgnoreCase(tableNameRetrieved)) {
                                recordStartPositions.remove(j);
                                break;
                            }
                        }
                        if(count > recordStartPositions.size()) {
                            davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                            for(Integer recordPosition: recordStartPositions) {
                                davisBaseTables.writeShort(recordPosition);
                                count--;
                            }
                            while(count != 0) {
                                davisBaseTables.writeShort(0);
                                count--;
                            }
                            davisBaseTables.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORD_COUNT_OFFSET);
                            davisBaseTables.writeByte(recordStartPositions.size());
                        }
                    }
                }

                // Remove record from davisbase_columns where table_name = <table-name>
                davisBaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                pageCount = davisBaseColumns.length()/davisBaseUtils.PAGE_SIZE;
                for(int i = 1; i <= pageCount; i++) {
                    recordStartPositions = new ArrayList<>();
                    davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORD_COUNT_OFFSET);
                    int count = davisBaseColumns.readByte();
                    int columnsCount = davisBaseUtils.DAVISBASE_COLUMNS_COLUMNS_COUNT - 1;
                    davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.PAGE_TYPE_OFFSET);
                    if(davisBaseColumns.readByte() == davisBaseUtils.PAGE_TYPE_LEAF) {
                        davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                        while ((position = (int) davisBaseColumns.readShort()) != 0) {
                            recordStartPositions.add(position);
                        }
                        for (int j = 0; j < recordStartPositions.size(); j++) {
                            davisBaseColumns.seek(recordStartPositions.get(j) + davisBaseUtils.COLUMNS_COUNT_OFFSET);
                            int tableNameLength = davisBaseColumns.readByte() - 0x0C;
                            byte[] tableNameBytes = new byte[tableNameLength];
                            davisBaseColumns.seek(davisBaseColumns.getFilePointer() + columnsCount - 1);
                            for (int k = 0; k < tableNameLength; k++) {
                                tableNameBytes[k] = davisBaseColumns.readByte();
                            }
                            String tableNameRetrieved = new String(tableNameBytes);
                            if (tableName.equalsIgnoreCase(tableNameRetrieved)) {
                                recordStartPositions.remove(j);
                                j--;
                            }
                        }
                        if(count > recordStartPositions.size()) {
                            davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORDS_ARRAY_OFFSET);
                            for(Integer recordPosition: recordStartPositions) {
                                davisBaseColumns.writeShort(recordPosition);
                                count--;
                            }
                            while(count != 0) {
                                davisBaseColumns.writeShort(0);
                                count--;
                            }
                            davisBaseColumns.seek(davisBaseUtils.PAGE_SIZE * (i - 1) + davisBaseUtils.RECORD_COUNT_OFFSET);
                            davisBaseColumns.writeByte(recordStartPositions.size());
                        }
                    }
                }
                fileToDelete = new File("data/user_data/" + tableName + ".tbl");
                fileToDelete.delete();
                message = "1 table dropped";
            } catch (Exception e) {
                LOGGER.error("Error deleting table " + tableName, e);
                message = "Error deleting table " + tableName;
                return message;
            }

        } else {
            LOGGER.error("Invalid DROP query");
            message = "Invalid DROP query";
            return message;
        }
        return message;
    }
}

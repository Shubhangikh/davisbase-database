package com.utd.davisbase.table;

import com.utd.davisbase.commands.dml.Select;
import com.utd.davisbase.commands.dml.Update;
import com.utd.davisbase.utils.DavisBaseUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class BPlusTreeHandler {
    private DavisBaseUtils davisBaseUtils = new DavisBaseUtils();
    private Map<String, Integer> parameters;

    public Map<String, Integer> extendTree(String tableName, RandomAccessFile table, int leftPageNo, int recordCount) throws Exception {
        parameters = new HashMap<>();
        // Add a new right leaf page
        table.setLength(davisBaseUtils.PAGE_SIZE * (leftPageNo + 1));
        parameters.put("NEW-LEAF-PAGE-NMBR", (int)table.length()/davisBaseUtils.PAGE_SIZE);
        parameters.put("NEW-PAGE-CELL-COUNT", 0);
        table.seek(davisBaseUtils.PAGE_SIZE * (parameters.get("NEW-LEAF-PAGE-NMBR")-1));
        table.writeByte(0x0D);
        table.writeByte(0x00);
        int rootPage = Select.parseSelectQuery("SELECT ROOT_PAGE FROM DAVISBASE_TABLES WHERE TABLE_NAME = '" + tableName + "'", false);
        if(rootPage == 1) {
            createRootPage(tableName, table, recordCount, leftPageNo);
        } else {
            updateRootPage(tableName, table, rootPage, recordCount, leftPageNo);
        }
        table.seek(davisBaseUtils.PAGE_SIZE * (leftPageNo - 1) + davisBaseUtils.RIGHT_PAGE_OFFSET);
        table.writeInt(parameters.get("NEW-LEAF-PAGE-NMBR"));
        return parameters;
    }

    private void updateRootPage(String tableName, RandomAccessFile table, int rootPage, int recordCount, int leftPageNo) throws Exception {
        int rootPageStart = davisBaseUtils.PAGE_SIZE * (rootPage - 1);
        table.seek(rootPageStart + davisBaseUtils.RECORD_COUNT_OFFSET);
        int cellCount = table.readByte();
        table.seek(rootPageStart + davisBaseUtils.RECORD_COUNT_OFFSET);
        table.writeByte(cellCount + 1);
        int cellContentStartPosition = table.readShort();
        int newStartPosition = cellContentStartPosition - davisBaseUtils.INTERIOR_PAGE_CELL_HEADER_SIZE;
        table.seek(rootPageStart + davisBaseUtils.START_OF_CONTENT_OFFSET);
        table.writeShort(newStartPosition);
        table.writeInt(parameters.get("NEW-LEAF-PAGE-NMBR"));
        table.seek(rootPageStart + davisBaseUtils.RECORDS_ARRAY_OFFSET + (cellCount * 2));
        table.writeShort(newStartPosition);

        table.seek(newStartPosition);
        table.writeInt(leftPageNo);
        table.writeInt(recordCount);
    }

    private void createRootPage(String tableName, RandomAccessFile table, int recordCount, int leftPageNo) throws Exception {
        table.setLength(table.length() + davisBaseUtils.PAGE_SIZE);
        int pageNumber = (int)table.length() / davisBaseUtils.PAGE_SIZE;
        Update.updateRootPage(tableName, pageNumber);
        table.seek(davisBaseUtils.PAGE_SIZE * (pageNumber - 1));
        table.writeByte(0x05);
        table.writeByte(0x01);
        int cellContentStartPosition = (int)table.length() - davisBaseUtils.INTERIOR_PAGE_CELL_HEADER_SIZE;
        table.writeShort(cellContentStartPosition);
        table.writeInt(parameters.get("NEW-LEAF-PAGE-NMBR"));
        table.writeShort(cellContentStartPosition);

        table.seek(cellContentStartPosition);
        table.writeInt(leftPageNo);
        table.writeInt(recordCount);
    }

    public int getLeafPageNumber(String tableName, RandomAccessFile table) throws IOException {
        int pageNumber = 1;
        if(!Select.checkTableExistence(tableName)) {
            return pageNumber;
        }
        int rootPage = Select.parseSelectQuery("SELECT ROOT_PAGE FROM DAVISBASE_TABLES WHERE TABLE_NAME = '" + tableName + "'", false);
        if(rootPage == 1) {
            return pageNumber;
        } else {
            table.seek(davisBaseUtils.PAGE_SIZE * (rootPage - 1));
            table.seek(davisBaseUtils.RIGHT_PAGE_OFFSET);
            int rightPageNumber = table.readInt();
            if(rightPageNumber != -1) {
                pageNumber = rightPageNumber;
            } else {
                table.seek(davisBaseUtils.PAGE_SIZE * (rootPage - 1));
                table.seek(davisBaseUtils.START_OF_CONTENT_OFFSET);
                int contentStartPosition = table.readShort();
                table.seek(contentStartPosition);
                pageNumber = table.readInt();
            }
            return pageNumber;
        }
    }
}

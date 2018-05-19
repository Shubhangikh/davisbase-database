package com.utd.davisbase.commands.ddl;

import com.utd.davisbase.commands.dml.Insert;
import com.utd.davisbase.commands.dml.Select;
import com.utd.davisbase.utils.DavisBaseUtils;
import org.apache.log4j.Logger;

import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Pattern;

public class Create {
    private final static Logger LOGGER = Logger.getLogger(Create.class.getName());

    private static Queue<String> validCreateQueryTokens;
    private static String tableName;
    private static List<String> columnNamesList;
    private static List<String> columnDataTypeList;
    private static List<String> columnConstraintList;
    private static String message;
    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();

    public static String parseCreateTableQuery(String createQuery) {
        // Instantiate the validCreateQueryTokens queue with valid create query keywords
        instantiateValidCreateQueryTokens();

        // parsing the create query and checking if it's valid
        int index = 0;
        columnNamesList = new ArrayList<>();
        columnDataTypeList = new ArrayList<>();
        columnConstraintList = new ArrayList<>();
        message = "Table creation successful";
        // StringBuffer columnsString = new StringBuffer();
        String subQuery = "";
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(createQuery.trim().split(" ")));
        // Remove elements from the LinkedList till the retrieval of the regex element
        while(!(validCreateQueryTokens.size() == 3)) {
            String token = validCreateQueryTokens.poll();
            if(!token.equalsIgnoreCase(commandTokens.get(index))) {
                if(token.equals("\\(([\\w\\s,]+)\\)")) {
                    /*for(int i = index; i < commandTokens.size(); i++) {
                        columnsString.append(commandTokens.get(i));
                    }*/
                    subQuery = createQuery.substring(createQuery.trim().indexOf("("));
                    if(!Pattern.compile(token).matcher(subQuery).matches()) {
                        message = "Invalid Create query. Please try again...";
                        return message;
                    }
                } else if(token.equals("<TABLE-NAME>")) {
                    tableName = commandTokens.get(index);
                    if(Select.checkTableExistence(tableName)) {
                        message = "Table already exists";
                        return message;
                    }
                } else {
                    message = "Invalid Create query. Please try again...";
                    return message;
                }
            }
            index++;
        }
        String[] columnsArr = subQuery.substring(1, subQuery.lastIndexOf(")")).trim().toLowerCase().split(",");
        for(int i = 0; i < columnsArr.length; i++) {
            String[] columns = columnsArr[i].trim().split(" ");
            if(i == 0) {
                if(columns.length != 4 || !columns[0].equalsIgnoreCase(validCreateQueryTokens.poll())
                    || !columns[1].equalsIgnoreCase(validCreateQueryTokens.poll())
                        || !(columns[2] + " " + columns[3]).equalsIgnoreCase(validCreateQueryTokens.poll())) {
                    message = "Invalid primary key parameters. Please try again with (ROW_ID INT PRIMARY KEY)";
                    return message;
                }
                columnNamesList.add("ROW_ID");
                columnDataTypeList.add(columns[1]);
                columnConstraintList.add("NO");
            } else {
                if(columns.length != 2) {
                    if(columns.length != 4) {
                        message = "Invalid column parameters count. Please try again...";
                        return message;
                    }
                }
                if(!davisBaseUtils.getDataTypes().contains(columns[1])) {
                    message = "Invalid column data type. Please try again...";
                    return message;
                }
                columnNamesList.add(columns[0]);
                columnDataTypeList.add(columns[1]);
                if(columns.length == 4 ) {
                    if(!((columns[2] + " " + columns[3]).equalsIgnoreCase("NOT NULL"))) {
                        message = "Invalid column constraint. Please try again...";
                        return message;
                    }
                    columnConstraintList.add("NO");
                } else {
                    columnConstraintList.add("YES");
                }
            }
        }
        createTable();
        return message;
    }

    private static void instantiateValidCreateQueryTokens() {
        validCreateQueryTokens = new LinkedList<>();

        validCreateQueryTokens.add("CREATE");
        validCreateQueryTokens.add("TABLE");
        validCreateQueryTokens.add("<TABLE-NAME>");
        validCreateQueryTokens.add("\\(([\\w\\s,]+)\\)");
        validCreateQueryTokens.add("ROW_ID");
        validCreateQueryTokens.add("INT");
        validCreateQueryTokens.add("PRIMARY KEY");
    }

    private static void createTable() {
        if(!tableName.equalsIgnoreCase("davisbase_tables") &&
                !tableName.equalsIgnoreCase("davisbase_columns")) {
            try {
                RandomAccessFile userTable = new RandomAccessFile("data/user_data/"+ tableName + ".tbl", "rw");
                // Initially the file is one page in length
                userTable.setLength(davisBaseUtils.PAGE_SIZE);
                // Set file pointer to the beginning of the file
                userTable.seek(davisBaseUtils.PAGE_TYPE_OFFSET);
                /* Write 0x0D to the page header to indicate a leaf page. The file
                 * pointer will automatically increment to the next byte. */
                userTable.write(davisBaseUtils.PAGE_TYPE_LEAF);
                /* Write 0x00 (although its value is already 0x00) to indicate there
                 * are no cells on this page */
                userTable.write(0x00);
                userTable.close();
            }
            catch (Exception e) {
                LOGGER.error("Unable to create the " + tableName + " table" , e);
            }
        }

        int insertCount = Insert.insertRecordIntoDavisBaseTables(tableName);
        if(insertCount > 0) {
            LOGGER.debug("Catalog updated. " + insertCount + " row(s) inserted into DAVISBASE_TABLES table");
        }
        insertCount = Insert.insertRecordIntoDavisBaseColumns(tableName, columnNamesList, columnDataTypeList, columnConstraintList);
        if(insertCount > 0) {
            LOGGER.debug("Catalog updated. " + insertCount + " row(s) inserted into DAVISBASE_COLUMNS table");
        }
    }
}

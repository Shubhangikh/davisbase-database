package com.utd.davisbase.mode;

import com.utd.davisbase.commands.ddl.Create;
import com.utd.davisbase.commands.ddl.Drop;
import com.utd.davisbase.commands.ddl.Show;
import com.utd.davisbase.commands.dml.Insert;
import com.utd.davisbase.commands.dml.Select;
import com.utd.davisbase.commands.dml.Update;
import com.utd.davisbase.utils.DavisBaseUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import static java.lang.System.out;

public class DavisBasePrompt {
    private final static Logger LOGGER = Logger.getLogger(DavisBasePrompt.class.getName());

    private static String promptStr = "davisql> ";
    private static boolean isExit = false;
    private static DavisBaseUtils davisBaseUtils = new DavisBaseUtils();
    private static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {
        initializeDataStore();
        splashScreen();
        String userCommand = "";

        while (!isExit) {
            out.print(promptStr);
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            parseQuery(userCommand);
        }
        print("Exiting...");
    }

    private static void initializeDataStore() {
        try {
            File dataDir = new File("data/catalog");
            if(!dataDir.exists()) {
                boolean isCreated = dataDir.mkdirs();
                if(isCreated) {
                    LOGGER.debug("Database catalog directory created successfully.");
                }
            }
            /*String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            if(oldTableFiles != null) {
                for (int i=0; i < oldTableFiles.length; i++) {
                    File anOldFile = new File(dataDir, oldTableFiles[i]);
                    anOldFile.delete();
                }
            }*/
            File userDataDir = new File("data/user_data");
            if(!userDataDir.exists()) {
                boolean isCreated = userDataDir.mkdirs();
                if(isCreated) {
                    LOGGER.debug("Database user directory created successfully.");
                }
            }
        }
        catch (SecurityException se) {
            LOGGER.error("Unable to create data container directory", se);
        }

        /* Create davisbase_tables system catalog */
        try {
            File file = new File("data/catalog/davisbase_tables.tbl");
            if(!file.exists()) {
                RandomAccessFile davisBaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

                /* Initially, the file is one page in length */
                davisBaseTablesCatalog.setLength(davisBaseUtils.PAGE_SIZE);
                /* Set file pointer to the beginning of the file */
                davisBaseTablesCatalog.seek(davisBaseUtils.PAGE_TYPE_OFFSET);
                /* Write 0x0D to the page header to indicate that it's a leaf page.
                 * The file pointer will automatically increment to the next byte. */
                davisBaseTablesCatalog.write(davisBaseUtils.PAGE_TYPE_LEAF);
                /* Write 0x00 (although its value is already 0x00) to indicate there
                 * are no cells on this page */
                davisBaseTablesCatalog.write(0x00);
                davisBaseTablesCatalog.close();
            }
        }
        catch (Exception e) {
            LOGGER.error("Unable to create the database_tables file ", e);
        }

        /* Create davisbase_columns systems catalog */
        try {
            File file = new File("data/catalog/davisbase_columns.tbl");
            if(!file.exists()) {
                RandomAccessFile davisBaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
                /* Initially the file is one page in length */
                davisBaseColumnsCatalog.setLength(davisBaseUtils.PAGE_SIZE);
                // Set file pointer to the beginning of the file
                davisBaseColumnsCatalog.seek(davisBaseUtils.PAGE_TYPE_OFFSET);
                /* Write 0x0D to the page header to indicate a leaf page. The file
                 * pointer will automatically increment to the next byte. */
                davisBaseColumnsCatalog.write(davisBaseUtils.PAGE_TYPE_LEAF);
                /* Write 0x00 (although its value is already 0x00) to indicate there
                 * are no cells on this page */
                davisBaseColumnsCatalog.write(0x00);
                davisBaseColumnsCatalog.close();
            }
        }
        catch (Exception e) {
            LOGGER.error("Unable to create the database_columns file ", e);
        }
    }

    private static void splashScreen() {
        print(line("-",80));
        print("Welcome to DavisBaseLite"); // Display the string.
        print("\nType \"help;\" to display supported commands.");
        print(line("-",80));
    }

    private static String line(String s, int num) {
        StringBuilder a = new StringBuilder();
        for(int i = 0; i < num; i++) {
            a.append(s);
        }
        return a.toString();
    }

    private static void parseQuery(String userCommand) {
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "create" :
                String createMessage = Create.parseCreateTableQuery(userCommand);
                LOGGER.debug(createMessage);
                print("");
                print(createMessage);
                print("-------------------");
                break;
            case "drop":
                String dropMessage = Drop.parseDropTableQuery(userCommand);
                LOGGER.debug(dropMessage);
                print(dropMessage);
                break;
            case "show":
                String showMessage = Show.parseShowTableQuery(userCommand);
                LOGGER.debug(showMessage);
                print(showMessage);
                print("\n");
                break;
            case "insert":
                String insertMessage = Insert.parseInsertQuery(userCommand);
                LOGGER.debug(insertMessage);
                print(insertMessage);
                break;
            case "select":
                int recordsSelected = Select.parseSelectQuery(userCommand, true);
                if(recordsSelected > 0) {
                    LOGGER.debug(recordsSelected + " row(s) in set");
                    print("");
                    print(recordsSelected + " row(s) in set");
                } else if(recordsSelected == 0) {
                    LOGGER.debug("Empty set");
                    print("");
                    print("Empty set");
                } else {
                    print("");
                    print("Invalid SELECT query");
                }
                break;
            case "update":
                String updateMessage = Update.parseUpdateQuery(userCommand);
                LOGGER.debug(updateMessage);
                print(updateMessage);
                break;
            case "help":
                help();
                break;
            case "exit":
                isExit = true;
                break;
            case "quit":
                isExit = true;
                break;
            default:
                print("I didn't understand the command: \"" + userCommand + "\". Please Try Again...");
                break;
        }
    }

    private static void help() {
        print(line("*",80));
        print("SUPPORTED COMMANDS\n");
        print("All commands below are case insensitive\n");
        print("SHOW TABLES;");
        print("\tDisplay the names of all tables.\n");
        print("CREATE TABLE <table_name> (row_id INT PRIMARY KEY, column_name2 data_type2 [NOT NULL]...)");
        print("\t Create <table_name> table.");
        print("DROP TABLE <table_name>;");
        print("\tRemove table data (i.e. all records) and its schema.\n");
        print("INSERT INTO <table_name> [<column_list>] VALUES <value_list>;");
        print("\tInsert a data record in the <table_name> table.\n");
        print("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        print("\tModify record's data whose optional <condition> is given by WHERE clause.\n");
        print("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        print("\tDisplay table records whose optional <condition> is <column_name> = <value>.\n");
        print("HELP;");
        print("\tDisplay this help information.\n");
        print("EXIT;");
        print("\tExit the program.\n");
        print(line("*",80));
    }

    public static void print(String line) {
        out.println(line);
    }
}

package com.utd.davisbase.commands.ddl;

import com.utd.davisbase.commands.dml.Select;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

public class Show {
    private final static Logger LOGGER = Logger.getLogger(Show.class.getName());

    public static String parseShowTableQuery(String showQuery) {
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(showQuery.trim().split(" ")));
        if(commandTokens.size() != 2) {
            LOGGER.error("Invalid SHOW query. Please try again...");
            return "Invalid SHOW query. Please try again...";
        } else {
            if(!commandTokens.get(0).equalsIgnoreCase("SHOW") && !commandTokens.get(1).equalsIgnoreCase("TABLES")) {
                LOGGER.error("Invalid SHOW query. Please try again...");
                return "Invalid SHOW query. Please try again...";
            }
        }
        int count = Select.parseSelectQuery("SELECT TABLE_NAME FROM DAVISBASE_TABLES", true);
        return count + " row(s) in set";
    }


}

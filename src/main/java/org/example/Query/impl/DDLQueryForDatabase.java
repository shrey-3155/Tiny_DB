package org.example.Query.impl;

import org.example.Query.DDLQueryDatabaseInterface;
import org.example.Utils.impl.Logger;

import java.io.File;
import java.io.IOException;

public class DDLQueryForDatabase implements DDLQueryDatabaseInterface {
    public String DB_PATH_NAME;

    @Override
    public void createDatabase(String databaseName) throws IOException {
        // Initialize the logger
        Logger.initializeLogger(databaseName);

        long startTime = System.currentTimeMillis();

        // Log the database creation query
        Logger.logQuery("CREATE DATABASE " + databaseName);

        File databaseFile = new File("src/main/java/org/example/Database" + File.separator + databaseName);
        if (!databaseFile.exists()) {
            if (databaseFile.mkdirs()) {
                System.out.println("Database created");
                Logger.logEvent("Database created: " + databaseName);
            } else {
                String errorMessage = "Error creating database directory.";
                System.out.println(errorMessage);
                Logger.logEvent(errorMessage);
            }
        } else {
            String message = "Database already exists";
            System.out.println(message);
            Logger.logEvent(message);
        }

        long endTime = System.currentTimeMillis();
        Logger.logGeneral(String.valueOf(endTime - startTime), 0, 0); // Log general info with 0 tables and records
    }

    @Override
    public void useDatabase(String databaseName) throws IOException {
        // Initialize the logger
        Logger.initializeLogger(databaseName);

        long startTime = System.currentTimeMillis();

        // Log the use database query
        Logger.logQuery("USE DATABASE " + databaseName);

        DB_PATH_NAME = "src/main/java/org/example/Database/" + databaseName;
        File databaseFile = new File(DB_PATH_NAME);

        if (!databaseFile.exists()) {
            String errorMessage = "Database does not exist: " + DB_PATH_NAME;
            System.out.println(errorMessage);
            Logger.logEvent(errorMessage);
        } else {
            // If the database exists, set the path and log the event
            String message = "Database set to: " + databaseName;
            DB_PATH_NAME = databaseFile.getAbsolutePath(); // Update the DB_PATH_NAME with the absolute path
            System.out.println(message);
            Logger.logEvent(message);
        }

        long endTime = System.currentTimeMillis();
        Logger.logGeneral(String.valueOf(endTime - startTime), 0, 0); // Log general info with 0 tables and records
    }

    @Override
    public void dropTable(String path, String tableName) throws IOException {
        // Initialize the logger
        String databaseName = new File(path).getName(); // Extract database name from path
        Logger.initializeLogger(databaseName);

        long startTime = System.currentTimeMillis();

        // Log the drop table query
        Logger.logQuery("DROP TABLE " + tableName);

        File tableFile = new File(path + File.separator + tableName + ".txt");

        if (!tableFile.exists()) {
            String errorMessage = "Table file does not exist: " + tableFile.getPath();
            System.out.println(errorMessage);
            Logger.logEvent(errorMessage);
        } else if (tableFile.isDirectory()) {
            String errorMessage = "The specified path is a directory, not a table file: " + tableFile.getPath();
            System.out.println(errorMessage);
            Logger.logEvent(errorMessage);
        } else {
            if (tableFile.delete()) {
                String message = "Table dropped successfully: " + tableName;
                System.out.println(message);
                Logger.logEvent(message);
            } else {
                String errorMessage = "Failed to delete the table file: " + tableFile.getPath();
                System.out.println(errorMessage);
                Logger.logEvent(errorMessage);
            }
        }

        long endTime = System.currentTimeMillis();
        Logger.logGeneral(String.valueOf(endTime - startTime), 0, 0); // Log general info with 0 tables and records
    }
}

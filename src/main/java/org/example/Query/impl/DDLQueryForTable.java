package org.example.Query.impl;

import org.example.Query.DDLQueryTableInterface;
import org.example.Utils.impl.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class DDLQueryForTable implements DDLQueryTableInterface {

    @Override
    public void createTable(String path, String tableName, Map<String, String> columnDetails) throws IOException {
        // Initialize the logger for the specified database
        String databaseName = new File(path).getName(); // Extract database name from path
        Logger.initializeLogger(databaseName);

        long startTime = System.currentTimeMillis();

        // Log the create table query
        Logger.logQuery("CREATE TABLE " + tableName + " " + columnDetails.toString());

        // Check if a primary key is defined
        boolean primaryKeyDefined = false;
        for (String columnDefinition : columnDetails.values()) {
            if (columnDefinition.contains("PRIMARY KEY")) {
                primaryKeyDefined = true;
                break;
            }
        }

        if (!primaryKeyDefined) {
            String errorMessage = "Error: No PRIMARY KEY defined. Table creation aborted.";
            System.out.println(errorMessage);
            Logger.logEvent(errorMessage);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0); // Log general info with 0 table and record count
            return;
        }

        File tableFile = new File(path + File.separator + tableName + ".txt");
        File metadataFile = new File(path + File.separator + tableName + "_metadata.txt");

        if (!tableFile.exists()) {
            try {
                tableFile.createNewFile();
                StringBuilder columnsLine = new StringBuilder();
                for (String columnName : columnDetails.keySet()) {
                    if (columnsLine.length() > 0) {
                        columnsLine.append(" | ");
                    }
                    columnsLine.append(columnName);
                }
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile))) {
                    bw.write(columnsLine.toString());
                    bw.newLine();
                    System.out.println("Table created with name: " + tableName);
                    Logger.logEvent("Table created with name: " + tableName);
                }

                // Create metadata file
                try (BufferedWriter metaBw = new BufferedWriter(new FileWriter(metadataFile))) {
                    for (Map.Entry<String, String> entry : columnDetails.entrySet()) {
                        String columnName = entry.getKey();
                        String columnDefinition = entry.getValue();

                        // Extract type and relationship
                        String[] parts = columnDefinition.split("\\|");
                        String columnType = parts[0].trim(); // First part is the data type
                        String relationship = parts.length > 1 ? parts[1].trim() : "None"; // Default to no relationship
                        String userDefinedRelationship = "None";
                        if (parts.length > 2) {
                            userDefinedRelationship = parts[2].trim();
                        }

                        // Write metadata in the format: columnName | dataType | relationship
                        metaBw.write(columnName + " | " + columnType + " | " + relationship + " | " + userDefinedRelationship);
                        metaBw.newLine();
                    }
                    System.out.println("Metadata file created for table: " + tableName);
                    Logger.logEvent("Metadata file created for table: " + tableName);
                } catch (IOException e) {
                    String errorMessage = "Error creating metadata file: " + e.getMessage();
                    System.out.println(errorMessage);
                    Logger.logEvent(errorMessage);
                    Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0); // Log general info with 0 table and record count
                    e.printStackTrace();
                }
            } catch (IOException e) {
                String errorMessage = "Error creating table file: " + tableFile;
                System.out.println(errorMessage);
                Logger.logEvent(errorMessage);
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0); // Log general info with 0 table and record count
                e.printStackTrace();
            }
        } else {
            String message = "Table already exists!";
            System.out.println(message);
            Logger.logEvent(message);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0); // Log general info with 0 table and record count
        }

        long endTime = System.currentTimeMillis();
        Logger.logGeneral(String.valueOf(endTime - startTime), 1, 0); // Assuming one table was created, and initially no records
    }
}

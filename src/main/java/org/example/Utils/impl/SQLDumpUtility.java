package org.example.Utils.impl;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLDumpUtility {
    public void export(String databasepath){
        File dbFolder = new File(databasepath);
        String database = dbFolder.getName();
        String databaseFolderPath = databasepath;
        String dumpFilePath = "src/main/java/org/example/SQLDumps" + File.separator + database +"_dump.sql";

        //Check if dump file exist
        File f1 = new File(dumpFilePath);
        if(f1.exists()){
            f1.delete();
            System.out.println("Old dump deleted!!");
        }

        try (FileWriter writer = new FileWriter(dumpFilePath)) {
            // Write create database and use database commands
            writer.write("CREATE DATABASE "+database+";\n");
            writer.write("USE "+database+";\n\n");

            //Finding only table files ignoring _metadata.txt
            List<File> dataFiles = findDataFiles(databaseFolderPath);
            for (File dataFile : dataFiles) {
                String tableName = extractTableName(dataFile.getName());
                String metadataFilePath = databaseFolderPath + File.separator + tableName + "_metadata.txt";

                // Read metadata
                String metadata = readMetadata(metadataFilePath);

                // Generate SQL dump for the table
                String sqlDump = generateTableSQLDump(tableName, dataFile.getAbsolutePath(), metadata);

                // Write SQL dump to file
                writer.write(sqlDump + "\n\n");
            }

            System.out.println("SQL dump file created successfully: " + dumpFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<File> findDataFiles(String databaseFolderPath) {
        List<File> dataFiles = new ArrayList<>();
        File folder = new File(databaseFolderPath);
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".txt") && !file.getName().contains("_metadata")) {
                    dataFiles.add(file);
                }
            }
        }
        return dataFiles;
    }

    private static String extractTableName(String fileName) {
        return fileName.substring(0, fileName.lastIndexOf('.'));
    }

    private static String readMetadata(String metadataFilePath) throws IOException {
        StringBuilder metadataBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(metadataFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                metadataBuilder.append(line.trim()).append("\n");
            }
        }
        return metadataBuilder.toString();
    }

    private static String generateTableSQLDump(String tableName, String dataFilePath, String metadata) throws IOException {
        StringBuilder sqlDumpBuilder = new StringBuilder();
        sqlDumpBuilder.append(String.format("CREATE TABLE %s (\n", tableName));

        // Append metadata lines as table columns
        String[] metadataLines = metadata.split("\\n");
        for (String metadataLine : metadataLines) {
            String[] parts = metadataLine.trim().split("\\|");
            String columnName = parts[0].trim();
            String dataType = parts[1].trim();
            String constraints = parts.length > 2 ? parts[2].trim() : "";

            if(constraints.equals("None")){
                sqlDumpBuilder.append(String.format("    %s %s,\n", columnName, dataType));
            }
            else{
                sqlDumpBuilder.append(String.format("    %s %s %s,\n", columnName, dataType, constraints));
            }
        }

        // Remove the trailing comma from the last column definition
        sqlDumpBuilder.deleteCharAt(sqlDumpBuilder.length() - 2); // -2 to remove the comma and newline

        sqlDumpBuilder.append("\n);\n\n");

        // Read data and generate INSERT statements
        try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {
            String line;
            reader.readLine();  //Skip headers
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue; // Skip empty lines
                }
                String[] values = line.split("\\s*\\|\\s*");
                String[] finalValues = addQuotesIfNotNumeric(values);
                sqlDumpBuilder.append(String.format("INSERT INTO %s VALUES (%s);\n", tableName, String.join(", ", finalValues)));
            }
        }

        return sqlDumpBuilder.toString();
    }

    private static String[] addQuotesIfNotNumeric(String[] arr) {
        List<String> result = new ArrayList<>();

        for (String item : arr) {
            if (!item.matches("\\d+")) {  // Check if the item is not a numeric string
                item = "'" + item + "'";  // Add single quotes around non-numeric strings
            }
            result.add(item);
        }

        // Convert List<String> to String[]
        return result.toArray(new String[0]);
    }
}

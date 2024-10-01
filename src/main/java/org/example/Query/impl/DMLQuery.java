package org.example.Query.impl;

import org.example.Query.DMLQueryInterface;
import org.example.Utils.impl.Logger;

import java.io.*;
import java.util.*;

public class DMLQuery implements DMLQueryInterface {
    @Override
    public void insertIntoRows(String path, String tableName, Map<String, String> rowValues) throws IOException {
        long startTime = System.currentTimeMillis();
        File tableFile = new File(path + File.separator + tableName + ".txt");
        File metadataFile = new File(path + File.separator + tableName + "_metadata.txt");

        // Check if the table file exists
        if (!tableFile.exists()) {
            Logger.logEvent("Table file does not exist: " + tableFile);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0);
            return;
        }

        // Read the header line to get the column names
        List<String> columns = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String headerLine = reader.readLine(); // Read the first line (header)
            if (headerLine == null) {
                Logger.logEvent("Table file is empty: " + tableFile);
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0);                return;
            }
            columns = Arrays.asList(headerLine.split(" \\| ")); // Assuming columns are separated by " | "
        } catch (IOException e) {
            Logger.logEvent("Error reading from table file: " + e.getMessage());
            return;
        }

        // Identify the primary key column from the metadata file
        String primaryKeyColumn = null;
        try (BufferedReader metaReader = new BufferedReader(new FileReader(metadataFile))) {
            String line;
            while ((line = metaReader.readLine()) != null) {
                if (line.contains("PRIMARY KEY")) {
                    String[] parts = line.split("\\|");
                    if (parts.length > 0) {
                        primaryKeyColumn = parts[0].trim();
                        break;
                    }
                }
            }
        } catch (IOException e) {
            Logger.logEvent("Error reading from metadata file: " + e.getMessage());
            return;
        }

        // Check if the primary key column was found
        if (primaryKeyColumn == null) {
            Logger.logEvent("No primary key defined for the table '" + tableName + "'.");
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        // Validate that the primary key is present in rowValues
        if (!rowValues.containsKey(primaryKeyColumn)) {
            Logger.logEvent("Primary key '" + primaryKeyColumn + "' must be provided.");
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        // Validate that all keys in rowValues exist in the table's columns
        for (String key : rowValues.keySet()) {
            if (!columns.contains(key)) {
                Logger.logEvent("Column '" + key + "' does not exist in the table '" + tableName + "'.");
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime),  getTableCount(path), getRecordCount(path, tableName));
                return;
            }
        }

        // Prepare the row data in the format key1 | key2 | key3
        StringBuilder rowLine = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            if (i > 0) {
                rowLine.append(" | ");
            }
            // Insert 'null' if the value for the column is not provided
            rowLine.append(rowValues.getOrDefault(columnName, "null"));
        }

        // Write the row data to the table file, starting from the second line
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) { // 'true' for append mode
            writer.write(rowLine.toString());
            writer.newLine();
            Logger.logEvent("Row added to table '" + tableName + "': " + rowLine);
            Logger.logQuery("Insert query executed for table: " + tableName);
        } catch (IOException e) {
            e.printStackTrace();
            Logger.logEvent("Error writing to table file: " + tableFile);
        }}

    @Override
    public void selectFromTable(String path, String tableName, String[] columns) throws IOException {
        long startTime = System.currentTimeMillis();
        int rowsReturned = 0;
        Map<String, Integer> columnToIndex = new HashMap<String, Integer>();
        File tableFile = new File(path + File.separator + tableName + ".txt");
        // Check if the table file exists
        if (!tableFile.exists()) {
            Logger.logEvent("Table file does not exist: " + tableFile);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }
        BufferedReader br = new BufferedReader(new FileReader(tableFile));
        if(columns[0].equals("*")){
            String firstline;
            firstline = br.readLine();
            System.out.println(firstline);

            String line;
            while ((line = br.readLine()) != null) {
                rowsReturned++;
                System.out.println(line);
            }
        }
        else{
                String firstline;
                firstline = br.readLine();
                String[] headers = firstline.split("\\|");
                if(!checkValidCol(headers, columns)){
                    return;
                }
                //populate columntoIndex map with column name and index
                for(int i = 0; i < headers.length; i++){
                    columnToIndex.put(headers[i].trim(), i);
                }
                //Print header line
                for(int i = 0; i < columns.length; i++){
                    System.out.print(columns[i] + " | ");
                }
                System.out.println();
                //Reading rows after table header line
                String dataline;
                while((dataline = br.readLine()) != null){
                    //dataline = br.readLine();
                    rowsReturned++;
                    String[] data = dataline.split("\\|");
                    for(int i = 0; i < columns.length; i++){
                        if(columnToIndex.containsKey(columns[i])){
                            System.out.print(data[columnToIndex.get(columns[i])]+ " | ");
                        }
                    }
                    System.out.println();
                }
        }
        Logger.logQuery("Select query executed for table: " + tableName);
        Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), rowsReturned);
        System.out.println(rowsReturned+ " rows returned");
    }

    public List<Map<String, String>> selectFromTableCondition(String path, String tableName, String[] columns, String condition) throws IOException {
        long startTime = System.currentTimeMillis();
        Map<String, Integer> columnToIndex = new HashMap<>();
        File tableFile = new File(path + File.separator + tableName + ".txt");
        int rowsReturned = 0;
        // Check for valid condition
        if (!condition.contains("=") && !condition.contains("<") && !condition.contains(">") &&
                !condition.contains("!=") && !condition.contains("<=") && !condition.contains(">=")) {
            Logger.logEvent("Illegal expression: Condition must contain '=', '!=', '<', '<=', '>', or '>='");
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path,tableName));
            return null;
        }

        // Split condition based on supported operators
        String[] conditions = null;
        if (condition.contains("!=")) {
            conditions = condition.split("!=");
        } else if (condition.contains("<=")) {
            conditions = condition.split("<=");
        } else if (condition.contains(">=")) {
            conditions = condition.split(">=");
        } else if (condition.contains("=")) {
            conditions = condition.split("=");
        } else if (condition.contains("<")) {
            conditions = condition.split("<");
        } else if (condition.contains(">")) {
            conditions = condition.split(">");
        } else {
            Logger.logEvent("Unsupported condition operator");
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return null;
        }

        // Check if the table file exists
        if (!tableFile.exists()) {
            Logger.logEvent("Table file does not exist: " + tableFile);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return null;
        }

        BufferedReader br = new BufferedReader(new FileReader(tableFile));
        String firstline = br.readLine();
        String[] headers = firstline.split("\\|");

        if(!columns[0].equals("*")){
            if(!checkValidCol(headers, columns)){
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                return null;
            }
        }

        // Populate columnToIndex map with column name and index
        for (int i = 0; i < headers.length; i++) {
            columnToIndex.put(headers[i].trim(), i);
        }

        // Find index of condition column
        int conditionIndex = -1;
        //conditionIndex = ;
        if(columnToIndex.get(conditions[0].trim()) == null){
            Logger.logEvent("Unknown column " + conditions[0]);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return null;
        }
        else{
            conditionIndex = columnToIndex.get(conditions[0].trim());
        }

        // Print header line
        if (columns[0].equals("*")) {
            for (int i = 0; i < headers.length; i++) {
                System.out.print(headers[i] + "|");
            }
            System.out.println();
        } else {
            for (int i = 0; i < columns.length; i++) {
                    System.out.print(columns[i] + "|");
            }
            System.out.println();
        }

        // Reading rows after table header line
        String dataline;
        while ((dataline = br.readLine()) != null) {
            String[] data = dataline.split("\\|");

            boolean conditionMet = false;
            if (condition.contains("!=")) {
                conditionMet = !data[conditionIndex].trim().equals(conditions[1].trim().replaceAll("^'|'$", ""));

            } else if (condition.contains("<=")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile <= valueInCondition;

                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return null;
                }
            } else if (condition.contains(">=")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile >= valueInCondition;
                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return null;
                }
            } else if(condition.contains("=")) {
                    conditionMet = data[conditionIndex].trim().equals(conditions[1].trim().replaceAll("^'|'$", ""));
            } else if(condition.contains("<")) {
                try{
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile < valueInCondition;
                }
                catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return null;
                }
            } else if (condition.contains(">")) {
                try{
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile > valueInCondition;
                }
                catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return null;
                }
            }

            // Print matching rows
            if (conditionMet) {
                rowsReturned++;
                if (columns[0].equals("*")) {
                    System.out.println(dataline);
                } else {
                    for (int i = 0; i < columns.length; i++) {
                        if (columnToIndex.containsKey(columns[i])) {
                            System.out.print(data[columnToIndex.get(columns[i])] + "|");
                        }
                    }
                    System.out.println();
                }
            }
        }
        Logger.logQuery("Select query with condition executed for table: " + tableName);
        Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, rowsReturned);
        System.out.println(rowsReturned + " rows returned");
        return null;
    }

    @Override
    public void updateToTable(String path, String tableName, String colValuePair, String condition) throws IOException {

        String[] colValue = colValuePair.split("\\="); //set col1='xyz'
        File tableFile = new File(path + File.separator + tableName + ".txt");
        long startTime = System.currentTimeMillis();
        int rowsReturned = 0;
        StringBuilder sb = new StringBuilder();

        if((!colValuePair.contains("="))){
            Logger.logEvent("Illegal expression in colValuePair: " + colValuePair);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        // Check for valid condition
        if (!condition.contains("=") && !condition.contains("<") && !condition.contains(">") &&
                !condition.contains("!=") && !condition.contains("<=") && !condition.contains(">=")) {
            Logger.logEvent("Illegal expression in condition: " + condition);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        // Split condition based on supported operators
        String[] conditions = null;
        if (condition.contains("!=")) {
            conditions = condition.split("!=");
        } else if (condition.contains("<=")) {
            conditions = condition.split("<=");
        } else if (condition.contains(">=")) {
            conditions = condition.split(">=");
        } else if (condition.contains("=")) {
            conditions = condition.split("=");
        } else if (condition.contains("<")) {
            conditions = condition.split("<");
        } else if (condition.contains(">")) {
            conditions = condition.split(">");
        } else {
            Logger.logEvent("Unsupported condition operator in condition: " + condition);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        //check table file exist
        if (!tableFile.exists()) {
            Logger.logEvent("Table file does not exist: " + tableFile);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        BufferedReader br = new BufferedReader(new FileReader(tableFile));
        String firstLine = br.readLine();
        sb.append(firstLine).append(System.lineSeparator());
        String[] headers = firstLine.split("\\|");
        String dataline;
        int conditionIndex = -1;
        int colToModifyIndex = -1;

        //Locate conditionIndex and colToModify map with column name and index
        String cleanedValue;
        for(int i = 0; i < headers.length; i++){
            if(headers[i].trim().equals(conditions[0].trim())){
                conditionIndex = i;
            }
            cleanedValue = colValue[0].trim().replaceAll("^'|'$", "");;
            if(headers[i].trim().equals(cleanedValue)){
                colToModifyIndex = i;
            }
        }

        if(conditionIndex < 0){
            Logger.logEvent("Unknown column in condition: " + conditions[0]);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0);
            return;
        }

        if(colToModifyIndex < 0){
            Logger.logEvent("Unknown column in colValuePair: " + colValuePair.split("=")[0]);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        cleanedValue = conditions[1].trim().replaceAll("^'|'$", "");
        //Searching rest of the table
        while((dataline = br.readLine())!=null){
            String[] data = dataline.split("\\|");

            boolean conditionMet = false;
            if (condition.contains("!=")) {
                if (condition.contains("!=")) {
                    conditionMet = !data[conditionIndex].trim().equals(cleanedValue);
                } else {
                    conditionMet = data[conditionIndex].trim().equals(cleanedValue);
                }
            } else if (condition.contains("<=")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(cleanedValue);
                    if (condition.contains("<=")) {
                        conditionMet = valueInFile <= valueInCondition;
                    } else {
                        conditionMet = valueInFile < valueInCondition;
                    }
                } catch (NumberFormatException e) {
                    Logger.logEvent("Comparison failed: Non-integer value found in column");
                    Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                    return;
                }
            } else if (condition.contains("=>")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(cleanedValue);
                    if (condition.contains(">=")) {
                        conditionMet = valueInFile >= valueInCondition;
                    } else {
                        conditionMet = valueInFile > valueInCondition;
                    }
                } catch (NumberFormatException e) {
                    Logger.logEvent("Comparison failed: Non-integer value found in column");
                    Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                    return;
                }
            } else if(condition.contains("=")) {
                    conditionMet = data[conditionIndex].trim().equals(cleanedValue);
            } else if(condition.contains("<")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(cleanedValue);
                    conditionMet = valueInFile < valueInCondition;
                } catch (NumberFormatException e) {
                    Logger.logEvent("Comparison failed: Non-integer value found in column");
                    Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                    return;
                }
            } else if (condition.contains(">")) {
                try {
                    int valueInFile = Integer.parseInt(data[conditionIndex].trim());
                    int valueInCondition = Integer.parseInt(cleanedValue);
                    conditionMet = valueInFile > valueInCondition;
                } catch (NumberFormatException e) {
                    Logger.logEvent("Comparison failed: Non-integer value found in column");
                    Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                    return;
                }
            }

            // Print matching rows
            if (conditionMet) {
                rowsReturned++;
                data[colToModifyIndex] = colValue[1].trim().replaceAll("^'|'$", "");
                int columnCount = 0;
                for (String value : data) {
                    if (columnCount > 0) {
                        sb.append(" | ");
                    }
                    sb.append(value.trim().replaceAll("^'|'$", ""));
                    columnCount++;
                }
                sb.append(System.lineSeparator());
            }
            else{
                sb.append(dataline).append(System.lineSeparator());
            }
        }

        // Write the updated content back to the file
        System.out.println(rowsReturned+ " rows modified");
        Logger.logQuery("Update query executed for table: " + tableName);
        Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), rowsReturned, getRecordCount(path, tableName));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile));
        writer.write(sb.toString());
        writer.close();
    }

    private boolean checkValidCol(String[] headers, String[] requiredColumns){
        //Cleaning header
        for(int i=0; i< headers.length; i++){
            headers[i] = headers[i].trim();
        }
        //Cleaning required columns
        for(int j=0; j<requiredColumns.length; j++){
            requiredColumns[j] = requiredColumns[j].trim();
        }

        for (int k = 0; k < requiredColumns.length; k++) {
            if(!(Arrays.stream(headers).anyMatch(requiredColumns[k]::equals))){
                System.out.println("Unknown column "+requiredColumns[k]);
                return false;
            }
        }

        return true;
    }
    @Override
    public void deleteFromTable(String path, String tableName, String whereCondition) throws IOException {
        long startTime = System.currentTimeMillis();
        File tableFile = new File(path + File.separator + tableName + ".txt");
        if (!tableFile.exists()) {
            Logger.logEvent("Table file does not exist: " + tableFile);
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0);
            return;
        }

        StringBuilder fileContent = new StringBuilder();
        int rowsDeleted;
        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            String firstLine = reader.readLine();
            if (firstLine == null) {
                System.out.println("Table file is empty.");
                Logger.logEvent("Table file is empty: " + tableFile);
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), 0, 0);
                return;
            }

            fileContent.append(firstLine).append(System.lineSeparator());
            rowsDeleted = 0;

            String[] conditions = whereCondition.split("=");
            if (conditions.length != 2) {
                System.out.println("Invalid WHERE condition.");
                Logger.logEvent("Invalid WHERE condition: " + whereCondition);
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));

                return;
            }

            String columnName = conditions[0].trim();
            String valueToMatch = conditions[1].trim().replaceAll("^'|'$", "");
            Map<String, Integer> columnToIndex = new HashMap<>();

            String[] headers = firstLine.split("\\|");
            for (int i = 0; i < headers.length; i++) {
                columnToIndex.put(headers[i].trim(), i);
            }

            if (!columnToIndex.containsKey(columnName)) {
                System.out.println("Column not found in table.");
                Logger.logEvent("Column not found in table: " + columnName);
                Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
                return;
            }

            int conditionIndex = columnToIndex.get(columnName);
            String currentLine;

            boolean rowExists = false;  // Flag to check if any row matches the condition

            while ((currentLine = reader.readLine()) != null) {
                String[] data = currentLine.split("\\|");
                if (data.length <= conditionIndex || !data[conditionIndex].trim().equals(valueToMatch)) {
                    fileContent.append(currentLine.trim()).append(System.lineSeparator());
                } else {
                    rowExists = true;  // Set flag to true if a matching row is found
                    System.out.println("Deleted row: " + currentLine.trim());
                }
            }
            // Check the flag after the loop
            if (!rowExists) {
                Logger.logEvent("Row either deleted or does not exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("An error occurred while processing the table file.");
            Logger.logEvent("An error occurred while processing the table file: " + e.getMessage());
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
            writer.write(fileContent.toString());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("An error occurred while writing back to the table file.");
            Logger.logEvent("An error occurred while writing back to the table file: " + e.getMessage());
            Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), getTableCount(path), getRecordCount(path, tableName));
        }
        Logger.logEvent(rowsDeleted + " rows deleted from table: " + tableName);
        Logger.logQuery("Delete query executed for table: " + tableName);
        Logger.logGeneral(String.valueOf(System.currentTimeMillis() - startTime), rowsDeleted, 0);
        System.out.println(rowsDeleted + " rows deleted");

    }

    private int getTableCount(String path) {
        File directory = new File(path);
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".txt"));
        if (files != null) {
            return files.length;
        }
        return 0;
    }

    private int getRecordCount(String path, String tableName) throws IOException {
        File tableFile = new File(path + File.separator + tableName + ".txt");
        if (!tableFile.exists()) {
            return 0;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
            int count = 0;
            while (reader.readLine() != null) {
                count++;
            }
            return count - 1; // Subtract 1 to exclude the header row
        }
    }

    public Map<String, Integer> getTableSchema(String path, String tableName) throws IOException {
        Map<String, Integer> columnSchema = new HashMap<>();
        File tableFile = new File(path + File.separator + tableName + ".txt");
        if (tableFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                String headerLine = reader.readLine();
                if (headerLine != null) {
                    String[] headers = headerLine.split("\\|");
                    for (int i = 0; i < headers.length; i++) {
                        columnSchema.put(headers[i].trim(), i);
                    }
                }
            }
        }
        return columnSchema;
    }

}

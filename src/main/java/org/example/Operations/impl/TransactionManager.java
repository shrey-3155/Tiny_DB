package org.example.Operations.impl;

import org.example.Operations.TransactionManagerInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionManager implements TransactionManagerInterface {

    private final TransactionOperations transactionOperations;

    public TransactionManager(TransactionOperations transactionOperations) {
        this.transactionOperations = transactionOperations;
    }

    @Override
    public void insertRow(String tableName, String colName, String values) throws IOException {
        Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
        if (tableData == null) {
            System.out.println("Table " + tableName + " does not exist.");
            return;
        }
        String[] columns = colName.split(",\\s*");
        String[] valueArray = splitValues(values);

        if (columns.length != valueArray.length) {
            System.out.println("Number of columns and values must match.");
            return;
        }

        for (String column : columns) {
            if (!tableData.containsKey(column.trim())) {
                System.out.println("Column " + column.trim() + " does not exist in table " + tableName + ".");
                return;
            }
        }

        for (int i = 0; i < columns.length; i++) {
            tableData.get(columns[i].trim()).add(valueArray[i].trim());
        }
    }

    @Override
    public void deleteRow(String tableName, String condition) throws IOException {
        Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
        if (tableData == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }

        String[] conditionParts = condition.split("=");
        String columnName = conditionParts[0].trim();
        String value = conditionParts[1].trim();

        List<String> columnData = tableData.get(columnName);
        if (columnData == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist.");
        }

        for (int i = 0; i < columnData.size(); i++) {
            if (columnData.get(i).equals(value)) {
                for (Map.Entry<String, List<String>> entry : tableData.entrySet()) {
                    entry.getValue().remove(i);
                }
                break;
            }
        }
    }

//    @Override
//    public void updateToTable(String tableName, String colValuePair, String condition) throws IOException {
//        Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
//        if (tableData == null) {
//            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
//        }
//        String[] conditionParts = condition.split("=");
//        String conditionColumn = conditionParts[0].trim();
//        String conditionValue = conditionParts[1].trim();
//
//        String[] updateParts = colValuePair.split("=");
//        String updateColumn = updateParts[0].trim();
//        String updateValue = updateParts[1].trim();
//
//        List<String> conditionColumnData = tableData.get(conditionColumn);
//        if (conditionColumnData == null) {
//            throw new IllegalArgumentException("Column " + conditionColumn + " does not exist.");
//        }
//
//        for (int i = 0; i < conditionColumnData.size(); i++) {
//            if (conditionColumnData.get(i).equals(conditionValue)) {
//                tableData.get(updateColumn).set(i, updateValue);
//                break;
//            }
//        }
//    }
//
//    @Override
//    public void selectFromTable(String tableName, String columns, String whereCondition) throws IOException {
//        Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
//        if (tableData == null) {
//            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
//        }
//
//        String[] columnArray;
//        if (columns.trim().equals("*")) {
//            columnArray = tableData.keySet().toArray(new String[0]);
//        } else {
//            columnArray = columns.split(",\\s*");
//        }
//        List<Integer> selectedIndices = new ArrayList<>();
//
//        // Check if all specified columns exist in the table
//        for (String column : columnArray) {
//            if (!tableData.containsKey(column.trim())) {
//                throw new IllegalArgumentException("Column " + column + " does not exist in the table " + tableName);
//            }
//        }
//
//        if (!whereCondition.equals("none")) {
//            String[] conditionParts = whereCondition.split("=");
//            if (conditionParts.length != 2) {
//                throw new IllegalArgumentException("Invalid WHERE condition format. Expected format: column=value");
//            }
//
//            String conditionColumn = conditionParts[0].trim();
//            String conditionValue = conditionParts[1].trim();
//
//            List<String> conditionColumnData = tableData.get(conditionColumn);
//            if (conditionColumnData == null) {
//                throw new IllegalArgumentException("Column " + conditionColumn + " does not exist in the table " + tableName);
//            }
//
//            for (int i = 0; i < conditionColumnData.size(); i++) {
//                if (conditionColumnData.get(i).equals(conditionValue)) {
//                    selectedIndices.add(i);
//                }
//            }
//        } else {
//            for (int i = 0; i < tableData.values().iterator().next().size(); i++) {
//                selectedIndices.add(i);
//            }
//        }
//
//        for (int index : selectedIndices) {
//            for (String column : columnArray) {
//                List<String> columnData = tableData.get(column.trim());
//                if (columnData != null && index < columnData.size()) {
//                    System.out.print(columnData.get(index) + " | ");
//                } else {
//                    System.out.print("null | "); // Handle missing data gracefully
//                }
//            }
//            System.out.println();
//        }
//    }

@Override
public void updateToTable(String tableName, String colValuePair, String condition) {
    Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
    if (tableData == null) {
        System.out.println("Table " + tableName + " does not exist.");
        return;
    }

    String[] colValue = colValuePair.split("\\="); // Split colValuePair into column name and value
    int rowsReturned = 0;

    if (!colValuePair.contains("=")) {
        System.out.println("Illegal expression");
        return;
    }

    // Check for valid condition
    if (!condition.contains("=") && !condition.contains("<") && !condition.contains(">") &&
            !condition.contains("!=") && !condition.contains("<=") && !condition.contains(">=")) {
        System.out.println("Illegal expression: Condition must contain '=', '!=', '<', '<=', '>', or '>='");
        return;
    }

    // Split condition based on supported operators
    String[] conditions;
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
        System.out.println("Unsupported condition operator");
        return;
    }

    // Locate conditionIndex and colToModifyIndex
    int conditionIndex = -1;
    int colToModifyIndex = -1;

    // Iterate through tableData map to find column indices
    List<String> headers = new ArrayList<>(tableData.keySet());
    for (int i = 0; i < headers.size(); i++) {
        String header = headers.get(i);
        if (header.trim().equals(conditions[0].trim())) {
            conditionIndex = i;
        }
        String cleanedValue = colValue[0].trim().replaceAll("^'|'$", "");
        if (header.trim().equals(cleanedValue)) {
            colToModifyIndex = i;
        }
    }

    if (conditionIndex < 0) {
        System.out.println("Unknown column " + conditions[0]);
        return;
    }

    if (colToModifyIndex < 0) {
        System.out.println("Unknown column " + colValue[0]);
        return;
    }

    String cleanedValue = conditions[1].trim().replaceAll("^'|'$", "");
    // Iterate through table data
    for (int i = 0; i < tableData.get(headers.get(conditionIndex)).size(); i++) {
        String dataValue = tableData.get(headers.get(conditionIndex)).get(i);

        boolean conditionMet = false;
        if (condition.contains("!=")) {
            conditionMet = !dataValue.trim().equals(cleanedValue);
        } else if (condition.contains("<=")) {
            try {
                int valueInFile = Integer.parseInt(dataValue.trim());
                int valueInCondition = Integer.parseInt(cleanedValue);
                conditionMet = valueInFile <= valueInCondition;
            } catch (NumberFormatException e) {
                System.out.println("Comparison failed: Non-integer value found in column");
                return;
            }
        } else if (condition.contains(">=")) {
            try {
                int valueInFile = Integer.parseInt(dataValue.trim());
                int valueInCondition = Integer.parseInt(cleanedValue);
                conditionMet = valueInFile >= valueInCondition;
            } catch (NumberFormatException e) {
                System.out.println("Comparison failed: Non-integer value found in column");
                return;
            }
        } else if (condition.contains("=")) {
            conditionMet = dataValue.trim().equals(cleanedValue);
        } else if (condition.contains("<")) {
            try {
                int valueInFile = Integer.parseInt(dataValue.trim());
                int valueInCondition = Integer.parseInt(cleanedValue);
                conditionMet = valueInFile < valueInCondition;
            } catch (NumberFormatException e) {
                System.out.println("Comparison failed: Non-integer value found in column");
                return;
            }
        } else if (condition.contains(">")) {
            try {
                int valueInFile = Integer.parseInt(dataValue.trim());
                int valueInCondition = Integer.parseInt(cleanedValue);
                conditionMet = valueInFile > valueInCondition;
            } catch (NumberFormatException e) {
                System.out.println("Comparison failed: Non-integer value found in column");
                return;
            }
        }

        // Update matching rows
        if (conditionMet) {
            rowsReturned++;
            tableData.get(headers.get(colToModifyIndex)).set(i, colValue[1].trim().replaceAll("^'|'$", ""));
        }
    }
}

@Override
public void selectFromTable(String tableName, String columnsJoint, String condition) throws IOException{
    //Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
    Map<String, List<String>> tableData = transactionOperations.allTableData.get(tableName);
    if (tableData == null) {
        System.out.println("Table " + tableName + " does not exist.");
        return;
    }
    Map<String, Integer> columnToIndex = new HashMap<>();
    // Split the column names
    String[] columns = columnsJoint.split(",\\s*");
    //String[] columnSeparated = columns.split(",\\s*");
    int rowsReturned = 0;

    // Check for valid condition
    if (!condition.contains("=") && !condition.contains("<") && !condition.contains(">") &&
            !condition.contains("!=") && !condition.contains("<=") && !condition.contains(">=") &&
            !condition.contains("none")) {
        System.out.println("Illegal expression: Condition must contain '=', '!=', '<', '<=', '>', or '>='");
        return;
    }

    // Split condition based on supported operators
    String[] conditions = null;
    if(!condition.equals("none")){
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
            System.out.println("Unsupported condition operator");
            return;
        }
    }

    // Check if columns[0] is "*"
    boolean selectAllColumns = columns[0].equals("*");

    // Get headers from tableData (assuming all columns have the same number of entries)
    List<String> headers = new ArrayList<>(tableData.keySet());

    // Populate columnToIndex map with column name and index
    for (int i = 0; i < headers.size(); i++) {
        columnToIndex.put(headers.get(i), i);
    }

    // Find index of condition column
    int conditionIndex = -1;
    if(!condition.equals("none")){if (columnToIndex.containsKey(conditions[0].trim())) {
        conditionIndex = columnToIndex.get(conditions[0].trim());
    } else {
        System.out.println("Unknown column " + conditions[0]);
        return;
    }
    }


    // Print header line
    if (selectAllColumns) {
        for (String header : headers) {
            System.out.print(header + "|");
        }
        System.out.println();
    } else {
        for (String column : columns) {
            if (columnToIndex.containsKey(column)) {
                System.out.print(column + "|");
            }
        }
        System.out.println();
    }

    // Iterate through data rows
    for (int i = 0; i < tableData.get(headers.get(0)).size(); i++) {
        List<String> data = new ArrayList<>();
        for (String header : headers) {
            data.add(tableData.get(header).get(i));
        }

        boolean conditionMet = false;
        if(!condition.equals("none")){
            String dataValue = data.get(conditionIndex).trim();
            if (condition.contains("!=")) {
                conditionMet = !dataValue.equals(conditions[1].trim().replaceAll("^'|'$", ""));
            } else if (condition.contains("<=")) {
                try {
                    int valueInFile = Integer.parseInt(dataValue);
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile <= valueInCondition;
                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return;
                }
            } else if (condition.contains(">=")) {
                try {
                    int valueInFile = Integer.parseInt(dataValue);
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile >= valueInCondition;
                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return;
                }
            } else if (condition.contains("=")) {
                conditionMet = dataValue.equals(conditions[1].trim().replaceAll("^'|'$", ""));
            } else if (condition.contains("<")) {
                try {
                    int valueInFile = Integer.parseInt(dataValue);
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile < valueInCondition;
                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return;
                }
            } else if (condition.contains(">")) {
                try {
                    int valueInFile = Integer.parseInt(dataValue);
                    int valueInCondition = Integer.parseInt(conditions[1].trim());
                    conditionMet = valueInFile > valueInCondition;
                } catch (NumberFormatException e) {
                    System.out.println("Comparison failed: Non-integer value found in column");
                    return;
                }
            }
        }else{
            conditionMet=true;
        }



        // Print matching rows
        if (conditionMet) {
            rowsReturned++;
            if (selectAllColumns) {
                for (String value : data) {
                    System.out.print(value + "|");
                }
                System.out.println();
            } else {
                for (String column : columns) {
                    if (columnToIndex.containsKey(column)) {
                        System.out.print(data.get(columnToIndex.get(column)) + "|");
                    }
                    else{
                        System.out.println("Unknown column " + column);
                        return;
                    }
                }
                System.out.println();
            }
        }
    }
    System.out.println(rowsReturned + " rows returned");
}


    private String[] splitValues(String values) {
        return values.split(",(?=([^']*'[^']*')*[^']*$)");
    }

}

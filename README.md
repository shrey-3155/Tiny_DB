
# TinyDB - Database Management System

## Objective

A Database Management System developed in core JAVA which performs all operations like queries, transaction, reverse engineering, SQL Dump and log management with a user interface for Login Signup

---

## Project Features

### 1. **Database Design**
- **Data Structure Selection**:
    - A linear data structure i.e. array[] has been chosen to process queries and manage data.
    - Data is stored in a custom file format i.e. txt file where data is separated by a "|" symbol.
    - Hash Map is used to store data in buffer till the final execution in file is made.
- **Custom File Format**:
    - Custom-designed file format used to store persistent data and maintain the data dictionary/metadata.
    - The file structure design decisions are detailed in a separate document, outlining the structure and storage logic.

### 2. **Query Implementation**
- Supported SQL Operations:
    - `CREATE DATABASE`
    - `USE DATABASE`
    - `CREATE TABLE`
    - `INSERT INTO TABLE`
    - `SELECT FROM` (with a single `WHERE` condition)
    - `UPDATE TABLE` (with a single `WHERE` condition)
    - `DELETE FROM TABLE` (with a single `WHERE` condition)
    - `DROP TABLE`

### 3. **Transaction Processing**
- Transactions are handled separately from regular queries.
- Transaction operations are executed in-memory using the selected data structure and only written to persistent storage after commit.
- Transactions adhere to ACID properties (achieved via log scanning).

### 4. **Log Management**
- Three types of logs are maintained:
    1. **General Logs**: Captures query execution times and the state of the database (e.g., the number of tables and records).
    2. **Event Logs**: Captures database changes, concurrent transactions, crash reports, etc.
    3. **Query Logs**: Records user queries with a timestamp.
- Logs are written in JSON format.

### 5. **Data Modelling - Reverse Engineering**
- The system generates an Entity-Relationship Diagram (ERD) based on the current state of the database.
- Output: A text file containing the ERD representation, including tables, columns, relationships, and cardinality.

### 6. **Export Structure & Data**
- The system can export both the structure and data of a selected database in standard SQL format.
- The SQL dump captures the current state of the database, reflecting any changes made during operations (e.g., `UPDATE` statements).

### 7. **User Interface & Login Security**
- Basic console-based UI with the following features:
    1. **User Registration**:
        - Users provide a UserID, password, and answers to security questions.
        - Data is stored in a text file, with UserID and password hashed (using MD5 or SHA1).
    2. **Login**:
        - Users log in using their UserID, hashed password, and security answers.
    3. **Menu Options**:
        - Write Queries
        - Export Data and Structure
        - Generate ERD
        - Exit

---

## How to Run

1. Run the TinyDB.java file which has the main method
2. A user interface will show up with options to login/signup
3. After that all the features of the system will show up in the interface.

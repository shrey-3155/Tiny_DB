CREATE DATABASE test;
USE test;

CREATE TABLE Consumers (
    email varchar(255) None,
    Name varchar(255) None,
    ID int PRIMARY KEY

);

INSERT INTO Consumers VALUES (ID, Name, email);
INSERT INTO Consumers VALUES (1, jason, email@gmail);
INSERT INTO Consumers VALUES (2, smothj, email@gmail);
INSERT INTO Consumers VALUES (3, what, email@gmail);


CREATE TABLE Orders (
    PersonID int FOREIGN KEY REFERENCES Persons(ID),
    OrderNumber varchar(255) None,
    OrderID int PRIMARY KEY

);

INSERT INTO Orders VALUES (PersonID, OrderNumber, OrderID);
INSERT INTO Orders VALUES (1, abc, 340);
INSERT INTO Orders VALUES (2, efg, 341);
INSERT INTO Orders VALUES (3, xyz, 342);
INSERT INTO Orders VALUES (4, , 343);
INSERT INTO Orders VALUES (5, , 344);



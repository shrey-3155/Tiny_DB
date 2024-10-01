CREATE DATABASE test;
USE test;

CREATE TABLE Consumers (
    email varchar(255),
    Name varchar(255),
    ID int PRIMARY KEY

);

INSERT INTO Consumers VALUES (1, 'email@gmail', 'jason');
INSERT INTO Consumers VALUES (2, 'email@gmail', 'smothj');
INSERT INTO Consumers VALUES (3, 'email@gmail', 'what');
INSERT INTO Consumers VALUES (4, 'email@gmail', 'Alice');


CREATE TABLE Orders (
    PersonID int FOREIGN KEY REFERENCES Persons(ID),
    OrderNumber varchar(255),
    OrderID int PRIMARY KEY

);

INSERT INTO Orders VALUES (1, 'abc', 340);
INSERT INTO Orders VALUES (2, 'efg', 341);
INSERT INTO Orders VALUES (3, 'xyz', 342);
INSERT INTO Orders VALUES (4, 'hh', 343);
INSERT INTO Orders VALUES (5, 'hh', 344);
INSERT INTO Orders VALUES (6, 'klm', 345);
INSERT INTO Orders VALUES (7, 'notRandom', 346);
INSERT INTO Orders VALUES (8, 'notRandom', 347);
INSERT INTO Orders VALUES (9, '', 348);
INSERT INTO Orders VALUES (10, '', 349);



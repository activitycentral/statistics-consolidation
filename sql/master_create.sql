CREATE TABLE Usages ( 
		timeStamp TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
		userId INTEGER NOT NULL, 
		resourceId INTEGER NOT NULL, 
		startDate TIMESTAMP NOT NULL, 
		dataType INTEGER NOT NULL, 
		data INTEGER NOT NULL);

CREATE TABLE Resources (
	id INTEGER auto_increment unique,	
	name CHAR(250),
	PRIMARY KEY (name)
);

CREATE TABLE Users(
	id INTEGER auto_increment unique,	
	machineSN CHAR(80),
	age INTEGER NOT NULL,
	school CHAR(80),
	software_version CHAR (80),
	timeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (machineSN)
);

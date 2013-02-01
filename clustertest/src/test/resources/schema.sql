CREATE SEQUENCE seq;

create table Event(
	id number primary key,
	aggregateid varchar(255),
	class varchar(255),
	event CLOB,
	kryo BOOLEAN,
  kryoeventdata BLOB,
);

create table Saga(
	id varchar(255),
	clazz varchar(255),
	state tinyint
);
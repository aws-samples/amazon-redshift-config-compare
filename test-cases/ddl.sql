create table if not exists example_table
(id INTEGER IDENTITY(1, 1) NOT NULL, column_value varchar(10), insert_timestamp timestamp default sysdate);

insert into example_table (column_value) values('data');

drop table employee_batch;

create table employee_batch (
    "EMP_NUMBER" NUMBER(6,0) GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, 
	"EMP_NAME" VARCHAR2(100 BYTE), 
	"EMP_JOIN_DT" DATE NOT NULL, 
	"EMP_ADDR" VARCHAR2(1200 BYTE), 
    PRIMARY KEY ("EMP_NUMBER")
);

insert into employee_batch(EMP_NAME, EMP_JOIN_DT, EMP_ADDR) values ('SpringBatch', SYSDATE, 'Lake View St.'); 
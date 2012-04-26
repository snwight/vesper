create table PERSON(
  ID integer primary key,
  NAME varchar not null 
);
create table STUDENT(
  ROLLNO integer primary key,
  DEGREE varchar,
  ID integer unique not null,
  foreign key(ID) references PERSON(ID)
);
create table PROFESSOR(
  ID integer primary key,
  TITLE varchar,
  constraint PERSON_FK foreign key(ID) references PERSON(ID)
);
create table DEPT(
  CODE varchar primary key,
  NAME varchar unique not null
);
create table SEMESTER(
  SNO integer primary key,
  YEAR date not null,
  SESSION varchar,
  check (SESSION in ('SPRING', 'SUMMER', 'FALL'))
);
create table COURSE(
  CNO integer primary key,
  TITLE varchar,
  CODE varchar not null,
  foreign key (CODE) references DEPT(CODE)
);
create table OFFER(
  ONO integer primary key,
  CNO integer,
  SNO integer,
  PID integer,
  CONO integer,
  foreign key (CNO) references COURSE(CNO),
  foreign key (SNO) references SEMESTER(SNO),
  foreign key (PID) references PROFESSOR(ID),
  foreign key (CONO) references OFFER(ONO)
);
create table STUDY(
  ONO integer,
  RNO integer,
  GRADE varchar,
  foreign key (ONO) references OFFER(ONO),
  foreign key (RNO) references STUDENT(ROLLNO),
  constraint STUDY_PK primary key (ONO, RNO)
);
create table REG(
  SID integer,
  SNO integer,
  foreign key (SNO) references SEMESTER(SNO),
  foreign key (SID) references STUDENT(ID),
  constraint REG_PK primary key (SID, SNO)
);
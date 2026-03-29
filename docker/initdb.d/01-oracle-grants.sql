-- init/01-oracle-grants.sql

ALTER SESSION SET CONTAINER=taskiq;

CREATE USER taskiq_user IDENTIFIED BY taskiq_pwd;

-- Basic permissions
GRANT CREATE SESSION TO taskiq_user;
GRANT CREATE TABLE TO taskiq_user;
GRANT CREATE VIEW TO taskiq_user;
GRANT CREATE SEQUENCE TO taskiq_user;
ALTER USER taskiq_user quota unlimited on USERS;

-- AQ permissions
-- TODO: Are all these permissions required? Can we reduce them?
GRANT EXECUTE ON dbms_aq TO taskiq_user;
GRANT RESOURCE TO taskiq_user;
GRANT CONNECT TO taskiq_user;
GRANT EXECUTE ANY PROCEDURE TO taskiq_user;
GRANT aq_administrator_role TO taskiq_user;
GRANT aq_user_role TO taskiq_user;
GRANT EXECUTE ON dbms_aqadm TO taskiq_user;
GRANT EXECUTE ON dbms_aq TO taskiq_user;
GRANT EXECUTE ON dbms_aqin TO taskiq_user;

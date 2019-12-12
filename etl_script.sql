/* 
    Lab 8 - Dimensional Model and ETL
    Student name: Chenxi Zhang
    Student number: C16434996
    Description: 
        Using create table statement to create staging and dimension tables
        by using multiple sequences to hold the count for surrogate keys in staging tables
        construct procedures for loading and generating surrogate keys
        the loading of data from different tables uses MERGE command to MERGE the data from one table to another
        the generation of surrogate keys is finding whether the name of the team/player are the same,
        if they are, that means they are the same entity => same surrogate keys
        tournaments are distinct so it doesn't need any comparasion.
*/


/*
    STAGING TABLES
*/

-- DROP all tables
DROP TABLE Date_DIM CASCADE CONSTRAINTS PURGE;
DROP TABLE TEAM_STAGING;
DROP TABLE PLAYER_STAGING;
DROP TABLE TOURNAMENT_STAGING;
DROP TABLE FACTRESULT_STAGING;

-- Update the tables from DB change dollars to euro by 1 = 1.3
UPDATE Results1 SET price = price * 1.3;

-- Sequence for handling surrogate keys
DROP SEQUENCE TEAM_STAGING_SEQ;
CREATE SEQUENCE TEAM_STAGING_SEQ START WITH 1 MINVALUE 1 INCREMENT BY 1 NOMAXVALUE;

-- Create Date dimension
CREATE TABLE Date_DIM AS
    SELECT
        n AS Date_SK,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'DD') AS Day,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'MM') AS Month,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'YYYY') AS Year,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'WW') AS Week,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'Q') AS Quarter,
        TO_CHAR(TO_DATE('31/12/2013','DD/MM/YYYY') + NUMTODSINTERVAL(n,'day'),'D') AS DayOfWeek
    FROM (
        SELECT level n
        from dual
        connect by level <= 2000
    );


-- Alter Date_DIM to make Date_SK to be the PRIMARY KEY
ALTER TABLE Date_DIM
ADD CONSTRAINT Date_DIM_PK PRIMARY KEY (Date_SK);


-- Staging table for Team table
CREATE TABLE TEAM_STAGING (
    team_id         INTEGER,
    team_name       VARCHAR2(100),
    
    sourceDB        INTEGER,
    teamSK          INTEGER
);

-- Sequence for recording surrogate key for PLAYER_STAGING table
DROP SEQUENCE PLAYER_STAGING_SEQ;
CREATE SEQUENCE PLAYER_STAGING_SEQ START WITH 1 MINVALUE 1 INCREMENT BY 1 NOMAXVALUE;


-- Staging table for Player table
CREATE TABLE PLAYER_STAGING (
    p_id            INTEGER,
    p_name          VARCHAR2(100),
    team_id         INTEGER,
    
    sourceDB        INTEGER,
    playerSK        INTEGER,
    teamSK          INTEGER
);


-- Sequence for recording surrogate key for TOURNAMENT_STAGING table
DROP SEQUENCE TOURNAMENT_STAGING_SEQ;
CREATE SEQUENCE TOURNAMENT_STAGING_SEQ START WITH 1 MINVALUE 1 INCREMENT BY 1 NOMAXVALUE;


-- Staging table for Tournament table
CREATE TABLE TOURNAMENT_STAGING (
    t_id            INTEGER,
    t_description   VARCHAR2(100),
    t_date          DATE,
    total_price     FLOAT,

    sourceDB        INTEGER,
    tournamentSK    INTEGER
);


-- Staging table for Fact table
CREATE TABLE FACTRESULT_STAGING (
    t_id            INTEGER,
    p_id            INTEGER,
    rank            INTEGER,
    t_date          DATE,

    price           FLOAT,
    sourceDB        INTEGER,
    tournamentSK    INTEGER,
    playerSK        INTEGER,
    dateSK          INTEGER,
    teamSK          INTEGER
);


-- Procedure for loading data from TEAM1 and TEAM2 to TEAM_STAGING table
create or replace PROCEDURE LOAD_TEAMS_STAGING IS
BEGIN
    -- INSERT data from TEAM1 and TEAM2 table to Team_Staing table
    MERGE INTO TEAM_STAGING st USING
    (SELECT team_id, team_name FROM TEAM1) t
    ON (st.team_id = t.team_id AND st.team_name = t.team_name AND st.sourceDB = 1)
    WHEN NOT MATCHED THEN
    INSERT (st.team_id, st.team_name, st.sourceDB) VALUES (t.team_id, t.team_name, 1);

    COMMIT;

    MERGE INTO TEAM_STAGING st USING
    (SELECT team_id, team_name FROM TEAM2) t
    ON (st.team_id = t.team_id AND st.team_name = t.team_name AND st.sourceDB = 2)
    WHEN NOT MATCHED THEN
    INSERT (st.team_id, st.team_name, st.sourceDB) VALUES (t.team_id, t.team_name, 2);
    COMMIT;
END LOAD_TEAMS_STAGING;
/


-- Procedure for generating TEAM_STAGING surrogate keys
CREATE OR REPLACE PROCEDURE TEAM_STAGING_GETSK AS
    -- Get all the records that have empty surrogate key
    CURSOR cur IS SELECT * FROM TEAM_STAGING WHERE teamSK IS NULL;
    -- For getting the surrogate key if exists in some other record that means the same entity
    surrkey INTEGER;
BEGIN
    -- Loop through the records
    FOR cur_row IN cur
    LOOP
        -- Set surrkey initially empty
        surrkey := NULL;
        BEGIN
            -- Try to find a surrogate key with this current record's information
            SELECT teamSK INTO surrkey FROM TEAM_STAGING WHERE team_name = cur_row.team_name AND teamSK IS NOT NULL FETCH FIRST ROW ONLY;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- If NO_DATA_FOUND (this is a new entry), generate a new key from sequence.nextval() and UPDATE current record's surrogate key
            UPDATE TEAM_STAGING SET teamSK = TEAM_STAGING_SEQ.nextval WHERE team_id = cur_row.team_id AND sourceDB = cur_row.sourceDB;
            COMMIT;
        END;

        -- If the above SELECT statement returned a key, insert the surrogate key by UPDATING the current record
        IF surrkey IS NOT NULL THEN
                UPDATE TEAM_STAGING SET teamSK = surrkey WHERE team_id = cur_row.team_id AND sourceDB = cur_row.sourceDB;
                COMMIT;
        END IF;
    END LOOP;
END TEAM_STAGING_GETSK;
/


-- Procedure for loading data from PLAYERS1 and PLAYERS2 to PLAYER_STAGING table
CREATE OR REPLACE PROCEDURE LOAD_PLAYERS_STAGING AS
BEGIN
    -- INSERT data from PLAYERS1
    MERGE INTO PLAYER_STAGING st USING
    (SELECT p_id, CONCAT(CONCAT(p_name, ' '), p_sname) as name, team_id FROM PLAYERS1) t
    ON (st.p_id = t.p_id AND st.p_name = t.name AND st.team_id = t.team_id AND st.sourceDB = 1)
    WHEN NOT MATCHED THEN
    INSERT (st.p_id, st.p_name, st.team_id, st.sourceDB, st.teamSK) VALUES (t.p_id, t.name, t.team_id, 1, (SELECT teamSK FROM TEAM_STAGING WHERE team_id = t.team_id AND sourceDB = 1 FETCH FIRST ROW ONLY));

    COMMIT;

    -- INSERT data from PLAYERS2
    MERGE INTO PLAYER_STAGING st USING
    (SELECT p_id, CONCAT(CONCAT(p_name, ' '), p_sname) as name, team_id FROM PLAYERS2) t
    ON (st.p_id = t.p_id AND st.p_name = t.name AND st.team_id = t.team_id AND st.sourceDB = 2)
    WHEN NOT MATCHED THEN
    INSERT (st.p_id, st.p_name, st.team_id, st.sourceDB, st.teamSK) VALUES (t.p_id, t.name, t.team_id, 2, (SELECT teamSK FROM TEAM_STAGING WHERE team_id = t.team_id AND sourceDB = 2 FETCH FIRST ROW ONLY));

    COMMIT;
END LOAD_PLAYERS_STAGING;
/


-- Procedure for generating PLAYER_STAGING surrogate keys
CREATE OR REPLACE PROCEDURE PLAYER_STAGING_GETSK AS
    -- Get all the records that have empty surrogate key
    CURSOR cur IS SELECT * FROM PLAYER_STAGING WHERE playerSK IS NULL;
    -- For getting the surrogate key if exists in some other record that means the same entity
    surrkey INTEGER;
BEGIN
    -- Loop through the records
    FOR cur_row IN cur
    LOOP
        -- Set surrkey initially empty
        surrkey := NULL;
        BEGIN
            -- Try to find a surrogate key with this current record's information
            SELECT playerSK INTO surrkey FROM PLAYER_STAGING WHERE p_name = cur_row.p_name AND playerSK IS NOT NULL FETCH FIRST ROW ONLY;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- If NO_DATA_FOUND (this is a new entry), generate a new key from sequence.nextval() and UPDATE current record's surrogate key
            UPDATE PLAYER_STAGING SET playerSK = PLAYER_STAGING_SEQ.nextval WHERE p_id = cur_row.p_id AND sourceDB = cur_row.sourceDB;
            COMMIT;
        END;
        
        -- If the above SELECT statement returned a key, insert the surrogate key by UPDATING the current record
        IF surrkey IS NOT NULL THEN
                UPDATE PLAYER_STAGING SET playerSK = surrkey WHERE p_id = cur_row.p_id AND sourceDB = cur_row.sourceDB; 
                COMMIT;
        END IF;
    END LOOP;
END PLAYER_STAGING_GETSK;
/


-- Procedure for loading data from TOURNAMENT1 and TOURNAMENT2 to TOURNAMENT_STAGING table
CREATE OR REPLACE PROCEDURE LOAD_TOUR_STAGING AS
BEGIN
    -- INSERT data from TOURNAMENT1 and TOURNAMENT2 tables to TOURNAMENT_STAGING table
    MERGE INTO TOURNAMENT_STAGING st USING
    (SELECT t_id, t_description, total_price, t_date FROM TOURNAMENT1) t
    ON (st.t_id = t.t_id AND st.t_description = t.t_description AND st.t_date = t.t_date AND st.total_price = t.total_price AND st.sourceDB = 1)
    WHEN NOT MATCHED THEN
    INSERT (st.t_id, st.t_description, st.t_date, st.total_price, st.sourceDB, st.tournamentSK) VALUES (t.t_id, t.t_description, t.t_date, t.total_price, 1, TOURNAMENT_STAGING_SEQ.nextval);

    COMMIT;

    MERGE INTO TOURNAMENT_STAGING st USING
    (SELECT t_id, t_description, total_price, t_date FROM TOURNAMENT2) t
    ON (st.t_id = t.t_id AND st.t_description = t.t_description AND st.t_date = t.t_date AND st.total_price = t.total_price AND st.sourceDB = 2)
    WHEN NOT MATCHED THEN
    INSERT (st.t_id, st.t_description, st.t_date, st.total_price, st.sourceDB, st.tournamentSK) VALUES (t.t_id, t.t_description, t.t_date, t.total_price, 2, TOURNAMENT_STAGING_SEQ.nextval);

    COMMIT;
END LOAD_TOUR_STAGING;
/


-- Procedure for loading data from RESULTS1 and RESULTS2 to FACTRESULT_STAGING table
CREATE OR REPLACE PROCEDURE LOAD_FACT_STAGING AS
BEGIN
    -- INSERT data from RESULTS1 AND RESULTS2 tables to FACTRESULT_STAGING table
    MERGE INTO FACTRESULT_STAGING st USING
    (SELECT t_id, p_id, rank, price FROM RESULTS1) t
    ON (st.t_id = t.t_id AND st.p_id = t.p_id AND st.rank = t.rank AND st.price = t.price AND st.sourceDB = 1)
    WHEN NOT MATCHED THEN
    INSERT (st.t_id, st.p_id, st.rank, st.price, st.t_date, st.sourceDB) VALUES (t.t_id, t.p_id, t.rank, t.price, (SELECT t_date FROM TOURNAMENT2 WHERE t_id = t.t_id),1);

    COMMIT;

    MERGE INTO FACTRESULT_STAGING st USING
    (SELECT t_id, p_id, rank, price FROM RESULTS2) t
    ON (st.t_id = t.t_id AND st.p_id = t.p_id AND st.rank = t.rank AND st.price = t.price AND st.sourceDB = 2)
    WHEN NOT MATCHED THEN
    INSERT (st.t_id, st.p_id, st.rank, st.price, st.t_date, st.sourceDB) VALUES (t.t_id, t.p_id, t.rank, t.price, (SELECT t_date FROM TOURNAMENT2 WHERE t_id = t.t_id), 2);

    COMMIT;
END LOAD_FACT_STAGING;
/


-- Procedure for generating FACTRESULT_STAGING surrogate keys
CREATE OR REPLACE PROCEDURE FACT_STAGING_GETSK AS
    -- Get all the records that have empty surrogate key
    CURSOR cur IS SELECT * FROM FACTRESULT_STAGING WHERE tournamentSK IS NULL AND teamSK IS NULL AND dateSK IS NULL;
    -- For getting the surrogate key if exists in some other record that means the same entity
    team_surrkey INTEGER;
    p_surrkey INTEGER;
    date_surrkey INTEGER;
    t_surrkey INTEGER;
BEGIN
    -- Loop through the records
    FOR cur_row IN cur
    LOOP 
        -- Get surrogate key of team and player from PLAYER_STAGING table
        BEGIN
            SELECT teamSK, playerSK INTO team_surrkey, p_surrkey FROM PLAYER_STAGING WHERE p_id = cur_row.p_id AND sourceDB = cur_row.sourceDB FETCH FIRST ROW ONLY;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- If not found, output error text to screen and throw application error
            dbms_output.put_line('current data of p_id' || cur_row.p_id || ' soureDB ' || cur_row.sourceDB || ' will not be added, data not found');
            raise_application_error(-20000, 'Data not found!');
        END;

        -- Get surrogate key of tournament from TOURNAMENT_STAGING
        BEGIN
            SELECT tournamentSK INTO t_surrkey FROM TOURNAMENT_STAGING WHERE t_id = cur_row.t_id AND sourceDB = cur_row.sourceDB FETCH FIRST ROW ONLY; 
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- If not found, output error text to screen and throw application error
            dbms_output.put_line('current data of t_id' || cur_row.t_id || ' soureDB ' || cur_row.sourceDB || ' will not be added, data not found');
            raise_application_error(-20000, 'Data not found!');
        END;

        -- Get surrogate key of date from Date_DIM
        BEGIN
            SELECT date_sk INTO date_surrkey FROM DATE_DIM WHERE year = (cast(to_char(cur_row.t_date,'YYYY') as integer)) 
            AND month = (cast(to_char(cur_row.t_date,'MM') as integer)) AND day = (cast(to_char(cur_row.t_date,'DD') as integer));
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- If not found, output error text to screen and throw application error
            dbms_output.put_line('current data of t_date' || cur_row.t_date || 'is not found in Date_DIM, please check Date_DIM table');
            raise_application_error(-20000, 'Data not found!');
        END;

        UPDATE FACTRESULT_STAGING SET teamSK = team_surrkey, playerSK = p_surrkey, dateSK = date_surrkey, tournamentSK = t_surrkey WHERE p_id = cur_row.p_id AND sourceDB = cur_row.sourceDB AND t_id = cur_row.t_id AND t_date = cur_row.t_date;
        COMMIT;
    END LOOP;
END FACT_STAGING_GETSK;
/


/*
    DIMENSIONS
*/


-- DROP all tables
DROP TABLE Player_DIM CASCADE CONSTRAINTS PURGE;
DROP TABLE Team_DIM CASCADE CONSTRAINTS PURGE;
DROP TABLE Tournament_DIM CASCADE CONSTRAINTS PURGE;
DROP TABLE Fact_Results CASCADE CONSTRAINTS PURGE;


-- Create table statements for dimensional model
CREATE TABLE Player_DIM (
    Player_SK   INTEGER,
    Player_name VARCHAR2(100),

    CONSTRAINT Player_DIM_PK PRIMARY KEY (Player_SK)
);


-- Create table statements for dimensional model
CREATE TABLE Team_DIM (
    Team_SK     INTEGER,
    Team_name   VARCHAR2(100),

    CONSTRAINT Team_DIM_PK PRIMARY KEY (Team_SK)
);


-- Create table statements for dimensional model
CREATE TABLE Tournament_DIM (
    Tournament_SK       INTEGER,
    Tournament_Desc     VARCHAR2(100),
    Total_price         FLOAT,

    CONSTRAINT Tournament_DIM_PK PRIMARY KEY (Tournament_SK)
);


-- Create table statements for dimensional model
CREATE TABLE Fact_Results (
    Player_SK           INTEGER,
    Tournament_SK       INTEGER,
    Team_SK             INTEGER,
    Date_SK             INTEGER,
    Rank                INTEGER,
    price               FLOAT,

    CONSTRAINT Fact_Results_PK PRIMARY KEY (Player_SK, Tournament_SK, Team_SK, Date_SK),
    CONSTRAINT Fact_Player_FK FOREIGN KEY (Player_SK) REFERENCES Player_DIM(Player_SK),
    CONSTRAINT Fact_Team_FK FOREIGN KEY (Team_SK) REFERENCES Team_DIM(Team_SK),
    CONSTRAINT Fact_Tournament_FK FOREIGN KEY (Tournament_SK) REFERENCES Tournament_DIM(Tournament_SK),
    CONSTRAINT Fact_Date_FK FOREIGN KEY (Date_SK) REFERENCES Date_DIM(Date_SK)
);


-- Procedure for loading data from PLAYER_STAGING to Player_DIM table
CREATE OR REPLACE PROCEDURE LOAD_PLAYER_DIM AS
BEGIN
    -- Load data into Player_DIM from PLAYER_STAGING table
    MERGE INTO Player_DIM p USING
    (SELECT DISTINCT playerSK, p_name FROM PLAYER_STAGING) st
    ON (p.Player_SK = st.playerSK AND p.Player_name = st.p_name)
    WHEN NOT MATCHED THEN
    INSERT (p.Player_SK, p.Player_name) VALUES (st.playerSK, st.p_name);

    COMMIT;
END LOAD_PLAYER_DIM;
/


-- Procedure for loading data from TEAM_STAGING to Team_DIM table
CREATE OR REPLACE PROCEDURE LOAD_TEAM_DIM AS
BEGIN
    -- Load data into Team_DIM from TEAM_STAGING table
    MERGE INTO Team_DIM t USING
    (SELECT DISTINCT teamSK, team_name FROM TEAM_STAGING) st
    ON (t.Team_SK = st.teamSK AND t.Team_name = st.team_name)
    WHEN NOT MATCHED THEN
    INSERT (t.Team_SK, t.Team_name) VALUES (st.teamSK, st.team_name);

    COMMIT;
END LOAD_TEAM_DIM;
/


-- Procedure for loading data from TOURNAMENT_STAGING to Tournament_DIM table
CREATE OR REPLACE PROCEDURE LOAD_TOUR_DIM AS
BEGIN
    -- Load data into Tournament_DIM from TOURNAMENT_STAGING table
    MERGE INTO Tournament_DIM tr USING
    (SELECT DISTINCT tournamentSK, t_description, total_price FROM TOURNAMENT_STAGING) st
    ON (tr.Tournament_SK = st.tournamentSK AND tr.Tournament_Desc = st.t_description AND tr.Total_price = st.total_price)
    WHEN NOT MATCHED THEN
    INSERT (tr.Tournament_SK, tr.Tournament_Desc, tr.Total_price) VALUES (st.tournamentSK, st.t_description, st.total_price);

    COMMIT;
END LOAD_TOUR_DIM;
/


-- Procedure for loading data from FACTRESULT_STAGING to Fact_Results table
CREATE OR REPLACE PROCEDURE LOAD_FACT_DIM AS
BEGIN
    -- Load data into Fact_Results from FACTRESULT_STAGING table
    MERGE INTO Fact_Results f USING
    (SELECT DISTINCT playerSK, tournamentSK, teamSK, dateSK, rank, price FROM FACTRESULT_STAGING) st
    ON (f.Player_SK = st.playerSK AND f.Tournament_SK = st.tournamentSK AND f.Team_SK = st.teamSK AND f.Date_SK = st.dateSK)
    WHEN NOT MATCHED THEN
    INSERT (f.Player_SK, f.Tournament_SK, f.Team_SK, f.Date_SK, f.Rank, f.price) VALUES (st.playerSK, st.tournamentSK, st.teamSK, st.dateSK, st.rank, st.price);

    COMMIT;
END LOAD_FACT_DIM;
/


-- /*
--     ETL PROCESS - LOAD DATA TO STAGING TABLES AND ASSIGN SURROGATE KEY
-- */
-- -- Load data to TEAM_STAGING
-- EXECUTE LOAD_TEAMS_STAGING;
-- -- Assign surrogate keys for TEAM_STAGING
-- EXECUTE TEAM_STAGING_GETSK;
-- -- Load data to PLAYER_STAGING
-- EXECUTE LOAD_PLAYERS_STAGING;
-- -- Assign surrogate keys for PLAYER_STAGING
-- EXECUTE PLAYER_STAGING_GETSK;
-- -- Load data to TOURNAMENT_STAGING, No need for assign surrogate keys since each tournament is distinct (from lab sheet)
-- EXECUTE LOAD_TOUR_STAGING;
-- -- Load data to FACTRESULT_STAGING
-- EXECUTE LOAD_FACT_STAGING;
-- -- Assign surrogate keys for FACTRESULT_STAGING (getting all the keys from the staging tables)
-- EXECUTE FACT_STAGING_GETSK;
-- /*
--     ETL PROCESS - LOAD STAGING TABLES DATA TO DIMENSIONS
-- */
-- -- Load data to Team_DIM
-- EXECUTE LOAD_TEAM_DIM;
-- -- Load data to Player_DIM
-- EXECUTE LOAD_PLAYER_DIM;
-- -- Load data to Tournament_DIM
-- EXECUTE LOAD_TOUR_DIM;
-- -- Load data to Fact_Results
-- EXECUTE LOAD_FACT_DIM;

CREATE OR REPLACE PROCEDURE DO_ETL AS
BEGIN
/*
    ETL PROCESS - LOAD DATA TO STAGING TABLES AND ASSIGN SURROGATE KEY
*/
    -- Load data to TEAM_STAGING
    LOAD_TEAMS_STAGING();
    -- Assign surrogate keys for TEAM_STAGING
    TEAM_STAGING_GETSK();
    -- Load data to PLAYER_STAGING
    LOAD_PLAYERS_STAGING();
    -- Assign surrogate keys for PLAYER_STAGING
    PLAYER_STAGING_GETSK();
    -- Load data to TOURNAMENT_STAGING, No need for assign surrogate keys since each tournament is distinct (from lab sheet)
    LOAD_TOUR_STAGING();
    -- Load data to FACTRESULT_STAGING
    LOAD_FACT_STAGING();
    -- Assign surrogate keys for FACTRESULT_STAGING (getting all the keys from the staging tables)
    FACT_STAGING_GETSK();
/*
    ETL PROCESS - LOAD STAGING TABLES DATA TO DIMENSIONS
*/
    -- Load data to Team_DIM
    LOAD_TEAM_DIM();
    -- Load data to Player_DIM
    LOAD_PLAYER_DIM();
    -- Load data to Tournament_DIM
    LOAD_TOUR_DIM();
    -- Load data to Fact_Results
    LOAD_FACT_DIM();

    dbms_output.put_line('DONE ETL. CHECK THE TABLES');
END DO_ETL;
/

-- Run the script to do a ETL process
SET SERVEROUTPUT ON
EXECUTE DO_ETL;

/*
    SECOND LOAD (the date is missing, got to manually add a date within Date_DIM range)
*/
INSERT INTO PLAYERS1(P_ID, P_NAME, P_SNAME, TEAM_ID) VALUES(7, 'Alan', 'Parker', 1);
INSERT INTO PLAYERS1(P_ID, P_NAME, P_SNAME, TEAM_ID) VALUES(8, 'Martha', 'Bag', 2);
INSERT INTO TOURNAMENT1(T_ID, T_DESCRIPTION, TOTAL_PRICE) VALUES(5, 'Saudi Open ', 500000, '29-dec-2014');
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (5, 1, 1, 60000);
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (5, 7, 5, 20000);
INSERT INTO RESULTS1 (T_ID, P_ID, RANK, PRICE) VALUES (2, 8, 3, 1000);

-- Run again to load more data in
SET SERVEROUTPUT ON
EXECUTE DO_ETL;



-- Testing for selecting date from date dimension
-- SELECTING DATE_SK FROM T_DATE BY T_ID
-- Select date_sk from date_dim WHERE year = (select cast(to_char(t_date,'YYYY') as integer) from TOURNAMENT1 where t_id = 2)
-- AnD month = (select cast(to_char(t_date,'MM') as integer) from TOURNAMENT1 where t_id = 2) AND day = (select cast(to_char(t_date,'DD') as integer) from TOURNAMENT1 where t_id = 2);
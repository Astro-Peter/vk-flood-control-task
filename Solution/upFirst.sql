CREATE TABLE IF NOT EXISTS timesAndUsers
(
    callTime timestamp NOT NULL ,
    userId     BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS timesAndUsersIndex ON timesAndUsers(callTime);
CREATE TABLE IF NOT EXISTS Count(
    userId BIGINT primary key,
    totalCalls BIGINT DEFAULT 0
);
CREATE OR REPLACE FUNCTION Add() RETURNS TRIGGER
AS $$
    BEGIN
        UPDATE Count AS C SET totalCalls = totalCalls + 1 WHERE C.userId = NEW.userId;
        IF NOT FOUND THEN INSERT INTO Count VALUES (NEW.userId, 1);
        END IF;
        RETURN new;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION Subtract() RETURNS TRIGGER
AS $$
    BEGIN
        UPDATE Count AS C SET totalCalls = totalCalls - 1 WHERE C.userId = OLD.userId;
        RETURN old;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE TRIGGER timeUserAdd AFTER INSERT ON timesAndUsers
    FOR EACH ROW EXECUTE PROCEDURE Add();

CREATE OR REPLACE TRIGGER timeUserSubtract AFTER DELETE ON timesAndUsers
    FOR EACH ROW EXECUTE PROCEDURE Subtract();

CREATE OR REPLACE FUNCTION TimeInsert(myUserId bigint) RETURNS BIGINT
AS $$
    DECLARE currentTime timestamp = now();
    begin
        INSERT INTO timesAndUsers(callTime, userId) VALUES (currentTime, myUserId);
        DELETE FROM timesAndUsers WHERE callTime < currentTime - interval '4 seconds';
        RETURN (SELECT totalCalls FROM Count WHERE userId = myUserId);
    end;
$$ LANGUAGE 'plpgsql';

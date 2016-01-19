CREATE FUNCTION Athlete_Info(name varchar, gender varchar, nation_code varchar, event varchar) RETURN int
AS LANGUAGE JAVA
NAME 'Athlete.Athlete_Insert(java.lang.String, java.lang.String, java.lang.String, java.lang.String) return int';
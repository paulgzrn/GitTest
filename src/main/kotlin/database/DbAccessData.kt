package database

class DbAccessData {
    companion object {
        val url : String = "jdbc:postgresql://localhost:5433/serviceware"
        val userName : String = "postgres"
        val password : String = "pwd"

        val createTableQuery = "create table if not exists processEntities(processID text, timestamp text, position text, valueType text, CONSTRAINT key PRIMARY KEY(processID, timestamp, position));"

    }

}
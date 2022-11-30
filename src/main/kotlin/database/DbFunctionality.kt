package database

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement

class DbFunctionality {

    companion object { // enthält die static Methoden (Keyword "static" gibt es in Kotlin nicht)


        public fun createTable(connection : Connection, createTableQuery : String) {

            try {
                val statement: Statement = connection.createStatement();
                statement.executeUpdate(createTableQuery);
                println("Table created successfully!")
            } catch (e: Exception) {
                println("Creating table failed!")
                e.printStackTrace()
            }
        }


        public fun writeToTableProcessEntities(connection: Connection, processId : String, timestamp : String,
                                               position : String, valueType : String) {
            val writeQuery : String = "insert into processEntities values ('" + processId + "', '" + timestamp +
                    "', '" + position + "', '" + valueType + "');"
            // Execute Query
            writeToDatabase(connection, writeQuery)
            println("Writing process data to database successful!")
        }

        public fun connectAndWriteToTableProcessEntities(processId : String, timestamp : String,
                                                         position : String, valueType : String) {
            // Connect to database
            val connection : Connection = connectToDatabase(DbAccessData.url, DbAccessData.userName, DbAccessData.password)!!
            // SQL Query
            val writeQuery : String = "insert into processEntities values ('" + processId + "', '" + timestamp +
                    "', '" + position + "', '" + valueType + "');"
            // Execute Query
            writeToDatabase(connection, writeQuery)
            println("Writing process data to database successful!")
        }

        /**
         * Connects to the given database and returns the connection object.
         *
         * Creates Database connection and returns it.
         * @param location Source location of database (local or remote), e.g., "localhost:5432"
         * @param name Name of the database within the source, e.g., "UniversityDatabase"
         * @param userName Username for accessing the database (default usually "postgres")
         * @param password Password fitting to the given username
         *
         * @return Connection Object that represents a connection to the given database, null if Connection failed
         */
        public fun connectToDatabase(location: String, name: String, userName: String, password: String): Connection? {

            val dbPrefix: String = "jdbc:postgresql://"
            return connectToDatabase(dbPrefix + location + name, userName, password)
        }

        /**
         * Connects to the given database and returns the connection object.
         *
         * @param url Url of database, e.g., "jdbc:postgresql://localhost:5432/lokal_TestDatabase"
         * @param userName Username for accessing the database (default usually "postgres")
         * @param password Password fitting to the given username
         *
         * @return Connection Object that represents a connection to the given database, null if Connection failed
         */
        public fun connectToDatabase(url: String, userName: String, password: String): Connection? {

            // Database must be running for successful access.
            try {
                Class.forName("org.postgresql.Driver") // load the postgreSQL Driver
                val connection : Connection = DriverManager.getConnection(url, userName, password);
                println("Connection successful!")
                return connection
            } catch (e: Exception) {
                println("Connection to database failed!")
                e.printStackTrace()
            }
            return null // hier kommt es nur im Falle einer Exception hin
        }


        /**
         * Writes values to a databases table by executing the given query.
         *
         * @param connection Established connection to database that is written to
         * @param writeQuery SQL-Query to insert values into a table (e.g., "insert into tableName (info) values ('lol')"
         *
         * @return Executed SQL-Query if execution was successful, null otherwise
         */
        public fun writeToDatabase(connection: Connection, writeQuery: String): String? {

            try {
                val statement: Statement = connection.createStatement();
                statement.executeUpdate(writeQuery);
                return writeQuery;
            } catch (e: Exception) {
                println("Writing to database failed!")
                e.printStackTrace()
            }
            return null // hier kommt es nur im Falle einer Exception hin
        }


        /**
         * Reads from connected database according to the given query and returns the ResulSet.
         *
         * @param connection Established connection to database that is read from
         * @param readQuery SQL-Query read values from a table (e.g., "select * from tableName")
         *
         * @return ResultSet-Object containing the result of the query, null if reading failed
         */
        public fun readFromDatabase(connection: Connection, readQuery: String): ResultSet? {
            try {
                val statement: Statement = connection.createStatement()
                return statement.executeQuery(readQuery);
            } catch (e: Exception) {
                println("Reading from database failed!")
                e.printStackTrace()
            }
            return null;
        }


        /**
         * Prints the contents of a ResultSet line by line. Make sure, that alle columnLabels are contained within the
         * ResultSet, Exception otherwise very likely.
         *
         * @param resultSet ResultsSet containing the Result of an SQL Query executed beforehand
         * @param columnLabels Labels (names) of the columns within the ResultSet, values are printed in the order
         * of appearance in this parameter
         */
        public fun printResultSet(resultSet: ResultSet, columnLabels: List<String>) {

            while (resultSet.next()) {
                for (label in columnLabels) {
                    print(resultSet.getString(label) + " ")
                }
                println()
            }
        }



    }

}
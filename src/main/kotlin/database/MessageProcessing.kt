package database

import database.DbFunctionality.Companion.writeToTableProcessEntities
import java.sql.Connection

class MessageProcessing {
    private val connection : Connection;
    private val url : String;
    private val userName : String;
    private val password : String;

    init{ // = Konstruktor
        url = DbAccessData.url;
        userName = DbAccessData.userName;
        password = DbAccessData.password;

        // Connect to database
        connection = DbFunctionality.connectToDatabase(url, userName, password)!!;

        // create table if it does not exist already
        DbFunctionality.createTable(connection, DbAccessData.createTableQuery);
    }

    public fun writeMessageToDatabaseIfValuetypeIsDeployment(map : HashMap<String, String>) {
        // later cleaner Version with predicate
        // Find values of variables defined in list
        val variables : List<String> = listOf("bpmnProcessId", "timestamp", "position", "valueType")
        val resultMap : HashMap<String, String> = getValuesOfVariables(map, variables)
        // Check if all variables are contained
        for (variable in variables) {
            if (!resultMap.keys.contains(variable)) {
                println("Does not contain all keys! Missing: " + variable)
                return
            }
        }
        // Check if more variables are contained
        if (resultMap.size > variables.size) {
            println("Contains to many keys!")
            return
        }
        // Check if it is the first message of process instance and thus needs to be stored
        if (resultMap.get("valueType") != "DEPLOYMENT") { // We only want to keep one message per process at first
            return
        }
        // If program reaches this line, it is fine to write to database
        writeToTableProcessEntities(connection, resultMap.get("bpmnProcessId")!!,
            resultMap.get("timestamp")!!, resultMap.get("position")!!, resultMap.get("valueType")!!)
    }

    companion object {


        /**
         * @Paul @Philipp Diese Methode müsst ihr für jede message der RabbitMq aufrufen (z.B. leitet eure
         * processMessage Methode an diese Methode hier weiter).
         * Diese Methode prüft, ob die Nachricht gespeichert werden soll, wenn ja, speichert sie die Werte
         * "bpmnProcessId", "timestamp", "position", "intent" in einer Tabelle in der Datenbank.
         * Falls ihr diese Methode für jede Message genau einmal aufruft, enthält die Tabelle am Ende
         * so viele Einträge, wie Prozessinstanzen gestartet wurden.
         */
        public fun writeMessageToDatabaseIfIntentIsCreated(map : HashMap<String, String>) {
            // later cleaner Version with predicate
            val variables : List<String> = listOf("bpmnProcessId", "timestamp", "position", "valueType")
            val resultMap : HashMap<String, String> = getValuesOfVariables(map, variables)
            // Check if all variables are contained
            for (variable in variables) {
                if (!resultMap.keys.contains(variable)) {
                    println("Does not contain all keys! Missing: $variable")
                    return
                }
            }
            // Check if more variables are contained
            if (resultMap.size > variables.size) {
                println("Contains to many keys!")
                return
            }
            // Check if it is the first message of process instance and thus needs to be stored
            if (resultMap.get("valueType") != "DEPLOYMENT") { // We only want to keep one message per process at first
                return
            }
            // If program reaches this line, it is fine to write to database
            DbFunctionality.connectAndWriteToTableProcessEntities(resultMap.get("bpmnProcessId")!!,
                resultMap.get("timestamp")!!, resultMap.get("position")!!, resultMap.get("valueType")!!)

            println("From method writeMessageToDatabaseIfIntentIsCreated: Writing successful!")
        }


        public fun getValuesOfVariables(map : HashMap<String, String>, variables : List<String>) : HashMap<String, String> {

            var result : HashMap<String, String> = HashMap()
            for (variable in variables) {
                if (stringOccursInMap(map, variable)) {
                    result.put(variable, getValueOfVariable(map, variable))
                }
            }
            return result
        }


        private fun getValueOfVariable(map : HashMap<String, String>, variable : String) : String {
            // Important: String must not contain whitespaces!

            // Search in Keys
            for (key : String in map.keys) {
                if (key.lowercase().contains(variable.lowercase())) {
                    // Strip of possible '"'
                    var result : String = map[key]!! // == map.get(key)!!
                    if (!result.contains('"')) {
                        return result
                    }
                    var newResult : StringBuilder = StringBuilder("")
                    var index : Int = 0
                    while (index < result.length) {
                        if (result[index] != '"') {
                            newResult.append(result[index])
                        }
                        index ++
                    }
                    return newResult.toString()
                }
            }

            // Search in values
            for (value : String in map.values) {
                if (value.lowercase().contains(variable.lowercase())) {

                    val containingString : String = value
                    var index : Int = containingString.indexOf(variable);
                    if (index == -1) {
                        throw IllegalArgumentException("Carl: See Code");
                    }
                    index += variable.length; // index bis ans Ende des Variablennamens weiterschalten
                    while (containingString[index] == ':' || containingString[index] == '"') { //containingString.[index] == '"' kann eigentlich weg (ist nur zur Absicherung da)
                        index ++
                    }
                    // Wert auslesen
                    var result : StringBuilder = StringBuilder("")
                    while (containingString[index] != ':' && containingString[index] != '"' ) {
                        result.append(containingString[index])
                        index++
                    }

                    return result.toString()
                }
            }

            throw IllegalStateException("Carl: Method should only be called, when occurrence is assured!")
        }


        private fun stringOccursInMap(map : HashMap<String, String>, searchString : String) : Boolean {

            // Search in Keys
            for (key : String in map.keys) {
                if (key.lowercase().contains(searchString.lowercase())) {
                    return true;
                }
            }

            // Search in values
            for (value : String in map.values) {
                if (value.lowercase().contains(searchString.lowercase())) {
                    return true;
                }
            }

            return false; // Hier kommt es nur hin, wenn der searchString nicht gefunden wurde
        }

    }


}
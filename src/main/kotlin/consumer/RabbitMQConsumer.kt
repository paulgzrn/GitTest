package consumer

import com.rabbitmq.client.*
import database.MessageProcessing
import java.nio.charset.StandardCharsets


/**
 * A [RabbitMQConsumer] consumes and processes messages from a rabbitMQ it is connected to
 *
 */
class RabbitMQConsumer{

    private val host = "rabbitmq"
    //private val host = "localhost"
    private val consumerTag = "rmqConsumer"

    /* parameters of the queue */
    private val queueName :String = "sw-workflow-engine.zeebe.all"
    private val durable = true
    private val exclusive = false
    private val autoDelete = false
    private val arguments  = null // Map<String,Object>
    private var connection: Connection
    private val messageProcessing : MessageProcessing;




    /**
     * Establishes a connection between the RabbitMQConsumer and the specified rabbitMQ
     */
    init {
        val connectionFactory = ConnectionFactory()
        connectionFactory.host = host
        connection = connectionFactory.newConnection()
        val channel = connection.createChannel()
        channel.queueDeclare(queueName,durable,exclusive,autoDelete,arguments)

        //object for storing data in the database
        messageProcessing = MessageProcessing()

        consume(channel)
        //abc
    }


    /**
     * Starts consuming on the
     * @param channel the channel that is consumed from
     *
     */
    private fun consume(channel :Channel){

        println("$consumerTag is consuming ... ")

        /**
         * The actions to perform when a message is received
         */
        val deliverCallback= DeliverCallback { consumerTag: String?, delivery: Delivery ->
            val message = String(delivery.body, StandardCharsets.UTF_8)
            println("[$consumerTag] Received message: \n '$message'")
            processMessage(message)
        }

        /**
         * The actions to perform when a consumer is canceled
         */
        val cancelCallback = CancelCallback { consumerTag: String? -> // implement error handling
            println("[$consumerTag] was canceled")
        }

        val autoAck = true
        channel.basicConsume(queueName,autoAck,consumerTag, deliverCallback, cancelCallback)
    }

    /**
     * This function formats a message and stores it to the database
     */
    private fun processMessage(message :String){

        val map = listToMap(splitMessageIntoElements(message))

        //TODO add functionality
        //for(e in map){println("Key: ${e.key} Value: ${e.value}")}
        //MessageProcessing.writeMessageToDatabaseIfIntentIsCreated(map)
        messageProcessing.writeMessageToDatabaseIfValuetypeIsDeployment(map)
    }

    /**
     * Splits a message produced by the zeebe workflow Engine by parsing it and splitting it using ',' as Delimiter
     * The message is not split when the ',' occurs inside a substring surrounded by curly brackets
     * @param message the message
     * @return a list of the elements
     */
    private fun splitMessageIntoElements(message: String) :ArrayList<String>{

        val listOfElements = ArrayList<String>()
        val actualElement = StringBuilder("")

        val trimmedMessage = message.removeSurrounding("{", "}")
        var inBrackets = false


        for (c in trimmedMessage.toCharArray()) {
            when (c) {
                '{' -> inBrackets = true
                '}' -> inBrackets = false
                ',' -> if (!inBrackets) {
                    //each element that is added looks like "<key>:<value>" (example: "ID:10")
                    listOfElements.add(actualElement.toString())
                    actualElement.setLength(0)
                    continue //continue so the Diameter is not added to the Element
                }
            }
            actualElement.append(c)
        }

        return listOfElements

    }

    /**
     * Converts a list of elements into a hashMap using the identifier of the element as key and the value of the element as value
     * @param list a list of elements
     * @return a hashMap with one entry for each element
     */
    private fun listToMap(list: ArrayList<String>) :HashMap<String,String>{

        val map = HashMap<String, String>()
        for (element in list) {
            val keyValuePair = element.split(":", limit = 2)
            map[keyValuePair[0].removeSurrounding("\"")] = keyValuePair[1].removeSurrounding("\"")
        }
        return map
    }



}

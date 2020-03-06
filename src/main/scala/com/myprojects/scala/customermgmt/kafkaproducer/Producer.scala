package com.myprojects.scala.customermgmt.kafkaproducer

object Producer  {
	import scala.io.Source
	import java.util.Properties
	import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

	def main(args: Array[String]): Unit = {
			var customerDetails = loadFile("/Users/sobinpaul/Myproject/Docs/customer.json")
			writeToKafkaTopic(customerDetails) 
	}

	def writeToKafkaTopic(customerDetails: String) = {
			val props = new Properties()
			val topic = "customer_topic";
			props.put("bootstrap.servers", "localhost:9092")
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
			val producer = new KafkaProducer[String, String](props)
			val record = new ProducerRecord[String, String](topic, "key", customerDetails)
			producer.send(record)
			producer.close()
	}                                
	
	def loadFile(fileName: String) = {
			Source.fromFile(fileName).getLines.mkString
	}                        



}
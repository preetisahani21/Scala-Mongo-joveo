package MongoScalaConnect

import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }



object PreetiMongoScala extends App {


  object Client {
    def apply(name: String, jobGroups: List[String]): Client = Client(new ObjectId(), name, jobGroups);
  }
  case class Client(_id: ObjectId, name: String, jobGroups: List[String])

  val codecRegistry = fromRegistries(fromProviders(classOf[Client]), DEFAULT_CODEC_REGISTRY)

  val mongoClient: MongoClient = if (args.isEmpty) MongoClient() else MongoClient(args.head)
  val database: MongoDatabase = mongoClient.getDatabase("local").withCodecRegistry(codecRegistry)
  val collection: MongoCollection[Client] = database.getCollection("client")
  collection.drop().results()


  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }


  // INSERTION
  val jobGroup1: List[String] = List("GroupA", "GroupB", "GroupC")
  val jobGroup2: List[String] = List("GroupA", "GroupB")
  val jobGroup3: List[String] = List("GroupA", "GroupB", "GroupC", "GroupD")
  val jobGroup4: List[String] = List("GroupA", "GroupB", "GroupF")
  val jobGroup5: List[String] = List("GroupA", "GroupB", "GroupC","GroupD", "GroupE")
  val jobGroup6: List[String] = List("GroupA")
  val jobGroup7: List[String] = List("GroupA", "GroupB" , "GroupK")


  val client: Client = Client("Preeti", jobGroup1)

  collection.insertOne(client).results()

  collection.find.first().printResults()

  val clients: Seq[Client] = Seq(
    Client("Shreya", jobGroup2),
    Client("Maya", jobGroup3),
    Client("Misha", jobGroup4),
    Client("Shreya", jobGroup5),
    Client("Maya", jobGroup6),
    Client("Dia", jobGroup7)

  )
  collection.insertMany(clients).printResults()


  collection.find().first().printHeadResult()


  collection.find(equal("name", "Preeti")).first().printHeadResult()

  collection.updateOne(equal("name", "Dia"), set("name", "Sansa")).printHeadResult("Update Result: ")


  collection.find(regex("name", "^M")).sort(ascending("lastName")).printResults()

  collection.deleteOne(equal("name", "Shreya")).printHeadResult("Delete Result: ")

  mongoClient.close()


}
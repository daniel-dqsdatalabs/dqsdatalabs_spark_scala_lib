import org.joda.time._
import org.joda.time.format._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class Test { 
  
  lazy val t0 = DateTimeFormat.forPattern("yyyy/MM/dd/HH").parseDateTime("2019/01/01/00")                                                             
  lazy val t1 = DateTimeFormat.forPattern("yyyy/MM/dd/HH").parseDateTime("2020/12/31/00")
  lazy val partitionList = new DateRange(t0, t1, org.joda.time.Period.hours(1), true).toList

  def upsert_data(partition: DateTime): Unit = {
    spark
      .read
      .format("json")
      .load(s"$landing_path" + basePath + partition)
      .repartition(partitionCount)
      .transform(
          Function.chain(
            Seq(
             generatePartitionColumns(partition)
           )
         )
      )
      .write
      .format("parquet")
      .mode("overwrite")
      .save(s"$staging_path" + basePath + partition)
      println("writing partition to " + s"$staging_path" + basePath + partition + " ... [OK]")
  }

  def startIngestion(): Unit = {
      partitionList.par.map {
        case p: DateTime => {
          Try(
            Await.result(
              parallelExecution(
                Seq[Unit]( 
                  upsert_data(p)
                ), 10
              ), 10.seconds
            )
          )
          match {
            case Success(s) => println(s)
            case Failure(e) => println(e)
          }
        }
      }      
    }
  }
}

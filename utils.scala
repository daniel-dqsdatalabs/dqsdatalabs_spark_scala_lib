import spark.implicits._
import java.time.LocalDate
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.Column
import org.joda.time.{Period, DateTime}
import org.joda.time.format._
import org.apache.commons.lang.time.DurationFormatUtils

object dqs {
  object lib {
    object utils {
      object Joda {
        implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
      }

      import Joda._
      class DateRange(val start: DateTime, val end: DateTime, val step: Period, inclusive: Boolean) extends Iterable[DateTime] {
        override def iterator: Iterator[DateTime] = new DateRangeIterator

        class DateRangeIterator extends Iterator[DateTime] {
          var current = start

          override def hasNext: Boolean = current.isBefore(end) || (inclusive && current == end)

          override def next(): DateTime = {
              val returnVal = current
              current = current.withPeriodAdded(step, 1)
              returnVal
          }
        }
        
        //Standard Config
        lazy val adls_name = dbutils.secrets.get(scope = "azure-scope", key = "adls-name")
        lazy val adls_token = dbutils.secrets.get(scope = "azure-scopee", key = "adls-token")
        spark.conf.set(s"fs.azure.account.key.$adls_name.dfs.core.windows.net", adls_token)
        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$adls_name.dfs.core.windows.net", adls_token)

        spark.conf.set("spark.sql.adaptive.enabled","true")
        spark.conf.set("spark.shuffle.statistics.verbose","true")
        spark.conf.set("spark.sql.adaptive.skewedJoin.enabled","true")
        spark.conf.set("spark.sql.parquet.binaryAsString","true")
        spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact","true")
        spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite","true")

        lazy val staging_path = s"abfss://staging@$adls_name.dfs.core.windows.net"
        lazy val landing_path = s"abfss://landing@$adls_name.dfs.core.windows.net"
        lazy val curated_path = s"abfss://curated@$adls_name.dfs.core.windows.net"
        
        import scala.concurrent.{Future, Await}
        import scala.util.{Failure, Success, Try}
        import scala.concurrent.duration._
        import java.util.concurrent.Executors
        import scala.concurrent.ExecutionContext
        import com.databricks.backend.common.rpc.CommandContext

        def executeFunction(function: Unit, ctx: Object)(implicit ec: ExecutionContext): Future[Try[Unit]] = Future {
          val setContext = dbutils.notebook.getClass.getMethods.find(_.getName == "setContext").get
          setContext.invoke(dbutils.notebook, ctx)

          Success(function)
        }

        def parallelExecution(functions: Seq[Unit], numInParallel: Int = 2): Future[Seq[Try[Unit]]] = {
          val ctx = dbutils.notebook.getContext()
          implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numInParallel))

          Future.sequence(
            functions.map {fn => 
               executeFunction(fn, ctx)
             }
           )
        }
        
        def showElapsedTime[T](block: => T): T = {
          val start = System.currentTimeMillis
          val result = block
          val timeInMS = System.currentTimeMillis - start
          val totalTime = DurationFormatUtils.formatDuration(timeInMS, "HH:mm:ss,SSS");
          println("Elapsed time: " + totalTime)
          result
        }


        def getNumberOfPartitions(): Int = {
          java.lang.Runtime.getRuntime.availableProcessors * (sc.statusTracker.getExecutorInfos.length -1) * 4
        }


        type Transform = DataFrame => DataFrame

        /**
        * @author ：DQS
        * @date ：Created in 
        * @description：Replaces "none" and "empty" string values to N/A
        * @modified By: -
        * @version: 1.0.0
        */
        def replaceEmptyValues(cols: Seq[String]): Transform = {
          df => df.na.replace(cols, Map("None" -> "N/A", "" -> "N/A")).na.fill("N/A")
        }

        /**
        * @author ：DQS
        * @date ：Created in 
        * @description：Generates a hash code for every row
        * @modified By: -
        * @version: 1.0.0
        */
        def generateSurrogateKey(skName: String, cols: Column*): Transform = {
          df => df.withColumn(
            skName, 
            sha2(
                upper(
                    concat_ws("|", cols: _*)
                ),256
            )
          )
        }

        /**
        * @author ：DQS
        * @date ：Created in 
        * @description：Generates columns timeKey, dateKey, partitionDate
        * @modified By: -
        * @version: 1.0.0
        */
        def generatePartitionColumns(pDate: String): Transform = {
          df => df
              .withColumn("ts_", current_timestamp())
              .withColumn("DateKey", regexp_replace(lit(pDate), "/", "").substr(0, 8).cast("int"))
              .withColumn("TimeKey", regexp_replace(lit(pDate), "/", "").substr(-4, 4).cast("int"))
              .withColumn("PartitionDate", lit(pDate))
              .distinct()
        }

        /**
        * @author ：DQS
        * @date ：Created in 
        * @description：Generates columns timeKey, dateKey, partitionDate
        * @modified By: -
        * @version: 1.0.0
        */
        def renameColumnsInBatch(columns: Map[String, String]): Transform = {
          df => 
            columns.isEmpty match {
                case true => df
                case false => df.select(columns.map(x => col(x._1).alias(x._2)).toList : _*)
            }
        }


       /**
        * @author ：DQS
        * @date ：Created in 
        * @description：Generates columns timeKey, dateKey, partitionDate
        * @modified By: -
        * @version: 1.0.0
        */
        lazy val sqlCharsToEscape = "()/-.'|+".map { c: Char => "\\" + c }.toList
        def escapeForSqlRegexp(str: String, charsToEscape: List[String] = sqlCharsToEscape): Option[String] = {
          val s = Option(str).getOrElse(return None)

          Some(charsToEscape.foldLeft(str) {
            case (res, pattern) =>
              res.replaceAll(pattern, "\\" + pattern)
          })
        }


      /**
      * @author ：DQS
      * @date ：Created in
      * @description：returns the last N partitions (paths)
      * @modified By: -
      * @version: 1.0.0
      */
      import scala.util.matching.Regex
      val numPattern = new Regex("(\\d{4}/\\d{2}/\\d{2}/\\d{2}/\\d{2}/)")
        def getPartitions(path: String, partitionCount: Int, level: Int): List[String] = {
          dbutils.fs.ls(path).map(file => {
            val path_ = file.path.replace("%25", "%")
            if (file.isDir && level < 5) getPartitions(path_, partitionCount, level + 1)
            else List[String] (
              if (level == 5)
                (numPattern.findFirstIn(file.path.stripMargin('/'))).mkString("")
              else ""
            ).filter(_.nonEmpty).distinct
          }).reduce(_ ++ _).takeRight(partitionCount)
        }
      }    
    }
  } 
}

import com.typesafe.config.Config
import com.usecase.AppUseCase.{getConfig, readParquet, writeParquet}
import com.usecase.sql.Transformation
import com.usecase.utils.Constant.PATH
import com.usecase.utils.LoadConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util
import scala.reflect.io.Directory

class TestApp extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark course")
    .getOrCreate()

  val conf:Config = LoadConf.getConfig

  //Read the dataframes files
  val dfCoaches: DataFrame = spark.read.parquet(conf.getString("input.pathCoaches"))
  val dfMedals: DataFrame = spark.read.parquet(conf.getString("input.pathMedals"))
  val dfAthletes: DataFrame = spark.read.parquet(conf.getString("input.pathAthletes"))

  /**
   * Test de createTop
   */
  test("test createTop works properly") {

    //val dfWithCol = Operation.addColumnRowNumber(df)
    val dfTop = Transformation.createTop(dfCoaches)

    dfTop.show()
    val isNotEmpty = !dfTop.isEmpty
    val total = dfTop.agg(sum("records")).first.get(0)
    println("total:" + total)
    val total_df = dfCoaches.count()
    println("total_df: " + total_df)


    print(s">>>>>> ${isNotEmpty}")

    assert(isNotEmpty)
    assert(total == total_df)
  }
  test("test createTop works properly v2") {

    val dfTop = Transformation.createTop(dfAthletes)
    dfTop.show()


    val rowFive = dfTop.take(5).last
    println("total:" + rowFive)
    val noc = rowFive.getString(0)
    val records =  rowFive.get(1)
    val total =  rowFive.get(2)
    val rank =  rowFive.get(3)

    assert(noc == "Germany", "NOC isn't correct")
    assert(records == 400, "NOC isn't correct")
    assert(total == 11085, "NOC isn't correct")
    assert(rank == 5, "NOC isn't correct")
  }

  /**
   * Test de getTableFiltered
   */
  test("test getTableFiltered works properly") {

    //val dfWithCol = Operation.addColumnRowNumber(df)
    val dfFiltered = Transformation.getTableFiltered(dfCoaches)

    dfFiltered.show()
    val grouped = dfFiltered.groupBy("noc").count()
    grouped.count()
    //val count = dfFiltered.groupBy("noc").count() == 1
    val isNotEmpty = !dfFiltered.isEmpty
    println(s">>>>>> ${isNotEmpty}")
    println(grouped.select(col("noc")).first.getString(0))

    assert(isNotEmpty)
    assert(grouped.count() == 1)
    assert(grouped.select(col("noc")).first.getString(0) == "Japan")
  }
  test("test getTableFiltered works properly v2") {

    val dfTopAthletes = readParquet("input.pathTopAthletes")
    val dfFiltered = Transformation.getTableFiltered(dfTopAthletes)
    dfFiltered.show()

    val rows = dfFiltered.count()
    val row = dfFiltered.first()

    println("row:" + row)
    val noc = row.getString(0)
    val records = row.get(1)
    val total = row.get(2)
    val rank = row.get(3)

    assert(rows == 1, "Failed total rows")
    assert(noc == "Japan", "NOC isn't correct")
    assert(records == 586, "NOC isn't correct")
    assert(total == 11085, "NOC isn't correct")
    assert(rank == 2, "NOC isn't correct")


  }

  /**
   * Test de createPercent
   */
  test("test createPercent works properly"){
    val dfTop = Transformation.createTop(dfCoaches)
    val dfPercent = Transformation.createPercent(dfTop)
    dfPercent.show(false)

    val isNotEmpty = !dfPercent.isEmpty
    println(s">>>>>> ${isNotEmpty}")

    val percent_record  = dfPercent.select("percent_records").first().getDouble(0)
    println("percent_record: " + percent_record)
    val records = dfPercent.select("records").first().getLong(0)
    println("records " + records)
    val total_records = dfPercent.select("total_records").first().getLong(0)
    println("total_records " + total_records)
    val percent = (records*100.0)/total_records
    println("percent " + percent)


    assert(isNotEmpty)
    assert(percent_record == percent)
  }
  test("test createPercent works properly v2"){
    val dfFiltered = readParquet("input.pathAthletesFiltered")
    val dfPercent = Transformation.createPercent(dfFiltered)
    dfPercent.show(false)

    val originaCount = dfFiltered.columns.length
    val percentCount = dfPercent.columns.length

    val percent = dfPercent.first().get(4)
    println("percent: " + percent)

    assert(percentCount == originaCount + 1, "Columns length is not correct")
    assert(percent == 5.286423094271538, "Wrong percent")
  }

  /**
   * Test de createMessage
   */
  test("test createMessage works properly"){
    val dfMessage = Transformation.createMessage(dfMedals)
    dfMessage.show(false)

    val isNotEmpty = !dfMessage.isEmpty
    println(s">>>>>> ${isNotEmpty}")

    val rank = dfMessage.select("rank_by_total").first().get(0)
    println("rank: " + rank)
    val medals = dfMessage.select("total").first().get(0)
    println("medals: " + medals)
    val message = dfMessage.select("message").first().getString(0)
    println("message: " + message)

    assert(isNotEmpty)
    assert(message.contains("place " + rank))
    assert(message.contains("with " + medals))
  }

  /**
   * Test de joinObjects
   */
  test("test joinObjects works properly"){
//    val dfCoachesTop = Transformation.createTop(dfCoaches)
//    val dfAthletesTop = Transformation.createTop(dfAthletes)
    val athletesResume = readParquet("input.pathAthletesResume")// */Transformation.createPercent(dfAthletesTop)
    val coachesResume = readParquet("input.pathCoachesResume") // Transformation.createPercent(dfCoachesTop)

    val dfJoins = Transformation.joinObjects(dfMedals, athletesResume, coachesResume)
    dfJoins.show(false)

    val isNotEmpty = !dfJoins.isEmpty
    println(s">>>>>> ${isNotEmpty}")



    assert(isNotEmpty)
//    writeParquet(dfJoins, PATH+"Olympics", "noc")
    assert(dfJoins.columns.length == 10)
  }
  test("test joinObjects works properly v2"){

    val athletesResume = readParquet("input.pathAthletesResume")
    val coachesResume = readParquet("input.pathCoachesResume")
    val medalsFiltered = readParquet("input.pathMedalsFiltered")

    val dfJoins = Transformation.joinObjects(medalsFiltered, athletesResume, coachesResume)
    dfJoins.show(false)

    val noc = dfJoins.select("noc").first().get(0)
    val medals_number = dfJoins.select("medals_number").first().get(0)
    val rank_medals_number = dfJoins.select("rank_medals_number").first().get(0)
    val athletes_number = dfJoins.select("athletes_number").first().get(0)
    val athletes_percent_number = dfJoins.select("athletes_percent_number").first().get(0)
    val rank_athletes_number = dfJoins.select("rank_athletes_number").first().get(0)
    val coaches_number = dfJoins.select("coaches_number").first().get(0)
    val coaches_percent_number = dfJoins.select("coaches_percent_number").first().get(0)
    val rank_coaches_number = dfJoins.select("rank_coaches_number").first().get(0)
    val message = dfJoins.select("message").first().get(0)

    assert(athletes_number == 586)
    assert(athletes_percent_number == 5.286423094271538)
    assert(coaches_number == 35)
    assert(coaches_percent_number == 8.883248730964468)
    assert(medals_number == 58)
    assert(message == "Japan in place 5 with 58 won medals!")
    assert(noc == "Japan")
    assert(rank_athletes_number == 2)
    assert(rank_coaches_number == 1)
    assert(rank_medals_number == 5)

    assert(dfJoins.columns.length == 10, "wrong columns length")

    val dir = new Directory(new File("/" + getConfig("output.pathTest")))
    dir.deleteRecursively()

    writeParquet(dfJoins, "output.pathTest", "noc")

    val dfSaved = readParquet("output.pathTestJapan")
    dfSaved.show(false)

//    val nocS = dfSaved.select("noc").first().get(0)
    val medals_numberS = dfSaved.select("medals_number").first().get(0)
    val rank_medals_numberS = dfSaved.select("rank_medals_number").first().get(0)
    val athletes_numberS = dfSaved.select("athletes_number").first().get(0)
    val athletes_percent_numberS = dfSaved.select("athletes_percent_number").first().get(0)
    val rank_athletes_numberS = dfSaved.select("rank_athletes_number").first().get(0)
    val coaches_numberS = dfSaved.select("coaches_number").first().get(0)
    val coaches_percent_numberS = dfSaved.select("coaches_percent_number").first().get(0)
    val rank_coaches_numberS = dfSaved.select("rank_coaches_number").first().get(0)
    val messageS = dfSaved.select("message").first().get(0)

    assert(athletes_numberS == 586)
    assert(athletes_percent_numberS == 5.286423094271538)
    assert(coaches_numberS == 35)
    assert(coaches_percent_numberS == 8.883248730964468)
    assert(medals_numberS == 58)
    assert(messageS == "Japan in place 5 with 58 won medals!")
//    assert(nocS == "Japan")
    assert(rank_athletes_numberS == 2)
    assert(rank_coaches_numberS == 1)
    assert(rank_medals_numberS == 5)
  }

  test("open Mexico"){
    val dfOlympics = readParquet("input.pathOlympicsMexico")
    dfOlympics.show(false)
  }

  test("Delete directory"){
    val dir = new Directory(new File("/" + getConfig("output.pathTest") + "/"))
    println(dir)
    assert(dir.isDirectory)
    println(getConfig("output.pathTest"))
    dir.deleteRecursively()
    assert(getConfig("output.pathTest") == "src/test/resources/data/output/test")

//    //Create Hadoop Configuration from Spark
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//
//    val srcPath = new Path(fs.getWorkingDirectory + "/" + getConfig("output.pathTest"))
//
//    //To Delete File
////    if (fs.exists(srcPath) && fs.isFile(srcPath))
////      fs.delete(srcPath, true)
//
//    println(fs.get)
//
//    //To Delete Directory
////    if (fs.exists(srcPath) && fs.isDirectory(srcPath))
////      fs.delete(srcPath, true)
////
//    assert(fs.exists(srcPath), "no existe")
////    assert(fs.isDirectory(srcPath), "No es un directorio")
  }
}


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

val spark = SparkSession.builder()
  .appName("Car Crash Analysis")
  .master("local")
  .getOrCreate()

val chargesDF = spark.read.option("header", "true")
  .csv("/Users/rcvinaynag/Documents/Vinay/trainings/BCGCaseStudy/src/main/resources/Data/Charges_use.csv")
val unitDF = spark.read.option("header","true")
  .csv("/Users/rcvinaynag/Documents/Vinay/trainings/BCGCaseStudy/src/main/resources/Data/Units_use.csv")
  .distinct()
val personDF = spark.read.option("header", "true")
  .csv("/Users/rcvinaynag/Documents/Vinay/trainings/BCGCaseStudy/src/main/resources/Data/Primary_Person_use.csv")
  .distinct()

val top25StatesWithHighestOffensesDF =
  unitDF.join(chargesDF, Seq("crash_id", "unit_nbr"), "inner")
    .groupBy("VEH_LIC_STATE_ID")
    .agg(count("charge").as("Offense_Count"))
    .orderBy(col("Offense_Count").desc)
    .select("VEH_LIC_STATE_ID")
    .limit(25)

val top10ColorsDF = unitDF
  .filter(col("VEH_COLOR_ID")=!="NA")
  .groupBy("VEH_COLOR_ID")
  .count
  .orderBy(col("count").desc)
  .select("VEH_COLOR_ID")
  .limit(10)

val licensedPersonDF = personDF.filter(not(col("DRVR_LIC_TYPE_ID")
  .isin("NA","UNKNOWN","UNLICENSED")))
  .select("crash_id","unit_nbr","PRSN_NBR")
  .distinct()

val filteredChargesDF = chargesDF
  .filter(upper(chargesDF("CHARGE")).like("%SPEED%"))

val mergedDF = unitDF
  .select("crash_id",
    "unit_nbr",
    "PRSN_NBR",
    "VEH_COLOR_ID",
    "VEH_LIC_STATE_ID",
    "VEH_MAKE_ID"
  )
  .join(filteredChargesDF, Seq("crash_id", "unit_nbr"),
  "inner")
  .join(licensedPersonDF, Seq("crash_id","unit_nbr","PRSN_NBR"),
  "inner")
  .join(top10ColorsDF, Seq("VEH_COLOR_ID"),
  "inner")
  .join(top25StatesWithHighestOffensesDF, Seq("VEH_LIC_STATE_ID"),
  "inner")

mergedDF.groupBy("VEH_MAKE_ID")
  .count.orderBy(col("count").desc)
  .show(10)


package com.bcg.jobs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, countDistinct, first, lower, not, row_number, substring, sum, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The carCrashAnalysisJobs object:
 * Acts as the container for all the methods that need
 * to be implemented as part of the analytics required.
 * Extends the carCrashAnalysisMethods Trait and contains
 * implementations for all the methods.
 */
object carCrashAnalysisJobs extends carCrashAnalysisMethods {

  /**
   * Initiate Spark Session:
   * Entry point of the spark program to use
   * the DataFrame API. Takes no parameters.
   * @return
   */
  override def initiateSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Car Crash Analysis")
      .master("local")
      .getOrCreate()
    spark
  }

  /**
   * crashesWithMaleFatalities :
   * This is a function that can be used to
   * find the number of crashes (accidents) in which
   * number of persons killed are male.
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   *
   * @return
   */
  override def crashesWithMaleFatalities(primaryPersonDF: DataFrame): Long =
    primaryPersonDF
      .filter(col("PRSN_GNDR_ID") === "MALE" &&
        col("PRSN_INJRY_SEV_ID") === "KILLED")
      .select("CRASH_ID").distinct()
      .count()

  /**
   * twoWheelerCrashes :
   * This is a function that can be used to
   * find number of two wheelers booked for crashes.
   *
   * @param unitsDF
   * Dataframe containing info about units involved
   * @param chargesDF
   * Dataframe containing info about charges
   *
   * @return
   */
  override def twoWheelerCrashes(unitsDF: DataFrame, chargesDF: DataFrame): Long = {
    val chargeDistinctDF = chargesDF.select("CRASH_ID","UNIT_NBR")
      .distinct()
    unitsDF.join(chargeDistinctDF, Seq("CRASH_ID","UNIT_NBR"), "inner")
      .filter(upper(col("VEH_BODY_STYL_ID")) === "MOTORCYCLE" ||
      upper(col("VEH_BODY_STYL_ID")) === "POLICE MOTORCYCLE")
      .count()
  }

  /**
   * This is a function that can be used to check
   * which state has highest number of accidents
   * in which females are involved.
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   *
   * @return
   */
  override def stateWithHighestFemaleAccidents(primaryPersonDF: DataFrame): String =
    primaryPersonDF.filter(upper(col("PRSN_GNDR_ID")) === "FEMALE")
      .groupBy("DRVR_LIC_STATE_ID")
      .agg(countDistinct("crash_id")
        .as("numberOfCrashes"))
      .orderBy(col("numberOfCrashes").desc)
      .select("DRVR_LIC_STATE_ID")
      .head.getString(0)

  /**
   * This is a function that can be used to get
   * Top 5th to 15th VEH_MAKE_IDs that contribute to the
   * largest number of injuries including death.
   *
   * @param unitsDF
   * Dataframe containing info about units involved
   *
   * @return
   */
  override def top5thTo15thVehicleMakeIDs(unitsDF: DataFrame): DataFrame = {
    val windowFunc = Window.orderBy(col("NO_OF_CASUALTIES").desc)
    val resultDF = unitsDF.groupBy("VEH_MAKE_ID")
      .agg(sum(col("TOT_INJRY_CNT") + col("DEATH_CNT"))
        .as("NO_OF_CASUALTIES"))
      .withColumn("RW_NUM", row_number() over windowFunc)
      .filter(col("RW_NUM") between(5, 15))
      .drop("RW_NUM")

    resultDF
  }

  /**
   * This is a function that can be used to get
   * the top ethnic user group of each unique body style
   * for all the body styles involved in crashes.
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   *
   * @param unitsDF
   * Dataframe containing info about units involved
   *
   * @return
   */
  override def topEthnicGroupOfBodyStyle(primaryPersonDF: DataFrame,
                                        unitsDF: DataFrame): DataFrame = {
    val personDF = primaryPersonDF.select("crash_id","unit_nbr",
      "PRSN_ETHNICITY_ID")
    val unitDF = unitsDF.select("crash_id","unit_nbr",
      "VEH_BODY_STYL_ID").distinct()
    val mergeDF = unitDF.join(personDF, Seq("crash_id","unit_nbr"), "inner")
    val bodyStyleEthnicityGroupedDF = mergeDF.groupBy("VEH_BODY_STYL_ID",
      "PRSN_ETHNICITY_ID").count
    val windowFunc = Window.partitionBy("VEH_BODY_STYL_ID")
      .orderBy(col("count").desc)

    val resultDF = bodyStyleEthnicityGroupedDF.withColumn("TOP_ETHNIC_GROUP",
      first("PRSN_ETHNICITY_ID") over windowFunc)
      .select("VEH_BODY_STYL_ID","TOP_ETHNIC_GROUP").distinct()
    resultDF
  }

  /**
   * This is a function that can be used to get
   * the Top 5 Zip Codes with highest number
   * crashes with alcohols as the contributing factor to a crash,
   * among car crashes
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   *
   * @param unitsDF
   * Dataframe containing info about units involved
   *
   * @return
   */
  override def topZipsWithHighestCrashesDueToAlcohol(primaryPersonDF: DataFrame,
                                                    unitsDF: DataFrame): DataFrame = {
    val personDF = primaryPersonDF.filter(col("DRVR_ZIP") =!= "null" &&
      col("DRVR_ZIP").isNotNull &&
      upper(col("PRSN_ALC_RSLT_ID"))==="POSITIVE")
    val carUnitDF = unitsDF
      .filter(upper(col("VEH_BODY_STYL_ID")) =!= "MOTORCYCLE" &&
        upper(col("VEH_BODY_STYL_ID")) =!= "POLICE MOTORCYCLE")
      .select("crash_id","unit_nbr")

    personDF
      .join(carUnitDF, Seq("crash_id","unit_nbr"), "inner")
      .groupBy("DRVR_ZIP")
      .agg(countDistinct("crash_id").as("num_records"))
      .orderBy(col("num_records").desc)
      .limit(5)
  }

  /**
   * This is a function that can be used to get
   * Count of Distinct Crash IDs where No Damaged Property
   * was observed and Damage Level is above 4 and car
   * avails Insurance.
   *
   * @param unitDF
   * Dataframe containing info about units involved
   *
   * @param damagesDF
   * Dataframe containing info about damages
   *
   * @return
   */
  override def countOfCrashesWithNoDamagedProperty(unitDF: DataFrame,
                                                   damagesDF: DataFrame): Long = {
    val cleansedDamageDF = damagesDF.filter(
      col("DAMAGED_PROPERTY").isNotNull ||
      lower(col("DAMAGED_PROPERTY"))=!="none")
      .select("crash_id").distinct()
    val unitFilteredDF = unitDF.filter(
      col("VEH_DMAG_SCL_1_ID").startsWith("DAMAGED") &&
        col("VEH_DMAG_SCL_2_ID").startsWith("DAMAGED") &&
        substring(col("VEH_DMAG_SCL_1_ID"), 9, 1) > 4 &&
        substring(col("VEH_DMAG_SCL_2_ID"), 9, 1) > 4 &&
        upper(col("FIN_RESP_TYPE_ID")).like("%INSURANCE%"))
      .select("crash_id").distinct()

    unitFilteredDF.join(cleansedDamageDF, Seq("crash_id"), "leftanti")
      .distinct().count()

  }

  /**
   * This is a function that can be used to
   * determine the Top 5 Vehicle Makes where drivers are charged with
   * speeding related offences, has licensed Drivers, uses top 10 used
   * vehicle colours and has car licensed with the Top 25 states
   * with highest number of offences
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   * @param unitsDF
   * Dataframe containing info about units involved
   * @param chargesDF
   * Dataframe containing info about charges
   * @return
   */
  override def top5VehicleMakes(primaryPersonDF: DataFrame,
                                unitsDF: DataFrame,
                                chargesDF: DataFrame): DataFrame = {
    val top25StatesWithHighestOffensesDF =
      unitsDF.join(chargesDF, Seq("crash_id", "unit_nbr"), "inner")
        .groupBy("VEH_LIC_STATE_ID")
        .agg(count("charge").as("Offense_Count"))
        .orderBy(col("Offense_Count").desc)
        .select("VEH_LIC_STATE_ID")
        .limit(25)

    val top10ColorsDF = unitsDF
      .filter(col("VEH_COLOR_ID")=!="NA")
      .groupBy("VEH_COLOR_ID")
      .count
      .orderBy(col("count").desc)
      .select("VEH_COLOR_ID")
      .limit(10)

    val licensedPersonDF = primaryPersonDF
      .filter(not(col("DRVR_LIC_TYPE_ID")
      .isin("NA","UNKNOWN","UNLICENSED")))
      .select("crash_id","unit_nbr","PRSN_NBR")
      .distinct()

    val filteredChargesDF = chargesDF
      .filter(upper(chargesDF("CHARGE")).like("%SPEED%"))

    val mergedDF = unitsDF
      .select("crash_id",
        "unit_nbr",
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
      .limit(5)
  }
}

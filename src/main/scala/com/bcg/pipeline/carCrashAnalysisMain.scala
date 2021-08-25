/****************************************
 * Car Crash Analysis - BCG Case Study
 * Author : Vinay Nag R C
 * Description : Part of recruitment procedures
 * Date last modified : 25-08-2021
 * Last modified by : Vinay Nag R C
 *****************************************/

package com.bcg.pipeline

import com.bcg.jobs.carCrashAnalysisJobs
import com.bcg.utilities.constants._
import com.bcg.utilities.schemaHandler.{chargesDFSchema, damagesDFSchema, endorsementsDFSchema, restrictsDFSchema}
import com.bcg.utilities.utilityMethods.{readFileIntoDataframe, saveCSV}
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.{Level, Logger}

/**
 * The main object - carCrashAnalysisMain.
 * Acts as a wrapper class from where other methods and
 * functions are being called from.
 */
object carCrashAnalysisMain {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // INITIATE SPARK SESSION : get it in a passable variable
    val spark = carCrashAnalysisJobs.initiateSparkSession()

    // READ ALL INPUTS :
    val chargesDF =
      readFileIntoDataframe(spark, chargesDFSchema, chargesFilePath)

    val damagesDF =
      readFileIntoDataframe(spark, damagesDFSchema,damagesFilePath)

    val primaryPersonDF =
      readFileIntoDataframe(spark,primaryPersonFilePath)
        .distinct()
    primaryPersonDF.persist(MEMORY_AND_DISK_2)

    val unitsDF =
      readFileIntoDataframe(spark,unitsFilePath)
        .distinct()
    unitsDF.persist(MEMORY_AND_DISK_2)

//    // NOT BEING READ SINCE THEY ARE NOT BEING USED
//    // IN ANY ANALYSIS
//    val endorsementsDF =
//      readFileIntoDataframe(spark, endorsementsDFSchema,endorsementsFilePath)
//
//    val restrictsDF =
//      readFileIntoDataframe(spark, restrictsDFSchema,restrictsFilePath)

    // RUN ANALYTICS ON THE LOADED DATA :
    val answerAnalytics1 = carCrashAnalysisJobs.crashesWithMaleFatalities(primaryPersonDF)
    val answerAnalytics2 = carCrashAnalysisJobs.twoWheelerCrashes(unitsDF, chargesDF)
    val answerAnalytics3 = carCrashAnalysisJobs.stateWithHighestFemaleAccidents(primaryPersonDF)
    val answerAnalytics4 = carCrashAnalysisJobs.top5thTo15thVehicleMakeIDs(unitsDF)
    val answerAnalytics5 = carCrashAnalysisJobs.topEthnicGroupOfBodyStyle(primaryPersonDF,unitsDF)
    val answerAnalytics6 = carCrashAnalysisJobs.topZipsWithHighestCrashesDueToAlcohol(primaryPersonDF,unitsDF)
    val answerAnalytics7 = carCrashAnalysisJobs.countOfCrashesWithNoDamagedProperty(unitsDF, damagesDF)
    val answerAnalytics8 = carCrashAnalysisJobs.top5VehicleMakes(primaryPersonDF, unitsDF, chargesDF)

    println("Analytics 1: The number of crashes (accidents) in which number of persons killed are male = "+
      answerAnalytics1)

    println("Analytics 2: The number of two-wheelers booked for crashes = "+
      answerAnalytics2)

    println("Analytics 3: The state which has highest number of accidents in which females " +
      "are involved = "+
      answerAnalytics3)

    println("Analytics 4: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of " +
      "injuries including death :")
//        answerAnalytics4.show(false)
    saveCSV(answerAnalytics4,"answerAnalytics4")

    println("Analytics 5: The top ethnic user group of each unique body style :")
//        answerAnalytics5.show(false)
    saveCSV(answerAnalytics5,"answerAnalytics5")

    println("Analytics 6: Top 5 Zip Codes with highest number crashes with alcohols as the " +
      "contributing factor to a crash :")
//    answerAnalytics6.show(false)
    saveCSV(answerAnalytics6,"answerAnalytics6")

    println("Analytics 7: Count of Distinct Crash IDs where No Damaged Property was observed, " +
      "Damage Level is above 4 and car avails Insurance = " +
      answerAnalytics7)

    println("Analytics 8: Top 5 Vehicle Makes where drivers are charged with speeding " +
      "related offences, has licensed Drivers, uses top 10 used vehicle colours and has " +
      "car licensed with the Top 25 states with highest number of offences :")
//    answerAnalytics8.show(false)
    saveCSV(answerAnalytics8,"answerAnalytics8")

  }
}

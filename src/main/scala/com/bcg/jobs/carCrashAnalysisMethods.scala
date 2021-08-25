package com.bcg.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

trait carCrashAnalysisMethods {

  /**
   * Initiate Spark Session:
   * Entry point of the spark program to use
   * the DataFrame API. Takes no parameters.
   * @return
   */
  def initiateSparkSession(): SparkSession

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
  def crashesWithMaleFatalities(primaryPersonDF: DataFrame): Long

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
  def twoWheelerCrashes(unitsDF: DataFrame, chargesDF: DataFrame): Long

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
  def stateWithHighestFemaleAccidents(primaryPersonDF: DataFrame): String

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
  def top5thTo15thVehicleMakeIDs(unitsDF: DataFrame): DataFrame

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
  def topEthnicGroupOfBodyStyle(primaryPersonDF: DataFrame, unitsDF: DataFrame): DataFrame

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
  def topZipsWithHighestCrashesDueToAlcohol(primaryPersonDF: DataFrame,unitsDF: DataFrame): DataFrame

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
  def countOfCrashesWithNoDamagedProperty(unitDF: DataFrame, damagesDF: DataFrame): Long

  /**
   * This is a function that can be used to
   * determine the Top 5 Vehicle Makes where drivers are charged with
   * speeding related offences, has licensed Drivers, uses top 10 used
   * vehicle colours and has car licensed with the Top 25 states
   * with highest number of offences
   *
   * @param primaryPersonDF
   * The dataframe containing primary person info
   *
   * @param unitsDF
   * Dataframe containing info about units involved
   *
   * @param chargesDF
   * Dataframe containing info about charges
   *
   * @return
   */
  def top5VehicleMakes(primaryPersonDF: DataFrame, unitsDF: DataFrame, chargesDF: DataFrame): DataFrame
}

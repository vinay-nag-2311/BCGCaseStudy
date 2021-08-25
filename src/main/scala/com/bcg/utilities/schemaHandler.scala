package com.bcg.utilities

import org.apache.spark.sql.types.{StringType, StructType}

/**
 * The schemaHandler object:
 * Has all the information regarding the schema of input
 * files, column names, data-types etc.
 */
object schemaHandler {

  val chargesDFSchema: StructType = new StructType()
    .add("CRASH_ID", StringType)
    .add("UNIT_NBR", StringType)
    .add("PRSN_NBR", StringType)
    .add("CHARGE", StringType)
    .add("CITATION_NBR", StringType)

  val endorsementsDFSchema: StructType = new StructType()
    .add("CRASH_ID", StringType)
    .add("UNIT_NBR", StringType)
    .add("DRVR_LIC_ENDORS_ID", StringType)

  val damagesDFSchema: StructType = new StructType()
    .add("CRASH_ID", StringType)
    .add("DAMAGED_PROPERTY", StringType)

  val restrictsDFSchema: StructType = new StructType()
    .add("CRASH_ID", StringType)
    .add("UNIT_NBR", StringType)
    .add("DRVR_LIC_RESTRIC_ID", StringType)

}

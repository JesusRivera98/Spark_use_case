package com.usecase

import com.usecase.sql.Transformation
import com.usecase.utils.AppSession

import com.usecase.utils.Constant.PATH

object AppUseCase extends AppSession {

  def run(): Unit = {

    /**
     * INPUTS
     */
    logger.info("=====> Reading file")

    val dfAthletes = readParquet("input.pathAthletes")
    val dfCoaches = readParquet("input.pathCoaches")
    val dfMedals = readParquet("input.pathMedals")


    /**
     * TRANSFORMATIONS
     */
    logger.info("=====> Transforming data")
    val dfAthletesWithExtraColumn = Transformation.addLiteralColumn(dfAthletes)

    logger.info("=====> Transforming topAthlets")
    val topAthlets = Transformation.createTop(dfAthletes)
    topAthlets.show(false)
    logger.info("=====> Transforming Coaches")
    val topCoaches = Transformation.createTop(dfCoaches)
    topCoaches.show(false)

    /**
     * FILTERS
     */

    logger.info("=====> Filtering dfMedals")
    val medalsFiltered = Transformation.getTableFiltered(dfMedals)
    medalsFiltered.show(false)
    logger.info("=====> Filtering topAthlets")
    val athletesFiltered = Transformation.getTableFiltered(topAthlets)
    athletesFiltered.show(false)
    logger.info("=====> Filtering topCoaches")
    val coachesFiltered = Transformation.getTableFiltered(topCoaches)
    coachesFiltered.show(false)

    /**
     * Operations
     */

    logger.info("=====> Operating with athletesFiltered")
    val athletesResume = Transformation.createPercent(athletesFiltered)
    athletesResume.show(false)
    logger.info("=====> Operating with coachesFiltered")
    val coachesResume = Transformation.createPercent(coachesFiltered)
    coachesResume.show(false)


    /**
     * Messages
     */

    val medalsMessage = Transformation.createMessage(medalsFiltered)
    medalsMessage.show(false)


    /**
     * Joins
     */

    val olimpicResume = Transformation.joinObjects(medalsFiltered, athletesResume, coachesResume)
    olimpicResume.show(false)


    /**
     * OUTPUT
     */

    writeParquet(dfAthletesWithExtraColumn, PATH)

    writeParquet(olimpicResume, PATH+"Olympics", "noc")
//    writeParquetCSV(olimpicResume, PATH+"Olympics", "noc")

    val dfOlympics = readParquet("input.pathOlympicsJapan")
    dfOlympics.show(false)

    spark.stop()
    logger.info("=====> end process")

  }
}

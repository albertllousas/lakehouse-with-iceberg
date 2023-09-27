package iceberg.fetch

import iceberg.UserClick
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class SparkUserClicksFetcher(
        private val spark: SparkSession,
        private val glueDB: String,
        private val table: String,
        private val logger: Logger = LoggerFactory.getLogger(SparkUserClicksFetcher::class.java)
) : UserClicksFetcher {

    @OptIn(ExperimentalTime::class)
    override fun fetch(userId: UUID, from: LocalDate, to: LocalDate, logQueryStats: Boolean): List<UserClick> { //from to extract measuring (benchmark) component outside Measure Elapsed Time $this->benchmark->elapsed_time('code_start', 'code_end');
        val timedValue = measureTimedValue {
            val query = "select * from $glueDB.$table where time > '$from' AND time < '$to' AND user_id = '$userId'"
            val rowDataset: Dataset<Row> = spark.sql(query)
            if (logQueryStats) spark.sql(query).explain("cost")
            rowDataset.collectAsList().map {
                UserClick(
                        event_id = it.getString(it.fieldIndex("event_id")),
                        user_id = it.getString(it.fieldIndex("user_id")),
                        device_id = it.getString(it.fieldIndex("device_token")),
                        element_id = it.getString(it.fieldIndex("element_id")),
                        latitude = it.getString(it.fieldIndex("latitude")),
                        longitude = it.getString(it.fieldIndex("longitude")),
                        country = it.getString(it.fieldIndex("country")),
                        time = it.getTimestamp(it.fieldIndex("time")).toLocalDateTime(),
                        ip_address = it.getString(it.fieldIndex("ip_address")),
                        city = it.getString(it.fieldIndex("city")),
                        os = it.getString(it.fieldIndex("os")),
                        device_model = it.getString(it.fieldIndex("device_model"))
                )
            }
//            rowDataset.show()
        }
        logger.info("Fetch took : ${timedValue.duration}")
        return timedValue.value
    }
}
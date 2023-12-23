package iceberg.store

import iceberg.UserClick
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class SparkUserClicksStorage(
        private val spark: SparkSession,
        private val glueDB: String,
        private val table: String,
        private val logger: Logger = LoggerFactory.getLogger(SparkUserClicksStorage::class.java)
): ClicksStorage {

    init {
        createGlueTableIfDoesntExists()
    }

    private fun createGlueTableIfDoesntExists() {
        spark.sql(
                """
            CREATE TABLE IF NOT EXISTS ${glueDB}.${table} 
                (
                    event_id string,
                    user_id string,
                    device_id string,
                    element_id string,
                    latitude string,
                    longitude string,
                    country string,
                    time timestamp,
                    ip_address string,
                    city string,
                    device_model string,
                    os string
                )
                USING iceberg
                PARTITIONED BY (days(time), bucket(128, element_id))
        """
        )
    }

    override fun store(userClicks: List<UserClick>) {
        val data: Dataset<Row> = spark.createDataFrame(userClicks, UserClick::class.java)
        data.writeTo("$glueDB.$table").append()
    }
}
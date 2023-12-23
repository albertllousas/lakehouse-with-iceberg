package iceberg.fetch

import iceberg.UserClick
import org.apache.iceberg.ScanSummary
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.expressions.Expressions.*
import org.apache.iceberg.io.CloseableIterable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue


class PlainIcebergUserClicksFetcher(
        private val s3Bucket: String,
        private val glueDB: String,
        private val table: String,
        private val logger: Logger = LoggerFactory.getLogger(PlainIcebergUserClicksFetcher::class.java)
) : UserClicksFetcher {

    val catalog = GlueCatalog().apply {
        initialize(
                "glue_catalog", mapOf(
                "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
                "warehouse" to s3Bucket,
                "clients.glue.credentials-provider-class" to "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
        )
        )
    }

    @OptIn(ExperimentalTime::class)
    override fun fetch(elementId: UUID, from: LocalDate, to: LocalDate, logQueryStats: Boolean): List<UserClick> {
        val timedValue = measureTimedValue {
            val tableIdentifier = TableIdentifier.of(glueDB, table)
            val table = catalog.loadTable(tableIdentifier)
            val scanBuilder = IcebergGenerics.read(table)
            val and = and(
                    greaterThan("time", "${from.atTime(LocalTime.MIN)}"),
                    lessThan("time", "${to.atTime(LocalTime.MIN)}"),
                    equal("element_id", elementId.toString())
            )
            val filteredScan: CloseableIterable<Record> = scanBuilder.where(and).build()
            val filter = table.newScan().filter(and)

            val result: List<UserClick> = filteredScan.map { it: Record ->
                UserClick(
                        event_id = it.getField("event_id") as String,
                        user_id = it.getField("user_id") as String,
                        device_id = it.getField("device_id") as String,
                        element_id = it.getField("element_id") as String,
                        latitude = it.getField("latitude") as String,
                        longitude = it.getField("longitude") as String,
                        country = it.getField("country") as String,
                        time = LocalDateTime.now(),//it.getField("time").toLocalDateTime(),
                        ip_address = it.getField("ip_address") as String,
                        city = it.getField("city") as String,
                        os = it.getField("os") as String,
                        device_model = it.getField("device_model") as String
                )
            }
            if (logQueryStats) {
                val stats = ScanSummary.of(filter).build()
                logger.info(stats.toString())
            }
            result
        }
        if (logQueryStats) {
            logger.info("Fetch took : ${timedValue.duration}")
        }
        return timedValue.value
    }
}

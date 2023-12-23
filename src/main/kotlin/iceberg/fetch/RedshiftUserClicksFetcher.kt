package iceberg.fetch

import iceberg.UserClick
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.DescribeStatementRequest
import software.amazon.awssdk.services.redshiftdata.model.ExecuteStatementRequest
import software.amazon.awssdk.services.redshiftdata.model.Field
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultRequest
import software.amazon.awssdk.services.redshiftdata.model.StatusString
import software.amazon.awssdk.services.redshiftdata.model.StatusString.ABORTED
import software.amazon.awssdk.services.redshiftdata.model.StatusString.FAILED
import software.amazon.awssdk.services.redshiftdata.model.StatusString.FINISHED
import software.amazon.awssdk.services.redshiftdata.model.StatusString.UNKNOWN_TO_SDK_VERSION
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

// svl_s3query table in redshift scheme contains data about the queries

class RedshiftUserClicksFetcher(
    val redshiftExternalScheme: String,
    val redshiftDB: String,
    val redshiftWorkgroupName: String,
    val table: String,
    private val getQueryStatusPollingInMs: Long = 100,
    private val logger: Logger = LoggerFactory.getLogger(RedshiftUserClicksFetcher::class.java)
) : UserClicksFetcher {

    private val redshift = RedshiftDataClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).build()

    private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @OptIn(ExperimentalTime::class)
    override fun fetch(elementId: UUID, from: LocalDate, to: LocalDate, logQueryStats: Boolean): List<UserClick> {
        val timedValue = measureTimedValue {
            val query = """SELECT * 
                           FROM $redshiftExternalScheme.$table 
                           WHERE (time between TIMESTAMP '$from 00:00:00' AND TIMESTAMP '$to 00:00:00') 
                                  AND element_id = '$elementId';"""
            val queryExecutionId = submitRedshiftQuery(query)
            waitForQueryToComplete(queryExecutionId)
            processResultRows(queryExecutionId)
        }
        if (logQueryStats) logger.info("Fetch took : ${timedValue.duration}")
        return timedValue.value
    }

    private fun submitRedshiftQuery(query: String): String =
        redshift.executeStatement(
            ExecuteStatementRequest.builder()
                .workgroupName(redshiftWorkgroupName)
                .database(redshiftDB)
                .sql(query)
                .build()
        ).id()

    private fun waitForQueryToComplete(queryExecutionId: String) {
        val statement = redshift.describeStatement(DescribeStatementRequest.builder().id(queryExecutionId).build())
        return when {
            statement.status().finishedWithError() ->
                throw RuntimeException("The Amazon Redshift query failed to run with status '${statement.status()}' and error message: " + statement.error())

            statement.status() == FINISHED -> return
            else -> {
                Thread.sleep(getQueryStatusPollingInMs)
                waitForQueryToComplete(queryExecutionId)
            }
        }
    }

    private fun StatusString.finishedWithError() = this == ABORTED || this == FAILED || this == UNKNOWN_TO_SDK_VERSION

    private fun processResultRows(queryExecutionId: String): List<UserClick> {
        val statementResult = redshift.getStatementResult(GetStatementResultRequest.builder().id(queryExecutionId).build())
        val columns = statementResult.columnMetadata().map { it.name() }.asMapOfPosValue()
        return statementResult.records().map { fields ->
            UserClick(
                event_id = fields.getAsString("event_id", columns),
                user_id = fields.getAsString("user_id", columns),
                device_id = fields.getAsString("device_id", columns),
                element_id = fields.getAsString("element_id", columns),
                latitude = fields.getAsString("latitude", columns),
                longitude = fields.getAsString("longitude", columns),
                country = fields.getAsString("country", columns),
                time = fields.getAsString("time", columns).let { ts -> LocalDateTime.parse(ts, dateFmt) },
                ip_address = fields.getAsString("ip_address", columns),
                city = fields.getAsString("city", columns),
                os = fields.getAsString("os", columns),
                device_model = fields.getAsString("device_model", columns),
            )
        }
    }

    private fun List<String>.asMapOfPosValue() = this.mapIndexed { i, v -> Pair(v, i) }.toMap()

    private fun List<Field>.getAsString(fieldName: String, columns: Map<String, Int>): String =
        columns[fieldName]?.let { this[it].stringValue() } ?: throw RuntimeException(
            "Field '$fieldName' not present within the columns ${columns.keys}}"
        )
}

package iceberg.fetch

import iceberg.UserClick
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest
import software.amazon.awssdk.services.athena.model.GetQueryRuntimeStatisticsRequest
import software.amazon.awssdk.services.athena.model.QueryExecutionContext
import software.amazon.awssdk.services.athena.model.QueryExecutionState.CANCELLED
import software.amazon.awssdk.services.athena.model.QueryExecutionState.FAILED
import software.amazon.awssdk.services.athena.model.QueryExecutionState.SUCCEEDED
import software.amazon.awssdk.services.athena.model.QueryExecutionStatistics
import software.amazon.awssdk.services.athena.model.ResultConfiguration
import software.amazon.awssdk.services.athena.model.Row
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

class AthenaUserClicksFetcher(
    athenaQueriesOutputBuket: String,
    private val table: String,
    database: String,
    private val getQueryStatusPollingInMs: Long = 100,
    private val logger: Logger = LoggerFactory.getLogger(AthenaUserClicksFetcher::class.java)
) : UserClicksFetcher {

    private val athena = AthenaClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).build()

    private val queryExecutionContext = QueryExecutionContext.builder().database(database).build()

    private val resultConfiguration = ResultConfiguration.builder().outputLocation(athenaQueriesOutputBuket).build()

    private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS 'UTC'")

    @OptIn(ExperimentalTime::class)
    override fun fetch(elementId: UUID, from: LocalDate, to: LocalDate, logQueryStats: Boolean): List<UserClick> {
        val timedValue = measureTimedValue {
            val query = """SELECT * FROM $table 
                           WHERE (time between TIMESTAMP '$from 00:00:00' AND TIMESTAMP '$to 00:00:00') 
                                 AND element_id = '$elementId'"""
            val queryExecutionId = submitAthenaQuery(query)
            val stats = waitForQueryToComplete(queryExecutionId)
            processResultRows(queryExecutionId)
                .also {
                    if (logQueryStats) {
                        logger.info("Query execution stats: $stats")
                        logger.info("Query runtime stats: ${
                            athena.getQueryRuntimeStatistics(
                                GetQueryRuntimeStatisticsRequest.builder().queryExecutionId(queryExecutionId).build()
                            )
                        }")
                    }
                }
        }
        if (logQueryStats) logger.info("Fetch took : ${timedValue.duration}")
        return timedValue.value
    }

    private fun submitAthenaQuery(query: String) =
        StartQueryExecutionRequest.builder()
            .queryString(query)
            .queryExecutionContext(queryExecutionContext)
            .resultConfiguration(resultConfiguration)
            .build()
            .let { athena.startQueryExecution(it).queryExecutionId() }

    private tailrec fun waitForQueryToComplete(queryExecutionId: String?): QueryExecutionStatistics {
        val getQueryExecutionResponse = athena.getQueryExecution(
            GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build()
        )
        return when (getQueryExecutionResponse.queryExecution().status().state()) {
            FAILED -> throw RuntimeException("The Amazon Athena query '$queryExecutionId' failed to run with error message: " +
                getQueryExecutionResponse.queryExecution().status().stateChangeReason())

            CANCELLED -> throw RuntimeException("The Amazon Athena query '$queryExecutionId' was cancelled.")
            SUCCEEDED -> getQueryExecutionResponse.queryExecution().statistics()
            else -> {
                Thread.sleep(getQueryStatusPollingInMs)
                waitForQueryToComplete(queryExecutionId)
            }
        }
    }

    private fun processResultRows(queryExecutionId: String?): List<UserClick> =
        athena.getQueryResultsPaginator(
            GetQueryResultsRequest.builder().queryExecutionId(queryExecutionId).build()
        ).flatMap { response ->
            val columnNames = response.resultSet().rows().firstOrNull()
            columnNames?.let { columNamesRow ->
                val columns = columNamesRow.data().map { it.varCharValue() }.asMapOfPosValue()
                response.resultSet().rows().drop(1).map { row ->
                    UserClick(
                        event_id = row.getAsString("event_id", columns),
                        user_id = row.getAsString("user_id", columns),
                        device_id = row.getAsString("device_token", columns),
                        element_id = row.getAsString("element_id", columns),
                        latitude = row.getAsString("latitude", columns),
                        longitude = row.getAsString("longitude", columns),
                        country = row.getAsString("country", columns),
                        time = row.getAsString("time", columns).let { ts -> LocalDateTime.parse(ts, dateFmt) },
                        ip_address = row.getAsString("ip_address", columns),
                        city = row.getAsString("city", columns),
                        os = row.getAsString("os", columns),
                        device_model = row.getAsString("device_model", columns),
                    )
                }
            } ?: emptyList()
        }

    private fun List<String>.asMapOfPosValue() = this.mapIndexed { i, v -> Pair(v, i) }.toMap()

    private fun Row.getAsString(fieldName: String, columns: Map<String, Int>): String =
        columns[fieldName]?.let { this.data()[it].varCharValue() } ?: throw RuntimeException(
            "Field '$fieldName' not present within the columns ${columns.keys}}"
        )
}

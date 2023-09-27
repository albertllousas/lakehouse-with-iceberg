package iceberg

import iceberg.fetch.AthenaUserClicksFetcher
import iceberg.fetch.RedshiftUserClicksFetcher
import iceberg.fetch.SparkUserClicksFetcher
import iceberg.store.IcebergAWSUserClicksStorage
import java.time.LocalDate.parse
import java.util.UUID

val traceableElementId: UUID = UUID.fromString("b17ee30c-05f6-4674-9166-5781f5740f9d")
val s3Bucket = "s3://iceberg-lakehouse"
val table = "user_clicks_iceberg"
val glueDB = "db"

fun main() {
    val spark = SparkSessionBuilder.buildForS3WithIcebergIntegration(s3Buket = s3Bucket)
    val awsStorage = IcebergAWSUserClicksStorage(
        spark = spark, table = table, glueDB = glueDB
    )
    val sparkUserClicksFetcher = SparkUserClicksFetcher(
        spark = spark, table = table, glueDB = glueDB
    )
    val athenaUserClicksFetcher = AthenaUserClicksFetcher(
        athenaQueriesOutputBuket = "$s3Bucket/athena-query-results/", table = table, database = glueDB
    )
    val redshiftLoginAttemptsFetcher = RedshiftUserClicksFetcher(
        redshiftExternalScheme = "test_2",
        redshiftDB = "dev",
        redshiftWorkgroupName = "test-redshift-with-iceberg",
        table = table
    )

    val userClicks = UserClickBuilder.create(
        from = parse("2023-01-01"),
        to = parse("2023-12-31"),
        traceableElementId = traceableElementId,
        dailyClicks = 6_000_000
    )
    awsStorage.store(userClicks)
    sparkUserClicksFetcher.fetch(traceableElementId, parse("2023-01-01"), parse("2023-12-01"), logQueryStats = true)
    athenaUserClicksFetcher.fetch(traceableElementId, parse("2023-01-01"), parse("2023-12-01"), logQueryStats = true)
    redshiftLoginAttemptsFetcher.fetch(traceableElementId, parse("2023-01-01"), parse("2023-12-01"), logQueryStats = true)
}
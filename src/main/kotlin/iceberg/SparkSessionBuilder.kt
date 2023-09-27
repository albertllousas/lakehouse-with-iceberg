package iceberg

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

    fun buildForS3WithIcebergIntegration(
            s3Buket: String,
            sparkCatalog: String = "spark_catalog",
            appName: String = "Spark-S3-Iceberg-User-Clicks"
    ): SparkSession = SparkConf().apply {
        this.setAppName(appName)
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//                .set("spark.sql.catalog.spark_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
//                .set("spark.sql.catalog.spark_catalog.lock.table", "myGlueLockTable")
//                .set("spark.eventLog.enabled", "true")
                .set("spark.sql.legacy.createHiveTableByDefault", "false")
                .set("spark.sql.defaultCatalog", sparkCatalog)
                .set("spark.sql.catalog.$sparkCatalog.glue.skip-name-validation", "true")
                .set("spark.sql.catalog.$sparkCatalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.$sparkCatalog.warehouse", s3Buket)
                .set("spark.sql.catalog.$sparkCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .set("spark.sql.catalog.$sparkCatalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .setMaster("local[*]")
                .set("spark.ui.enabled", "false")
    }.let { SparkSession.builder().config(it).getOrCreate() }
}

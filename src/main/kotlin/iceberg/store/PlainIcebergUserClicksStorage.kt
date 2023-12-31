package iceberg.store

import iceberg.UserClick
import org.apache.iceberg.DataFile
import org.apache.iceberg.FileFormat.PARQUET
import org.apache.iceberg.PartitionKey
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericAppenderFactory
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.InternalRecordWrapper
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.OutputFileFactory
import org.apache.iceberg.io.PartitionedFanoutWriter
import org.apache.iceberg.types.Types


class PlainIcebergUserClicksStorage(
    private val s3Bucket: String,
    private val glueDB: String,
    private val table: String,
) : ClicksStorage {

    private val glueCatalog = GlueCatalog().apply {
        initialize(
            "glue_catalog", mapOf(
                "catalog-impl" to "org.apache.iceberg.aws.glue.GlueCatalog",
                "warehouse" to s3Bucket,
                "clients.glue.credentials-provider-class" to "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            )
        )
    }

    private val schema = Schema(
        Types.NestedField.optional(1, "event_id", Types.StringType.get()),
        Types.NestedField.optional(2, "user_id", Types.StringType.get()),
        Types.NestedField.optional(3, "device_id", Types.StringType.get()),
        Types.NestedField.optional(4, "element_id", Types.StringType.get()),
        Types.NestedField.optional(5, "latitude", Types.StringType.get()),
        Types.NestedField.optional(6, "longitude", Types.StringType.get()),
        Types.NestedField.optional(7, "country", Types.StringType.get()),
        Types.NestedField.optional(8, "time", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(9, "ip_address", Types.StringType.get()),
        Types.NestedField.optional(10, "city", Types.StringType.get()),
        Types.NestedField.optional(11, "device_model", Types.StringType.get()),
        Types.NestedField.optional(12, "os", Types.StringType.get()),
    )

    private val spec = PartitionSpec.builderFor(schema)
        .day("time")
        .bucket("element_id", 128)
        .build()

    override fun store(userClicks: List<UserClick>) {
        val tableIdentifier = TableIdentifier.of(glueDB, table)
        val table = if (!glueCatalog.tableExists(tableIdentifier))
            glueCatalog.createTable(
                tableIdentifier,
                schema,
                spec,
                mapOf("write.metadata.delete-after-commit.enabled" to "true")
            )
        else glueCatalog.loadTable(tableIdentifier)

        val partitionedWriter: PartitionedFanoutWriter<Record> = object : PartitionedFanoutWriter<Record>(
            spec,
            PARQUET,
            GenericAppenderFactory(table.schema(), spec),
            OutputFileFactory.builderFor(table, 1, 1).format(PARQUET).build(),
            table.io(),
            1048576
        ) {
            override fun partition(record: Record): PartitionKey {
                val partitionKey = PartitionKey(table.spec(), spec.schema())
                val wrappedRecord = InternalRecordWrapper(schema.asStruct()).wrap(record)
                partitionKey.partition(wrappedRecord)
                return partitionKey
            }
        }
        val newTransaction = table.newTransaction()
        partitionedWriter.use {
            userClicks.forEach { click ->
                GenericRecord.create(schema).apply {
                    setField("event_id", click.event_id)
                    setField("user_id", click.user_id)
                    setField("device_id", click.device_id)
                    setField("element_id", click.element_id)
                    setField("latitude", click.latitude)
                    setField("longitude", click.longitude)
                    setField("country", click.country)
                    setField("time", click.time)
                    setField("ip_address", click.ip_address)
                    setField("city", click.city)
                    setField("device_model", click.device_model)
                    setField("os", click.os)
                }.also(partitionedWriter::write)
            }
        }
        val appendFiles = newTransaction.newFastAppend()
        partitionedWriter.dataFiles().forEach(appendFiles::appendFile)
        appendFiles.commit()
        newTransaction.commitTransaction()
    }
}
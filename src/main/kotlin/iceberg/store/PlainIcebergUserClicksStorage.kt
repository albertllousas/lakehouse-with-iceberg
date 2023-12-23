package iceberg.store

import iceberg.UserClick
import org.apache.iceberg.*
import org.apache.iceberg.FileFormat.PARQUET
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
        val table: Table =
                if (!glueCatalog.tableExists(tableIdentifier))
                    glueCatalog.createTable(tableIdentifier, schema, spec, mapOf("write.metadata.delete-after-commit.enabled" to "true"))
                else glueCatalog.loadTable(tableIdentifier)


        val outputFileFactory = OutputFileFactory.builderFor(table, 1, 1).format(PARQUET).build()
        val partitionKey = PartitionKey(table.spec(), spec.schema())

        val partitionedWriter: PartitionedFanoutWriter<Record> = object : PartitionedFanoutWriter<Record>(
                spec,
                PARQUET,
                GenericAppenderFactory(table.schema(), spec),
                outputFileFactory,
                table.io(),
                1048576
        ) {
            override fun partition(record: Record): PartitionKey {
                val wrappedRecord = InternalRecordWrapper(schema.asStruct()).wrap(record)
                partitionKey.partition(wrappedRecord)
                return partitionKey
            }
        }
        val newTransaction = table.newTransaction()
        userClicks
                .forEach { click ->
                    val record = GenericRecord.create(schema)
                    record.setField("event_id", click.event_id)
                    record.setField("user_id", click.user_id)
                    record.setField("device_id", click.device_id)
                    record.setField("element_id", click.element_id)
                    record.setField("latitude", click.latitude)
                    record.setField("longitude", click.longitude)
                    record.setField("country", click.country)
                    record.setField("time", click.time)
                    record.setField("ip_address", click.ip_address)
                    record.setField("city", click.city)
                    record.setField("device_model", click.device_model)
                    record.setField("os", click.os)
                    partitionedWriter.write(record)
                }
        partitionedWriter.close()

        val appendFiles = newTransaction.newFastAppend()
        partitionedWriter.dataFiles().forEach { dataFile: DataFile? -> appendFiles.appendFile(dataFile) }
        appendFiles.commit()
        newTransaction.commitTransaction()
    }
}
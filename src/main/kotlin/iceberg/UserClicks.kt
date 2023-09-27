package iceberg

import com.github.javafaker.Faker
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import kotlin.random.Random

data class UserClick(
    val event_id: String,
    val element_id: String,
    val user_id: String,
    val device_id: String,
    val latitude: String,
    val longitude: String,
    val country: String,
    val time: LocalDateTime,
    val ip_address: String,
    val city: String,
    val os: String,
    val device_model: String
)

private val faker = Faker()

object UserClickBuilder {

    fun create(from: LocalDate, to: LocalDate, dailyClicks: Int, traceableElementId: UUID): List<UserClick> =
        (from.toEpochDay()..to.toEpochDay()).flatMap {
            create(
                date = LocalDate.ofEpochDay(it),
                dailyAttempts = dailyClicks,
                traceableElementId = traceableElementId
            )
        }

    fun create(date: LocalDate, dailyAttempts: Int, traceableElementId: UUID): List<UserClick> =
        (1..dailyAttempts).map { create(time = date.atTime(Random.nextInt(1, 23), Random.nextInt(1, 59))) } +
            create(time = date.atTime(Random.nextInt(1, 23), Random.nextInt(1, 59)), element_id = traceableElementId)

    fun create(
        event_id: UUID = UUID.randomUUID(),
        user_id: UUID = UUID.randomUUID(),
        device_token: UUID = UUID.randomUUID(),
        element_id: UUID = UUID.randomUUID(),
        latitude: String = faker.address().latitude(),
        longitude: String = faker.address().longitude(),
        country: String = faker.country().countryCode2(),
        time: LocalDateTime = LocalDateTime.now(),
        ip_address: String = faker.internet().ipV4Address(),
        city: String = faker.address().city(),
        os: String = faker.options().option("IOS 17", "Android 13.0.0_r67", "Chrome 114"),
        device_model: String = faker.options().option("Iphone", "Android", "Web"),
    ) = UserClick(
        event_id = event_id.toString(),
        user_id = user_id.toString(),
        device_id = device_token.toString(),
        element_id = element_id.toString(),
        latitude = latitude,
        longitude = longitude,
        country = country,
        time = time,
        ip_address = ip_address,
        city = city,
        os = os,
        device_model = device_model
    )
}

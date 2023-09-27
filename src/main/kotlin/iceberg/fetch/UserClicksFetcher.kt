package iceberg.fetch

import iceberg.UserClick
import java.time.LocalDate
import java.util.UUID

interface UserClicksFetcher {

    fun fetch(userId: UUID, from: LocalDate, to: LocalDate, logQueryStats: Boolean = true): List<UserClick>
}

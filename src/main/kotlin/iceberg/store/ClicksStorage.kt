package iceberg.store

import iceberg.UserClick

interface ClicksStorage {
    fun store(userClicks: List<UserClick>)
}
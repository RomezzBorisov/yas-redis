package com.redis

import java.util.concurrent.{Executor, Executors}

class ConnectionConfig(val host: String = "localhost",
                       val port: Int = 6379,
                       val maxLineLength: Int = 1024 * 1024
)
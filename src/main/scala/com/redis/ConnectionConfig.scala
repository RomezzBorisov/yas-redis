package com.redis

import java.util.concurrent.{Executor, Executors}

class ConnectionConfig(val host: String = "localhost",
                       val port: Int = 6379,
                       val bossExecutor: Executor = Executors.newSingleThreadExecutor(),
                       val workerExecutor: Executor = Executors.newCachedThreadPool(),
                       val bossCount: Int = 1,
                       val workerCount: Int = Runtime.getRuntime.availableProcessors() * 2,
                       val maxLineLength: Int = 1024 * 1024)
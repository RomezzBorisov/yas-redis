yas-redis
=========

Yet another Scala Redis driver

Completely asynchronous Redis (http://www.redis.io) driver.

Based on Netty 4.x and Scala 10.x

Each command returns scala Future as a result.
Scala futures have monadic interface, so it's easy to combine commands.

Netty allows to scale the driver and (in the future) upgrade it to be usable with a Redis cluster, handling several
thousands of nodes.
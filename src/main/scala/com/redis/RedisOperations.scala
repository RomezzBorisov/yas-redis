package com.redis

import operations.{KeysOperations, HashOperations}

trait RedisOperations extends HashOperations with KeysOperations {
  self: RedisClient =>
}

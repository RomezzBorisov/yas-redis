package com.redis

trait RedisOperations extends HashOperations with KeysOperations {
  self: RedisClient =>
}

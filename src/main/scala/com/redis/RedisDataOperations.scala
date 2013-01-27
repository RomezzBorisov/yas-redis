package com.redis

import operations.{StringOperations, KeysOperations, HashOperations}

trait RedisDataOperations extends HashOperations with KeysOperations with StringOperations{
  self: RedisClient =>
}

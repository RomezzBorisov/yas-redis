package com.redis.operations

import org.specs2.mutable.Specification

class KeysOperationsSpec extends Specification with RedisEnv {
  sequential

  "del" should {
    "remove all keys specified" in new env {
      client.set("a","1")
      client.set("b","1")
      client.set("c","1")
      result(client.del("a","b","c","d")) must_== 3l
      result(client.get("a")) must_== None
      result(client.get("b")) must_== None
      result(client.get("c")) must_== None
    }

    "prohibit empty key set" in new env {
      result(client.del()) must throwA[IllegalArgumentException]
    }
  }

  "exists" should {
    "return true in case of existing key" in new env {
      client.set("a","1")
      result(client.exists("a")) must_== true
    }

    "return false in case of non-existing key" in new env {
      client.del("a")
      result(client.exists("a")) must_== false
    }
  }

  "expire" should {
    "put ttl to existing key" in new env {
      client.del("a")
      client.set("a","1")

      result(client.expire("a", 100)) must_== true
      val ttl = result(client.ttl("a"))
      ttl must be_<=(100l)
      ttl must be_>=(95l)
    }

    "signal the key does not exists" in new env {
      client.del("a")
      result(client.expire("a", 100)) must_== false
    }
  }

  "expireat" should {
    "put correct ttl to existing key" in new env {
      client.del("a")
      client.set("a","1")
      result(client.expireat("a", System.currentTimeMillis() / 1000 + 100l)) must_== true
      val ttl = result(client.ttl("a"))
      ttl must be_<=(100l)
      ttl must be_>=(95l)

    }

    "signal the key does not exist" in new env {
      client.del("a")
      result(client.expireat("a", System.currentTimeMillis() / 1000 + 100l)) must_== false
    }
  }

  "persist" should {
    "remove ttl from existing key" in new env {
      client.del("a")
      client.set("a","1")
      client.expire("a", 100)
      result(client.persist("a")) must_== true
      result(client.ttl("a")) must_== -1l
    }

    "signal the key does not have timeout" in new env {
      client.del("a")
      client.set("a","1")
      result(client.persist("a")) must_== false
    }

    "signal the key does not exist" in new env {
      client.del("a")
      result(client.persist("a")) must_== false
    }
  }


  /*

  def persist(key: String): Future[Boolean] =

  def pexpire(key: String, dur: Duration): Future[Boolean] =

  def pexpireAt(key: String, datetime: Date): Future[Boolean] =

  def pexpireAt(key: String, timestamp: Long): Future[Boolean] =

  def pttl(key: String): Future[Long] =

  def rename(from: String, to: String): Future[Boolean] =

  def renamenx(from: String, to: String): Future[Boolean] =

  def sort[T](key: String, req: SortRequest)(implicit parse: Parse[T]): Future[Option[Seq[T]]] =

  def ttl(key: String): Future[Long] =

   */



}

/**
**   Copyright (c) 2017 Blackfynn, Inc. All Rights Reserved.
**/
package com.pennsieve.streaming

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.scalatest.fixture.TestSuite
import scala.util.Random
import scalikejdbc.{ ConnectionPool, DBSession, SQL }
import scalikejdbc.scalatest.AutoRollback

trait TestConfig {
  implicit val config = ConfigFactory.load("test.conf");
}

trait TestDatabase extends AutoRollback { self: TestSuite =>

  Class.forName("org.h2.Driver")
  ConnectionPool.singleton("jdbc:h2:mem:db", "username", "password")

  override def fixture(implicit session: DBSession) {
    val sqlStatements = List(
      "CREATE SCHEMA IF NOT EXISTS timeseries",
      """
      CREATE TABLE IF NOT EXISTS timeseries.ranges(
        id SERIAL PRIMARY KEY,
        channel VARCHAR,
        rate NUMBER,
        hi NUMBER,
        lo NUMBER,
        location VARCHAR)
      """,
      """
      CREATE TABLE IF NOT EXISTS timeseries.unit_ranges(
        id SERIAL PRIMARY KEY,
        count NUMBER,
        channel VARCHAR,
        hi NUMBER,
        lo NUMBER,
        tsindex VARCHAR,
        tsblob VARCHAR
        )
      """,
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('N:channel:0c961088-46b8-4468-b45e-472eaf19a893',200,1301921822000000,1301922421995000,'combined.txt');
      """,
      /*
       * Set up a package with paginated channels - a single channel chunked into 2
       * separate files
       */
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('paginated_continuous_ch1_id',1,200000000,400000000,'paginated/page1')
      """,
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('paginated_continuous_ch1_id',1,400000000,600000000,'paginated/page2')
      """,
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('paginated_continuous_ch2_id',1,300000000,500000000,'paginated/page2')
      """,
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('paginated_continuous_ch2_id',1,500000000,700000000,'paginated/page1')
      """,
      """
      INSERT INTO timeseries.unit_ranges (count,channel,lo,hi,tsindex,tsblob) VALUES
        (200,'paginated_unit_ch1_id',200000000,400000000,'paginated_events/ch1_page1','paginated/page1')
      """,
      """
      INSERT INTO timeseries.unit_ranges (count,channel,lo,hi,tsindex,tsblob) VALUES
        (200,'paginated_unit_ch1_id',400000000,600000000,'paginated_events/ch1_page2','paginated/page2')
      """,
      """
      INSERT INTO timeseries.unit_ranges (count,channel,lo,hi,tsindex,tsblob) VALUES
        (200,'paginated_unit_ch2_id',300000000,500000000,'paginated_events/ch2_page1','paginated/page2')
      """,
      """
      INSERT INTO timeseries.unit_ranges (count,channel,lo,hi,tsindex,tsblob) VALUES
        (200,'paginated_unit_ch2_id',500000000,700000000,'paginated_events/ch2_page2','paginated/page1')
      """,
      /*
       * Just a generic channel for testing
       */
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
         ('N:channel:0c961088-46b8-4468-b45e-472eaf19a893',250,1483250462568155,1483336759964670, 'events')
      """,
      /*
       * Set up a montageable package
       *
       * Since we're using a referential vs cz montage in tests, all of
       * these will be compared to the cz channel, so it's OK for
       * testing purposes if all channels are the same except for cz
       */
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES

        ('Fp1_id', 200, 0, 20, 'montage/channel1'),
        ('Fp2_id', 200, 0, 20, 'montage/channel1'),
        ('F7_id', 200, 0, 20, 'montage/channel1'),
        ('F8_id', 200, 0, 20, 'montage/channel1'),
        ('T7_id', 200, 0, 20, 'montage/channel1'),
        ('T8_id', 200, 0, 20, 'montage/channel1'),
        ('P7_id', 200, 0, 20, 'montage/channel1'),
        ('P8_id', 200, 0, 20, 'montage/channel1'),
        ('F3_id', 200, 0, 20, 'montage/channel1'),
        ('F4_id', 200, 0, 20, 'montage/channel1'),
        ('C3_id', 200, 0, 20, 'montage/channel1'),
        ('C4_id', 200, 0, 20, 'montage/channel1'),
        ('P3_id', 200, 0, 20, 'montage/channel1'),
        ('P4_id', 200, 0, 20, 'montage/channel1'),
        ('Q1_id', 200, 0, 20, 'montage/channel1'),
        ('Q2_id', 200, 0, 20, 'montage/channel1'),
        ('F2_id', 200, 0, 20, 'montage/channel1'),
        ('P2_id', 200, 0, 20, 'montage/channel1'),

        ('Cz_id', 200, 0, 20, 'montage/channel2')
      """,
      /*
       * Set up a basic package with 2 channels
       */
      """
      INSERT INTO timeseries.ranges (channel,rate,lo,hi,location) VALUES
        ('channel1_id', 200, 0, 20, 'montage/channel1'),
        ('channel2_id', 200, 0, 20, 'montage/channel2')
      """,
      """
      INSERT INTO timeseries.unit_ranges (count,channel,lo,hi,tsindex,tsblob) VALUES
         (20,'0470a95f90f74d8f86b87bb9664ed61a',1301921822000000,1301922421995000,'0470a95f90f74d8f86b87bb9664ed61a_1301921822000000_1301922421995000.bfts','')
      """
    )

    sqlStatements foreach (stmt => SQL(stmt).update.apply())
  }
}

trait AkkaImplicits {
  implicit val system = ActorSystem("test-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
}

trait SessionGenerator {
  private val sessionGenerator = Random.alphanumeric
  def getRandomSession(): String = sessionGenerator.take(10).mkString("")
}

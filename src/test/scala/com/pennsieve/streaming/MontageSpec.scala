package com.pennsieve.streaming

import com.blackfynn.models.Channel
import com.blackfynn.streaming.server.{ VirtualChannel => BFVirtualChannel }
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._
import scala.collection.JavaConverters._

import com.blackfynn.streaming.server.{ Montage, MontageRequest, MontageType }
import com.blackfynn.streaming.server.TSJsonSupport._

class MontageSpec extends FlatSpec with Matchers {
  val REFERENTIAL_VS_CZ_PACKAGE = "referential_vs_cz_package"
  val UNMONTAGED_PACKAGE = "unmontaged_package"

  val packageMontages = new ConcurrentHashMap[String, MontageType]().asScala
  packageMontages.put(REFERENTIAL_VS_CZ_PACKAGE, MontageType.ReferentialVsCz)
  packageMontages.put(UNMONTAGED_PACKAGE, MontageType.NotMontaged)

  "a montage request" should "be parsed" in {
    val reqString = """
      {
        "packageId": "my_package",
        "montage": "REFERENTIAL_VS_CZ"
      }
    """
    val req = reqString.parseJson.convertTo[MontageRequest]
    assert(req.montage == MontageType.ReferentialVsCz)
  }

  "buildMontage" should "build the correct montage" in {
    val channelMap: Map[String, Channel] =
      MontageType.ReferentialVsCz.distinctValues.map { channelName =>
        val nodeId = s"${channelName}_node_id"
        nodeId -> Channel(
          nodeId = nodeId,
          packageId = 0,
          name = channelName,
          start = 0,
          end = 0,
          unit = "",
          rate = 0,
          `type` = "",
          group = None,
          lastAnnotation = 0
        )
      }.toMap

    val virtualChannels = MontageType.ReferentialVsCz.pairs
      .map { pair =>
        val name = Montage.getMontageName(pair._1, pair._2)
        BFVirtualChannel(id = s"${name}_node_id", name = name)
      }

    val montage =
      Montage
        .buildMontage(packageMontages, REFERENTIAL_VS_CZ_PACKAGE, channelMap, virtualChannels)
        .map(_.map {
          case (lead, secondary) => lead.name -> secondary.map(_.name)
        })

    assert(
      montage ==
        Right(
          MontageType.ReferentialVsCz.pairs
            .map(tup => tup._1 -> Some(tup._2))
        )
    )
  }

  "getMontagePair" should "not split channel names if there is no montage" in {
    val channelName = "my-invalid-montage-channel"

    val pair = Montage.getMontagePair(channelName, MontageType.NotMontaged)

    pair shouldBe Right((channelName, None))
  }

  "getMontagePair" should "split channel names if is a montage" in {
    val channelName = "montage-channel"

    val pair =
      Montage.getMontagePair(channelName, MontageType.ReferentialVsCz)

    pair shouldBe Right(("montage", Some("channel")))
  }

  "getMontagePair" should "fail to split invalid channel names if there is a montage" in {
    val channelName = "invalid-montage-channel"

    val pair =
      Montage.getMontagePair(channelName, MontageType.ReferentialVsCz)

    pair.isLeft shouldBe true
  }

  "buildMontage" should "succeed with invalid channel names if there is no montage" in {
    val invalidNames = List("tricky-name", "really-invalid-name")

    val channelMap: Map[String, Channel] =
      invalidNames.map { channelName =>
        val nodeId = s"${channelName}_node_id"
        nodeId -> Channel(
          nodeId = nodeId,
          packageId = 0,
          name = channelName,
          start = 0,
          end = 0,
          unit = "",
          rate = 0,
          `type` = "",
          group = None,
          lastAnnotation = 0
        )
      }.toMap

    val virtualChannels = invalidNames
      .map { name =>
        BFVirtualChannel(id = s"${name}_node_id", name = name)
      }

    val montage =
      Montage
        .buildMontage(packageMontages, UNMONTAGED_PACKAGE, channelMap, virtualChannels)
        .map(_.map {
          case (lead, secondary) => lead.name -> secondary.map(_.name)
        })

    montage.isRight shouldBe true

    montage.right.get should contain theSameElementsAs (invalidNames.map(_ -> None))
  }

  "buildMontage" should "fail with invalid channel names if is a montage" in {
    val invalidNames = List("tricky-name", "really-invalid-name")

    val channelMap: Map[String, Channel] =
      invalidNames.map { channelName =>
        val nodeId = s"${channelName}_node_id"
        nodeId -> Channel(
          nodeId = nodeId,
          packageId = 0,
          name = channelName,
          start = 0,
          end = 0,
          unit = "",
          rate = 0,
          `type` = "",
          group = None,
          lastAnnotation = 0
        )
      }.toMap

    val virtualChannels = invalidNames
      .map { name =>
        BFVirtualChannel(id = s"${name}_node_id", name = name)
      }

    val montage =
      Montage
        .buildMontage(packageMontages, REFERENTIAL_VS_CZ_PACKAGE, channelMap, virtualChannels)
        .map(_.map {
          case (lead, secondary) => lead.name -> secondary.map(_.name)
        })

    montage.isLeft shouldBe true
    montage.left.get.name shouldBe "InvalidMontageName"
  }
}

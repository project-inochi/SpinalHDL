package spinal.lib.misc.pipeline

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer

class MergeLink(override val ups: Seq[Node], val down: Node) extends Link {
  ups.foreach(_.down = this)
  down.up = this

  /* Node buffer to payload translate */
  val buffers = ups.zipWithIndex.map { case (_, i) =>
    new Node(down.defaultKey).setCompositeName(this, s"hit_${i}")
  }
  private val sourceDefs = ArrayBuffer.fill[Option[Source]](ups.size)(None)

  override def downs: Seq[Node] = List(down)

  class Source(val id: Int) extends spinal.core.Area {
    require(id >= 0 && id < ups.size, s"Merge source id $id is out of range")
    require(sourceDefs(id).isEmpty, s"Merge source id $id already has a Source")
    sourceDefs(id) = Some(this)

    def up: Node = ups(id)
    def down: Node = buffers(id)

    def bypass(payloads: Payload[_ <: Data]*): Unit = {
      for (payload <- payloads) {
        val p = payload.asInstanceOf[Payload[Data]]
        down(p) := up(p)
      }
    }
  }

  private def translatedKeys = buffers.flatMap(_.keyToData.keys).distinct

  override def propagateDown(): Unit = {
    for (key <- translatedKeys) {
      down(key)
    }
    down.valid
  }

  override def propagateUp(): Unit = ups.foreach(_.ready)

  override def build(): Unit = {
    val keys = translatedKeys
    for (key <- keys) {
      val missing = buffers.zipWithIndex.filterNot(_._1.keyToData.contains(key)).map(_._2)
      if (missing.nonEmpty) {
        SpinalError(s"MergeLayer payload $key is not translated for source(s) ${missing.mkString(", ")}")
      }
      down(key)
    }

    val hitOH = OHMasking.first(Vec(ups.map(_.isValid)))
    down.valid := ups.map(_.isValid).orR
    for ((up, hit) <- (ups, hitOH).zipped) {
      up.ready := hit && down.isReady
    }

    for (key <- keys) {
      down(key) := MuxOH.or(hitOH.asBits, buffers.map(_(key)), true)
    }
  }
}

class MergeLayer(val upKeys: Seq[Any], val downKey: Any) extends MergeLink(upKeys.map(new Node(_)), new Node(downKey)) {
  require(upKeys.nonEmpty, "MergeLayer needs at least one upstream layer")

  ups.zipWithIndex.foreach { case (up, i) =>
    up.setCompositeName(this, s"up_$i")
  }
  down.setCompositeName(this, "down")
}

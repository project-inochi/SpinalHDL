package spinal.lib.misc.pipeline

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer

/**
 * One upstream node feeding multiple downstream nodes.
 *
 * Multiple targets may be selected in the same cycle; BranchLink does not
 * enforce one-hot routing. This makes it a generic structural dispatcher, not
 * a synchronized broadcast primitive. If a design needs single-target routing,
 * that contract is provided by the user's selected logic.
 */
class BranchLink(val up: Node, override val downs: Seq[Node]) extends Link {
  downs.foreach(_.up = this)
  up.down = this

  private val targetDefs = ArrayBuffer.fill[Option[Target]](downs.size)(None)

  override def ups: Seq[Node] = List(up)

  class Target(val id: Int) extends spinal.core.Area {
    require(id >= 0 && id < downs.size, s"Branch target id $id is out of range")
    require(targetDefs(id).isEmpty, s"Branch target id $id already has a Target")
    targetDefs(id) = Some(this)

    def up: Node = BranchLink.this.up
    def down: Node = downs(id)
    /** True when this downstream target receives the current transaction. */
    def selected: Bool = True

    def bypass(payloads: Payload[_ <: Data]*): Unit = {
      for (payload <- payloads) {
        val p = payload.asInstanceOf[Payload[Data]]
        down(p) := up(p)
      }
    }
  }

  override def propagateDown(): Unit = downs.foreach(_.valid)

  override def propagateUp(): Unit = {
    up.ready
    downs.foreach(_.ready)
  }

  override def build(): Unit = {
    val selecteds = targetDefs.map(_.map(_.selected).getOrElse(False))
    val passReady = (selecteds, downs).zipped.map(_ && _.isReady)
    val anyPass = selecteds.orR

    for ((down, selected) <- (downs, selecteds).zipped) {
      down.valid := up.isValid && selected
    }
    up.ready := !up.isValid || !anyPass || passReady.orR
  }
}

class BranchLayer(val upKey: Any, val downKeys: Seq[Any]) extends BranchLink(new Node(upKey), downKeys.map(new Node(_))) {
  require(downKeys.nonEmpty, "BranchLayer needs at least one downstream layer")

  up.setCompositeName(this, "up")
  downs.zipWithIndex.foreach { case (down, i) =>
    down.setCompositeName(this, s"down_$i")
  }
}

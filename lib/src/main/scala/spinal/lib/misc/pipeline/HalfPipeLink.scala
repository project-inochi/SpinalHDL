package spinal.lib.misc.pipeline

import spinal.core._

object HalfPipeLink {
  def apply(up: Node, down: Node): HalfPipeLink = new HalfPipeLink(up, down)
}

/**
 * A strict non-flow-through half-pipe link.
 *
 * This is the pipeline-link equivalent of Stream.halfPipe(): it cuts the
 * valid, ready and payload paths with a conservative one-entry buffer. It is
 * used by HierarchicalPipeLine to make graph-level feedback edges safe.
 *
 * Unlike DirectLink it never forwards an empty transaction combinationally.
 * Unlike StageLink it also cuts the downstream ready path. The cost is that
 * the first implementation does not support same-cycle push/pop and can divide
 * bandwidth by two.
 */
class HalfPipeLink(val up: Node, val down: Node) extends Link {
  down.up = this
  up.down = this

  override def ups: Seq[Node] = List(up)
  override def downs: Seq[Node] = List(down)

  override def propagateDown(): Unit = {
    propagateDownAll()
    down.valid
    down.ctrl.forgetOneSupported = true
  }

  override def propagateUp(): Unit = {
    propagateUpAll()
    up.ready
  }

  override def build(): Unit = {
    val matches = down.fromUp.payload.intersect(up.fromDown.payload)

    val rValid = RegInit(False).setCompositeName(this, "rValid")
    val rData = matches.map { e => e -> RegNextWhen(up(e), up.ready).setCompositeName(up(e), "halfPipeBuffer")}

    up.ready := !rValid
    down.valid := rValid

    rData.foreach(d => down(d._1) := d._2)

    val drop = False
    down.ctrl.forgetOne.foreach { cond =>
      drop setWhen(cond && rValid)
    }
    down.ctrl.cancel.foreach { cond =>
      drop setWhen(cond && rValid)
    }
    val clear = (down.isReady && rValid) || drop

    rValid setWhen(up.isValid) clearWhen(clear)
  }
}

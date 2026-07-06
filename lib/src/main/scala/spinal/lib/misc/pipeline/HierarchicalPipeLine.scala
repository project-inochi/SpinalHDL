package spinal.lib.misc.pipeline

import spinal.core._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PipelineLayerDag {
  sealed trait EdgeKind
  object EdgeKind {
    case object Branch extends EdgeKind
    case object Merge extends EdgeKind
  }

  case class Edge(
    from: Any,
    to: Any,
    kind: EdgeKind,
    ownerId: Int,
    portId: Int
  )
}

/**
 * Layer-level DAG for HierarchicalPipeLine by using BFS.
 *
 * This DAG is mainly used for feedback detection, and provides necessary
 * information for the kind of connector for the layer connection.
 */
class PipelineLayerDag(branches: Seq[HierarchicalPipeLine#BranchLayer], merges: Seq[HierarchicalPipeLine#MergeLayer]) {
  import PipelineLayerDag._
  import PipelineLayerDag.EdgeKind

  val edges: Seq[Edge] = {
    val ret = ArrayBuffer[Edge]()

    for ((branch, branchId) <- branches.zipWithIndex) {
      for ((downKey, downId) <- branch.downKeys.zipWithIndex) {
        ret += Edge(
          from = branch.upKey,
          to = downKey,
          kind = EdgeKind.Branch,
          ownerId = branchId,
          portId = downId
        )
      }
    }

    for ((merge, mergeId) <- merges.zipWithIndex) {
      for ((upKey, upId) <- merge.upKeys.zipWithIndex) {
        ret += Edge(
          from = upKey,
          to = merge.downKey,
          kind = EdgeKind.Merge,
          ownerId = mergeId,
          portId = upId
        )
      }
    }

    ret.toSeq
  }

  def travel(from: Any, to: Any, skip: Option[Edge] = None): Boolean = {
    val visited = mutable.LinkedHashSet[Any]()
    val pending = mutable.Queue[Any]()

    pending.enqueue(from)

    while (pending.nonEmpty) {
      val current = pending.dequeue()

      if (current == to) return true

      if (!visited.contains(current)) {
        visited += current

        for (edge <- edges if skip.forall(_ != edge)) {
          if (edge.from == current && !visited.contains(edge.to)) {
            pending.enqueue(edge.to)
          }
        }
      }
    }

    false
  }

  lazy val feedbackEdges: Set[Edge] = edges.filter(e => travel(e.to, e.from, Some(e))).toSet

  def isFeedback(kind: EdgeKind, ownerId: Int, portId: Int): Boolean = feedbackEdges.exists(e =>
    e.kind == kind && e.ownerId == ownerId && e.portId == portId
  )

  def boundaryLink(up: Node, down: Node, halfPipe: Boolean): Link = if (halfPipe) HalfPipeLink(up, down) else DirectLink(up, down)

  def boundaryLinks(stageLayer: Any => StagedLayer, owner: Nameable, autoLoopCut: Boolean): Seq[Link] = {
    val ret = ArrayBuffer[Link]()

    for ((branch, branchId) <- branches.zipWithIndex) {
      ret += boundaryLink(
        up = stageLayer(branch.upKey).down,
        down = branch.up,
        halfPipe = false
      ).setCompositeName(owner, s"branch_${branchId}_upLink")

      for (((down, downKey), i) <- branch.downs.zip(branch.downKeys).zipWithIndex) {
        ret += boundaryLink(
          up = down,
          down = stageLayer(downKey).up,
          halfPipe = autoLoopCut && isFeedback(EdgeKind.Branch, branchId, i)
        ).setCompositeName(owner, s"branch_${branchId}_downLink_$i")
      }
    }

    for ((merge, mergeId) <- merges.zipWithIndex) {
      for (((up, upKey), i) <- merge.ups.zip(merge.upKeys).zipWithIndex) {
        ret += boundaryLink(
          up = stageLayer(upKey).down,
          down = up,
          halfPipe = autoLoopCut && isFeedback(EdgeKind.Merge, mergeId, i)
        ).setCompositeName(owner, s"merge_${mergeId}_upLink_$i")
      }

      ret += boundaryLink(
        up = merge.down,
        down = stageLayer(merge.downKey).up,
        halfPipe = false
      ).setCompositeName(owner, s"merge_${mergeId}_downLink")
    }

    ret.toSeq
  }
}

class HierarchicalPipeLine(autoLoopCut: Boolean = true) extends Area {
  val layers = mutable.LinkedHashMap[Any, StagedLayer]()
  val branches = ArrayBuffer[BranchLayer]()
  val merges = ArrayBuffer[MergeLayer]()

  class StageLayer(override val defaultKey: Any = null) extends StagePipelineLayer(defaultKey) {
    require(!layers.contains(defaultKey))
    layers.update(defaultKey, this)
    setCompositeName(HierarchicalPipeLine.this, s"layer_${defaultKey.toString}")
  }

  class StageCtrlLayer(override val defaultKey: Any = null) extends StageCtrlPipelineLayer(defaultKey) {
    require(!layers.contains(defaultKey))
    layers.update(defaultKey, this)
    setCompositeName(HierarchicalPipeLine.this, s"layer_${defaultKey.toString}")
  }

  class BranchLayer(val upKey: Any, val downKeys: Seq[Any]) extends BranchLink(new Node(upKey), downKeys.map(new Node(_))) {
    require(downKeys.nonEmpty, "BranchLayer needs at least one downstream layer")
    setCompositeName(HierarchicalPipeLine.this, s"branch_${branches.size}")
    branches += this

    up.setCompositeName(this, "up")
    downs.zipWithIndex.foreach { case (down, i) =>
      down.setCompositeName(this, s"down_$i")
    }
  }

  class MergeLayer(val upKeys: Seq[Any], val downKey: Any) extends MergeLink(upKeys.map(new Node(_)), new Node(downKey)) {
    require(upKeys.nonEmpty, "MergeLayer needs at least one upstream layer")
    setCompositeName(HierarchicalPipeLine.this, s"merge_${merges.size}")
    merges += this

    ups.zipWithIndex.foreach { case (up, i) =>
      up.setCompositeName(this, s"up_$i")
    }
    down.setCompositeName(this, "down")
  }

  private def stageLayer(id: Any): StagedLayer = {
    layers.get(id) match {
      case Some(layer) => layer
      case None => require(false, s"HierarchicalPipeLine layer $id does not exist"); null
    }
  }

  def build(): Unit = {
    val connectors = ArrayBuffer[Link]()
    for (layer <- layers.values) {
      layer.build()
      connectors ++= layer.connectors
    }
    for (branch <- branches) connectors += branch
    for (merge <- merges) connectors += merge

    val graph = new PipelineLayerDag(branches, merges)
    val boundaryLinks = graph.boundaryLinks(stageLayer, this, autoLoopCut)

    Builder(connectors ++ boundaryLinks)
  }
}

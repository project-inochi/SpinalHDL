package spinal.lib.misc.pipeline

import spinal.core._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class PipelineLayerEdge(val from: Any, val to: Any) {
  override def toString: String = s"$from -> $to"
}

sealed trait CutDirection
case object CUT_INPUT extends CutDirection
case object CUT_OUTPUT extends CutDirection

/* Provides information for graph analysis. */
trait PipelineLayerEdgeProvider {
  /**
   * Directed edges contributed by this provider.
   *
   * Implementations must return the same edge instances across calls so the DAG
   * can use reference identity to distinguish and query individual edges.
   */
  def edges: Seq[PipelineLayerEdge]

  /**
   * Preferred cut direction for this provider's feedback edges.
   *
   * A defined direction applies to every feedback edge. None will let DAG to
   * choose either direction independently for each feedback edge.
   */
  def cutDirection: Option[CutDirection]
}

/**
 * Layer-level DAG for HierarchicalPipeLine by using BFS.
 *
 * Each provider contributes stable directed edge objects. The DAG detects
 * feedback edges and resolves their cut directions, while HierarchicalPipeLine
 * maps those decisions onto physical boundary links.
 */
class PipelineLayerDag(providers: Seq[PipelineLayerEdgeProvider], feedbackSeed: Int = 0) {
  val edges: Seq[PipelineLayerEdge] = providers.flatMap(_.edges)

  def travel(from: Any, to: Any, skip: Option[PipelineLayerEdge] = None): Boolean = {
    val visited = mutable.LinkedHashSet[Any]()
    val pending = mutable.Queue[Any]()

    pending.enqueue(from)

    while (pending.nonEmpty) {
      val current = pending.dequeue()

      if (current == to) return true

      if (!visited.contains(current)) {
        visited += current

        for (edge <- edges if skip.forall(_ ne edge)) {
          if (edge.from == current && !visited.contains(edge.to)) {
            pending.enqueue(edge.to)
          }
        }
      }
    }

    false
  }

  lazy val feedbackEdges: Set[PipelineLayerEdge] = edges.filter(e => travel(e.to, e.from, Some(e))).toSet

  lazy val feedbackCutDirections: Map[PipelineLayerEdge, CutDirection] = {
    val random = new scala.util.Random(feedbackSeed)
    providers.flatMap { provider =>
      provider.edges.filter(feedbackEdges.contains).map { edge =>
        edge -> provider.cutDirection.getOrElse[CutDirection] {
          if (random.nextBoolean()) CUT_INPUT else CUT_OUTPUT
        }
      }
    }.toMap
  }

  def isFeedback(edge: PipelineLayerEdge): Boolean = feedbackEdges.contains(edge)

  def cutDirectionOf(edge: PipelineLayerEdge): Option[CutDirection] = feedbackCutDirections.get(edge)
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

  final class BranchLayer(val upKey: Any, val downKeys: Seq[Any])
      extends BranchLink(new Node(upKey), downKeys.map(new Node(_)))
      with PipelineLayerEdgeProvider {
    require(downKeys.nonEmpty, "BranchLayer needs at least one downstream layer")
    setCompositeName(HierarchicalPipeLine.this, s"branch_${branches.size}")
    branches += this

    override val edges: Seq[PipelineLayerEdge] = downKeys.map(downKey => new PipelineLayerEdge(upKey, downKey))
    override def cutDirection: Option[CutDirection] = Some(CUT_OUTPUT)

    up.setCompositeName(this, "up")
    downs.zipWithIndex.foreach { case (down, i) =>
      down.setCompositeName(this, s"down_$i")
    }
  }

  final class MergeLayer(val upKeys: Seq[Any], val downKey: Any)
      extends MergeLink(upKeys.map(new Node(_)), new Node(downKey))
      with PipelineLayerEdgeProvider {
    require(upKeys.nonEmpty, "MergeLayer needs at least one upstream layer")
    setCompositeName(HierarchicalPipeLine.this, s"merge_${merges.size}")
    merges += this

    override val edges: Seq[PipelineLayerEdge] = upKeys.map(upKey => new PipelineLayerEdge(upKey, downKey))
    final override val cutDirection: Option[CutDirection] = Some(CUT_INPUT)

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

  private def connectLink(up: Node, down: Node, halfPipe: Boolean): Link =
    if (halfPipe) HalfPipeLink(up, down) else DirectLink(up, down)

  def build(): Unit = {
    val connectors = ArrayBuffer[Link]()
    for (layer <- layers.values) {
      layer.build()
      connectors ++= layer.connectors
    }
    for (branch <- branches) connectors += branch
    for (merge <- merges) connectors += merge

    val graph = new PipelineLayerDag(branches.toSeq ++ merges.toSeq)

    for ((branch, branchId) <- branches.zipWithIndex) {
      connectors += connectLink(
        up = stageLayer(branch.upKey).down,
        down = branch.up,
        halfPipe = autoLoopCut && branch.edges.exists(graph.cutDirectionOf(_).contains(CUT_INPUT))
      ).setCompositeName(this, s"branch_${branchId}_upLink")

      for (((down, downKey), i) <- branch.downs.zip(branch.downKeys).zipWithIndex) {
        connectors += connectLink(
          up = down,
          down = stageLayer(downKey).up,
          halfPipe = autoLoopCut && graph.cutDirectionOf(branch.edges(i)).contains(CUT_OUTPUT)
        ).setCompositeName(this, s"branch_${branchId}_downLink_$i")
      }
    }

    for ((merge, mergeId) <- merges.zipWithIndex) {
      for (((up, upKey), i) <- merge.ups.zip(merge.upKeys).zipWithIndex) {
        connectors += connectLink(
          up = stageLayer(upKey).down,
          down = up,
          halfPipe = autoLoopCut && graph.cutDirectionOf(merge.edges(i)).contains(CUT_INPUT)
        ).setCompositeName(this, s"merge_${mergeId}_upLink_$i")
      }

      connectors += connectLink(
        up = merge.down,
        down = stageLayer(merge.downKey).up,
        halfPipe = autoLoopCut && merge.edges.exists(graph.cutDirectionOf(_).contains(CUT_OUTPUT))
      ).setCompositeName(this, s"merge_${mergeId}_downLink")
    }

    Builder(connectors)
  }
}

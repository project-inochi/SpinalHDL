package spinal.lib.misc.pipeline

import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.tester.SpinalAnyFunSuite

class PipelineLayerTester extends SpinalAnyFunSuite {
  def pushAndExpect(cd: ClockDomain, up: Stream[UInt], down: Stream[UInt], value: Int, expected: Int, maxCycles: Int = 64): Unit = {
    up.valid #= true
    up.payload #= value
    down.ready #= true

    var pushed = false
    var got = false
    var cycle = 0
    while (!got && cycle < maxCycles) {
      sleep(1)
      if (!pushed && up.ready.toBoolean) pushed = true
      if (down.valid.toBoolean) {
        assert(down.payload.toInt == expected)
        got = true
      }
      cd.waitSampling()
      if (pushed) up.valid #= false
      cycle += 1
    }
    up.valid #= false
    assert(got)
  }

  def requestPayloadAndValid[T <: Data](node: Node, payload: Payload[T]): Unit = {
    val key = NamedTypeKey(payload.asInstanceOf[Payload[Data]], node.defaultKey)
    node.fromUp.payload += key
    node.fromDown.payload += key
    node(payload)
    node.valid
  }

  test("pipelineLayerGraphFeedbackEdges") {
    SimConfig.compile(new Component {
      val pipe = new HierarchicalPipeLine()
      val branch = new pipe.BranchLayer("work", Seq("side", "finish"))
      val merge = new pipe.MergeLayer(Seq("main", "side"), "work")
      new pipe.StageLayer("stage")
      val graph = new PipelineLayerDag(pipe.branches.toSeq ++ pipe.merges.toSeq)
      val reversedGraph = new PipelineLayerDag((pipe.branches.toSeq ++ pipe.merges.toSeq).reverse)

      assert(graph.edges == branch.edges ++ merge.edges)
      assert(!graph.isFeedback(merge.edges(0)))
      assert(graph.isFeedback(merge.edges(1)))
      assert(graph.isFeedback(branch.edges(0)))
      assert(!graph.isFeedback(branch.edges(1)))
      assert(graph.cutDirectionOf(merge.edges(0)).isEmpty)
      assert(graph.cutDirectionOf(merge.edges(1)).contains(CUT_INPUT))
      assert(graph.cutDirectionOf(branch.edges(0)).contains(CUT_OUTPUT))
      assert(graph.cutDirectionOf(branch.edges(1)).isEmpty)
      assert(reversedGraph.feedbackEdges == graph.feedbackEdges)
    }).doSimUntilVoid { dut =>
      simSuccess()
    }
  }

  test("pipelineLayerGraphParallelEdgesKeepIdentity") {
    SimConfig.compile(new Component {
      val pipe = new HierarchicalPipeLine()
      val forward0 = new pipe.BranchLayer("a", Seq("b"))
      val forward1 = new pipe.BranchLayer("a", Seq("b"))
      val backward = new pipe.MergeLayer(Seq("b"), "a")
      val graph = new PipelineLayerDag(pipe.branches.toSeq ++ pipe.merges.toSeq)

      assert(forward0.edges(0) ne forward1.edges(0))
      assert(graph.travel("a", "b", Some(forward0.edges(0))))
      assert(graph.isFeedback(forward0.edges(0)))
      assert(graph.isFeedback(forward1.edges(0)))
      assert(graph.isFeedback(backward.edges(0)))
    }).doSimUntilVoid { dut =>
      simSuccess()
    }
  }

  test("pipelineLayerGraphDefaultCutDirectionIsStable") {
    val forward = new PipelineLayerEdge("a", "b")
    val backward = new PipelineLayerEdge("b", "a")
    val provider = new PipelineLayerEdgeProvider {
      override val edges = Seq(forward, backward)
      override val cutDirection: Option[CutDirection] = None
    }
    val graph0 = new PipelineLayerDag(Seq(provider))
    val graph1 = new PipelineLayerDag(Seq(provider))

    assert(graph0.feedbackCutDirections == graph1.feedbackCutDirections)
    assert(graph0.feedbackEdges.forall(graph0.cutDirectionOf(_).nonEmpty))
    assert(graph0.feedbackCutDirections.size == graph0.feedbackEdges.size)
  }

  test("halfPipeLinkStrictBuffer") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val down = master Stream(UInt(8 bits))
      val drop = in Bool()
      val forget = in Bool()

      val upNode = new Node("pipe")
      val downNode = new Node("pipe")
      val cut = HalfPipeLink(upNode, downNode)

      upNode.driveFrom(up)((self, payload) => self(VALUE) := payload)
      downNode.driveTo(down)((payload, self) => payload := self(VALUE))
      downNode.cancel := drop
      downNode.ctrl.forgetOneCreate()
      downNode.ctrl.forgetOne.get := forget

      Builder(List(cut))
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.up.payload #= 0
      dut.down.ready #= false
      dut.drop #= false
      dut.forget #= false
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 33
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.down.valid.toBoolean)
      dut.down.ready #= true
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.down.valid.toBoolean)
      cd.waitSampling()

      dut.up.valid #= false
      dut.down.ready #= false
      sleep(1)
      assert(!dut.up.ready.toBoolean)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 33)
      dut.down.ready #= true
      sleep(1)
      assert(!dut.up.ready.toBoolean)
      assert(dut.down.valid.toBoolean)
      cd.waitSampling()

      dut.down.ready #= false
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.down.valid.toBoolean)

      dut.up.valid #= true
      dut.up.payload #= 44
      cd.waitSampling()
      dut.up.valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 44)

      dut.drop #= true
      cd.waitSampling()
      dut.drop #= false
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.down.valid.toBoolean)

      dut.up.valid #= true
      dut.up.payload #= 55
      cd.waitSampling()
      dut.up.valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 55)

      dut.forget #= true
      cd.waitSampling()
      dut.forget #= false
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.down.valid.toBoolean)
      simSuccess()
    }
  }

  test("hierarchicalAutoHalfPipeTopology") {
    SimConfig.compile(new Component {
      val pipe = new HierarchicalPipeLine() {
        val main = new StageCtrlLayer("main")
        main.ctrl(0)

        val work = new StageCtrlLayer("work")
        work.ctrl(0)

        val side = new StageCtrlLayer("side")
        side.ctrl(0)

        val finish = new StageCtrlLayer("finish")
        finish.ctrl(0)

        new MergeLayer(Seq("main", "side"), "work")
        new BranchLayer("work", Seq("side", "finish"))
      }

      pipe.build()
    }).doSimUntilVoid { dut =>
      simSuccess()
    }
  }

  test("branchLinkSimple") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val downs = Vec.fill(2)(master Stream(UInt(8 bits)))

      val upNode = new Node("up")
      val downNodes = Seq(new Node("down0"), new Node("down1"))
      val split = new BranchLink(upNode, downNodes)

      new split.Target(0) {
        override def selected: Bool = up(VALUE) === 0
        down(VALUE) := up(VALUE) + 10
      }
      new split.Target(1) {
        override def selected: Bool = up(VALUE) === 1
        down(VALUE) := up(VALUE) + 20
      }

      upNode.driveFrom(up)((self, payload) => self(VALUE) := payload)
      downNodes(0).driveTo(downs(0))((payload, self) => payload := self(VALUE))
      downNodes(1).driveTo(downs(1))((payload, self) => payload := self(VALUE))

      Builder(List(split))
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.up.payload #= 0
      dut.downs.foreach(_.ready #= false)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 0
      dut.downs(0).ready #= false
      dut.downs(1).ready #= true
      sleep(1)
      assert(!dut.up.ready.toBoolean)
      assert(dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      assert(dut.downs(0).payload.toInt == 10)

      dut.downs(0).ready #= true
      sleep(1)
      assert(dut.up.ready.toBoolean)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 1
      dut.downs.foreach(_.ready #= true)
      sleep(1)
      assert(!dut.downs(0).valid.toBoolean)
      assert(dut.downs(1).valid.toBoolean)
      assert(dut.downs(1).payload.toInt == 21)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 3
      dut.downs.foreach(_.ready #= false)
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  test("mergeLinkSimple") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val ups = Vec.fill(2)(slave Stream(UInt(8 bits)))
      val down = master Stream(UInt(8 bits))

      val upNodes = Seq(new Node("up0"), new Node("up1"))
      val downNode = new Node("down")
      val joined = new MergeLink(upNodes, downNode)

      new joined.Source(0) {
        down(VALUE) := up(VALUE) + 10
      }
      new joined.Source(1) {
        down(VALUE) := up(VALUE) + 20
      }

      upNodes(0).driveFrom(ups(0))((self, payload) => self(VALUE) := payload)
      upNodes(1).driveFrom(ups(1))((self, payload) => self(VALUE) := payload)
      downNode.driveTo(down)((payload, self) => payload := self(VALUE))

      Builder(List(joined))
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.ups.foreach { up =>
        up.valid #= false
        up.payload #= 0
      }
      dut.down.ready #= false
      cd.waitSampling()

      dut.ups(0).valid #= true
      dut.ups(0).payload #= 5
      dut.ups(1).valid #= true
      dut.ups(1).payload #= 7
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 15)
      assert(!dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)

      dut.down.ready #= true
      sleep(1)
      assert(dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      dut.ups(0).valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 27)
      assert(dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  test("hierarchicalLayer") {
    SimConfig.compile(new Component {
      val pipe = new HierarchicalPipeLine()
      val a = new pipe.StageCtrlLayer("a")
      val b = new pipe.StageCtrlLayer("b")
      val n = new pipe.StageLayer("n")

      assert(pipe.layers("a") eq a)
      assert(pipe.layers("b") eq b)
      assert(pipe.layers("n") eq n)
      assert(!(a eq b))
      assert(a.ctrl(0).up.defaultKey == "a")
      assert(b.ctrl(0).up.defaultKey == "b")
      assert(n.node(0).defaultKey == "n")
      intercept[IllegalArgumentException] {
        new pipe.StageCtrlLayer("a")
      }
      intercept[IllegalArgumentException] {
        new pipe.StageLayer("b")
      }
    }).doSimUntilVoid { dut =>
      simSuccess()
    }
  }

  test("hierarchicalLayerQueryIsCheckedAtBuild") {
    SimConfig.compile(new Component {
      val pipe = new HierarchicalPipeLine()
      new pipe.StageCtrlLayer("source")
      new pipe.StageCtrlLayer("down")
      new pipe.StageCtrlLayer("up")
      new pipe.StageCtrlLayer("sink")
      new pipe.StageLayer("nodeLayer")

      new pipe.BranchLayer("missing", List("down"))
      new pipe.BranchLayer("source", List("missing"))
      new pipe.MergeLayer(List("up", "missing"), "sink")
      new pipe.MergeLayer(List("up"), "missing")
      new pipe.BranchLayer("nodeLayer", List("down"))
      new pipe.MergeLayer(List("up"), "nodeLayer")
    }).doSimUntilVoid { dut =>
      simSuccess()
    }

    intercept[IllegalArgumentException] {
      SimConfig.compile(new Component {
        val pipe = new HierarchicalPipeLine()
        new pipe.BranchLayer("missing", List("down"))
        new pipe.StageCtrlLayer("down").ctrl(0)
        pipe.build()
      })
    }
    intercept[IllegalArgumentException] {
      SimConfig.compile(new Component {
        val pipe = new HierarchicalPipeLine()
        new pipe.BranchLayer("source", List("missing"))
        new pipe.StageCtrlLayer("source").ctrl(0)
        pipe.build()
      })
    }
    intercept[IllegalArgumentException] {
      SimConfig.compile(new Component {
        val pipe = new HierarchicalPipeLine()
        new pipe.MergeLayer(List("missing"), "sink")
        new pipe.StageCtrlLayer("sink").ctrl(0)
        pipe.build()
      })
    }
    intercept[IllegalArgumentException] {
      SimConfig.compile(new Component {
        val pipe = new HierarchicalPipeLine()
        new pipe.MergeLayer(List("up"), "missing")
        new pipe.StageCtrlLayer("up").ctrl(0)
        pipe.build()
      })
    }
  }

  class HierarchicalBranchDut extends Component {
    val VALUE = Payload(UInt(8 bits))
    val up = slave Stream(UInt(8 bits))
    val downs = Vec.fill(2)(master Stream(UInt(8 bits)))

    val pipe = new HierarchicalPipeLine()
    val split = new pipe.BranchLayer("source", List("down0", "down1"))
    val source = new pipe.StageCtrlLayer("source")
    val down0 = new pipe.StageCtrlLayer("down0")
    val down1 = new pipe.StageCtrlLayer("down1")

    new split.Target(0) {
      override def selected: Bool = up(VALUE) === 0
      down(VALUE) := up(VALUE) + 10
    }
    new split.Target(1) {
      override def selected: Bool = up(VALUE) === 1
      down(VALUE) := up(VALUE) + 20
    }

    source.ctrl(0).up.driveFrom(up)((self, payload) => self(VALUE) := payload)
    down0.ctrl(0).down.driveTo(downs(0))((payload, self) => payload := self(VALUE))
    down1.ctrl(0).down.driveTo(downs(1))((payload, self) => payload := self(VALUE))

    pipe.build()
  }

  test("hierarchicalBranch") {
    SimConfig.compile(new HierarchicalBranchDut).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.up.payload #= 0
      dut.downs.foreach(_.ready #= false)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 0
      dut.downs(0).ready #= false
      dut.downs(1).ready #= true
      sleep(1)
      assert(!dut.up.ready.toBoolean)
      assert(dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      assert(dut.downs(0).payload.toInt == 10)

      dut.downs(0).ready #= true
      sleep(1)
      assert(dut.up.ready.toBoolean)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 1
      dut.downs.foreach(_.ready #= true)
      sleep(1)
      assert(dut.downs(1).valid.toBoolean)
      assert(!dut.downs(0).valid.toBoolean)
      assert(dut.downs(1).payload.toInt == 21)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 3
      dut.downs.foreach(_.ready #= false)
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  test("hierarchicalBranchMultiHitNoAssertion") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val downs = Vec.fill(2)(master Stream(UInt(8 bits)))

      val pipe = new HierarchicalPipeLine()
      val split = new pipe.BranchLayer("source", List("down0", "down1"))
      val source = new pipe.StageCtrlLayer("source")
      val down0 = new pipe.StageCtrlLayer("down0")
      val down1 = new pipe.StageCtrlLayer("down1")

      new split.Target(0) {
        down(VALUE) := up(VALUE) + 1
      }
      new split.Target(1) {
        down(VALUE) := up(VALUE) + 2
      }

      source.ctrl(0).up.driveFrom(up)((self, payload) => self(VALUE) := payload)
      down0.ctrl(0).down.driveTo(downs(0))((payload, self) => payload := self(VALUE))
      down1.ctrl(0).down.driveTo(downs(1))((payload, self) => payload := self(VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= true
      dut.up.payload #= 4
      dut.downs.foreach(_.ready #= true)
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(dut.downs(0).valid.toBoolean)
      assert(dut.downs(1).valid.toBoolean)
      cd.waitSampling()
      simSuccess()
    }
  }

  test("hierarchicalBranchBypass") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val EXTRA = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val downs = Vec.fill(2)(master Stream(UInt(8 bits)))

      val pipe = new HierarchicalPipeLine()
      val split = new pipe.BranchLayer("source", List("down0", "down1"))
      val source = new pipe.StageCtrlLayer("source")
      val down0 = new pipe.StageCtrlLayer("down0")
      val down1 = new pipe.StageCtrlLayer("down1")

      new split.Target(0) {
        override def selected: Bool = up(VALUE) === 0
        bypass(VALUE, EXTRA)
      }
      new split.Target(1) {
        override def selected: Bool = up(VALUE) === 1
        bypass(VALUE, EXTRA)
      }

      source.ctrl(0).up.driveFrom(up) { (self, payload) =>
        self(VALUE) := payload
        self(EXTRA) := payload + 3
      }
      down0.ctrl(0).down.driveTo(downs(0))((payload, self) => payload := self(VALUE))
      down1.ctrl(0).down.driveTo(downs(1))((payload, self) => payload := self(EXTRA))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= true
      dut.downs.foreach(_.ready #= true)

      dut.up.payload #= 0
      sleep(1)
      assert(dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      assert(dut.downs(0).payload.toInt == 0)
      cd.waitSampling()

      dut.up.payload #= 1
      sleep(1)
      assert(!dut.downs(0).valid.toBoolean)
      assert(dut.downs(1).valid.toBoolean)
      assert(dut.downs(1).payload.toInt == 4)
      cd.waitSampling()

      simSuccess()
    }
  }

  class HierarchicalNodeBranchDut extends Component {
    val VALUE = Payload(UInt(8 bits))
    val up = slave Stream(UInt(8 bits))
    val downs = Vec.fill(2)(master Stream(UInt(8 bits)))

    val pipe = new HierarchicalPipeLine()
    val split = new pipe.BranchLayer("source", List("down0", "down1"))
    val source = new pipe.StageLayer("source")
    val down0 = new pipe.StageLayer("down0")
    val down1 = new pipe.StageLayer("down1")

    new split.Target(0) {
      override def selected: Bool = up(VALUE) === 0
      down(VALUE) := up(VALUE) + 10
    }
    new split.Target(1) {
      override def selected: Bool = up(VALUE) === 1
      down(VALUE) := up(VALUE) + 20
    }

    source(0).driveFrom(up)((self, payload) => self(VALUE) := payload)
    down0(0).driveTo(downs(0))((payload, self) => payload := self(VALUE))
    down1(0).driveTo(downs(1))((payload, self) => payload := self(VALUE))

    pipe.build()
  }

  test("hierarchicalNodeBranch") {
    SimConfig.compile(new HierarchicalNodeBranchDut).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.up.payload #= 0
      dut.downs.foreach(_.ready #= false)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 0
      dut.downs(0).ready #= false
      dut.downs(1).ready #= true
      sleep(1)
      assert(!dut.up.ready.toBoolean)
      assert(dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      assert(dut.downs(0).payload.toInt == 10)

      dut.downs(0).ready #= true
      sleep(1)
      assert(dut.up.ready.toBoolean)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 1
      dut.downs.foreach(_.ready #= true)
      sleep(1)
      assert(dut.downs(1).valid.toBoolean)
      assert(!dut.downs(0).valid.toBoolean)
      assert(dut.downs(1).payload.toInt == 21)
      cd.waitSampling()

      dut.up.valid #= true
      dut.up.payload #= 3
      dut.downs.foreach(_.ready #= false)
      sleep(1)
      assert(dut.up.ready.toBoolean)
      assert(!dut.downs(0).valid.toBoolean)
      assert(!dut.downs(1).valid.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  class HierarchicalMergeDut extends Component {
    val VALUE = Payload(UInt(8 bits))
    val ups = Vec.fill(2)(slave Stream(UInt(8 bits)))
    val down = master Stream(UInt(8 bits))

    val pipe = new HierarchicalPipeLine()
    val joined = new pipe.MergeLayer(List("up0", "up1"), "sink")
    val up0 = new pipe.StageCtrlLayer("up0")
    val up1 = new pipe.StageCtrlLayer("up1")
    val sink = new pipe.StageCtrlLayer("sink")

    new joined.Source(0) {
      down(VALUE) := up(VALUE) + 10
    }
    new joined.Source(1) {
      down(VALUE) := up(VALUE) + 20
    }

    up0.ctrl(0).up.driveFrom(ups(0))((self, payload) => self(VALUE) := payload)
    up1.ctrl(0).up.driveFrom(ups(1))((self, payload) => self(VALUE) := payload)
    sink.ctrl(0).down.driveTo(down)((payload, self) => payload := self(VALUE))

    pipe.build()
  }

  test("hierarchicalMerge") {
    SimConfig.compile(new HierarchicalMergeDut).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.ups.foreach { up =>
        up.valid #= false
        up.payload #= 0
      }
      dut.down.ready #= false
      cd.waitSampling()

      dut.ups(0).valid #= true
      dut.ups(0).payload #= 5
      dut.ups(1).valid #= true
      dut.ups(1).payload #= 7
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 15)
      assert(!dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)

      dut.down.ready #= true
      sleep(1)
      assert(dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      dut.ups(0).valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 27)
      assert(dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  test("hierarchicalMergeBypass") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val EXTRA = Payload(UInt(8 bits))
      val ups = Vec.fill(2)(slave Stream(UInt(8 bits)))
      val down = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val joined = new pipe.MergeLayer(List("up0", "up1"), "sink")
      val up0 = new pipe.StageCtrlLayer("up0")
      val up1 = new pipe.StageCtrlLayer("up1")
      val sink = new pipe.StageCtrlLayer("sink")

      new joined.Source(0) {
        bypass(VALUE, EXTRA)
      }
      new joined.Source(1) {
        bypass(VALUE, EXTRA)
      }

      up0.ctrl(0).up.driveFrom(ups(0)) { (self, payload) =>
        self(VALUE) := payload
        self(EXTRA) := payload + 1
      }
      up1.ctrl(0).up.driveFrom(ups(1)) { (self, payload) =>
        self(VALUE) := payload
        self(EXTRA) := payload + 2
      }
      sink.ctrl(0).down.driveTo(down)((payload, self) => payload := self(VALUE) + self(EXTRA))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.down.ready #= true

      dut.ups(0).valid #= true
      dut.ups(0).payload #= 5
      dut.ups(1).valid #= true
      dut.ups(1).payload #= 7
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 11)
      assert(dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      dut.ups(0).valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 16)
      assert(dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  class HierarchicalNodeMergeDut extends Component {
    val VALUE = Payload(UInt(8 bits))
    val ups = Vec.fill(2)(slave Stream(UInt(8 bits)))
    val down = master Stream(UInt(8 bits))

    val pipe = new HierarchicalPipeLine()
    val joined = new pipe.MergeLayer(List("up0", "up1"), "sink")
    val up0 = new pipe.StageLayer("up0")
    val up1 = new pipe.StageLayer("up1")
    val sink = new pipe.StageLayer("sink")

    new joined.Source(0) {
      down(VALUE) := up(VALUE) + 10
    }
    new joined.Source(1) {
      down(VALUE) := up(VALUE) + 20
    }

    up0(0).driveFrom(ups(0))((self, payload) => self(VALUE) := payload)
    up1(0).driveFrom(ups(1))((self, payload) => self(VALUE) := payload)
    sink(0).driveTo(down)((payload, self) => payload := self(VALUE))

    pipe.build()
  }

  test("hierarchicalNodeMerge") {
    SimConfig.compile(new HierarchicalNodeMergeDut).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.ups.foreach { up =>
        up.valid #= false
        up.payload #= 0
      }
      dut.down.ready #= false
      cd.waitSampling()

      dut.ups(0).valid #= true
      dut.ups(0).payload #= 5
      dut.ups(1).valid #= true
      dut.ups(1).payload #= 7
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 15)
      assert(!dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)

      dut.down.ready #= true
      sleep(1)
      assert(dut.ups(0).ready.toBoolean)
      assert(!dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      dut.ups(0).valid #= false
      sleep(1)
      assert(dut.down.valid.toBoolean)
      assert(dut.down.payload.toInt == 27)
      assert(dut.ups(1).ready.toBoolean)
      cd.waitSampling()

      simSuccess()
    }
  }

  test("hierarchicalSingleCtrlChain") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val down = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val layer = new pipe.StageCtrlLayer("pipe")
      layer.ctrl(0).up.driveFrom(up)((self, payload) => self(VALUE) := payload)
      layer.ctrl(2).down.driveTo(down)((payload, self) => payload := self(VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.down.ready #= false
      cd.waitSampling()
      pushAndExpect(cd, dut.up, dut.down, 42, 42)
      simSuccess()
    }
  }

  test("hierarchicalSingleNodeChain") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val down = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val layer = new pipe.StageLayer("pipe")
      layer.node(0).driveFrom(up)((self, payload) => self(VALUE) := payload)
      layer.node(2).driveTo(down)((payload, self) => payload := self(VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.down.ready #= false
      cd.waitSampling()
      pushAndExpect(cd, dut.up, dut.down, 37, 37)
      simSuccess()
    }
  }

  test("hierarchicalOneToOneBranchAndMerge") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val branchUp = slave Stream(UInt(8 bits))
      val branchDown = master Stream(UInt(8 bits))
      val mergeUp = slave Stream(UInt(8 bits))
      val mergeDown = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val split = new pipe.BranchLayer("srcCtrl", List("dstNode"))
      val joined = new pipe.MergeLayer(List("srcNode"), "dstCtrl")
      val srcCtrl = new pipe.StageCtrlLayer("srcCtrl")
      val dstNode = new pipe.StageLayer("dstNode")
      val srcNode = new pipe.StageLayer("srcNode")
      val dstCtrl = new pipe.StageCtrlLayer("dstCtrl")

      new split.Target(0) {
        bypass(VALUE)
      }
      new joined.Source(0) {
        bypass(VALUE)
      }

      srcCtrl.ctrl(0).up.driveFrom(branchUp)((self, payload) => self(VALUE) := payload)
      dstNode.node(0).driveTo(branchDown)((payload, self) => payload := self(VALUE))
      srcNode.node(0).driveFrom(mergeUp)((self, payload) => self(VALUE) := payload)
      dstCtrl.ctrl(0).down.driveTo(mergeDown)((payload, self) => payload := self(VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.branchUp.valid #= false
      dut.branchDown.ready #= false
      dut.mergeUp.valid #= false
      dut.mergeDown.ready #= false
      cd.waitSampling()

      pushAndExpect(cd, dut.branchUp, dut.branchDown, 9, 9)
      pushAndExpect(cd, dut.mergeUp, dut.mergeDown, 11, 11)
      simSuccess()
    }
  }

  test("hierarchicalBranchThenMergeAcyclicMixed") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val down = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val split = new pipe.BranchLayer("source", List("left", "right"))
      val joined = new pipe.MergeLayer(List("left", "right"), "sink")
      val source = new pipe.StageCtrlLayer("source")
      val left = new pipe.StageLayer("left")
      val right = new pipe.StageCtrlLayer("right")
      val sink = new pipe.StageLayer("sink")

      new split.Target(0) {
        override def selected: Bool = !up(VALUE)(0)
        down(VALUE) := up(VALUE) + 10
      }
      new split.Target(1) {
        override def selected: Bool = up(VALUE)(0)
        down(VALUE) := up(VALUE) + 20
      }
      new joined.Source(0) {
        down(VALUE) := up(VALUE) + 100
      }
      new joined.Source(1) {
        down(VALUE) := up(VALUE) + 200
      }

      source.ctrl(0).up.driveFrom(up)((self, payload) => self(VALUE) := payload)
      left.node(0)
      right.ctrl(0)
      sink.node(0).driveTo(down)((payload, self) => payload := self(VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.down.ready #= false
      cd.waitSampling()

      pushAndExpect(cd, dut.up, dut.down, 4, 114)
      pushAndExpect(cd, dut.up, dut.down, 5, 225)
      simSuccess()
    }
  }

  test("hierarchicalMergeThenBranchCyclicCtrl") {
    SimConfig.compile(new Component {
      val VALUE = Payload(UInt(8 bits))
      val up = slave Stream(UInt(8 bits))
      val down = master Stream(UInt(8 bits))

      val pipe = new HierarchicalPipeLine()
      val joined = new pipe.MergeLayer(List("feedback", "source"), "work")
      val split = new pipe.BranchLayer("work", List("sink", "feedback"))
      val source = new pipe.StageCtrlLayer("source")
      val feedback = new pipe.StageCtrlLayer("feedback")
      val work = new pipe.StageCtrlLayer("work")
      val sink = new pipe.StageCtrlLayer("sink")

      new joined.Source(0) {
        bypass(VALUE)
      }
      new joined.Source(1) {
        bypass(VALUE)
      }
      new split.Target(0) {
        override def selected: Bool = up(VALUE) > 5
        bypass(VALUE)
      }
      new split.Target(1) {
        override def selected: Bool = up(VALUE) <= 5
        down(VALUE) := up(VALUE) + 1
      }

      source.ctrl(0).up.driveFrom(up)((self, payload) => self(VALUE) := payload)
      feedback.ctrl(0).ignoreReadyWhen(True)
      feedback.ctrl(1)
      work.ctrl(0)
      sink.ctrl(0).down.driveTo(down)((payload, self) => payload := self(VALUE))

      Seq(
        source.ctrl(0).up,
        source.ctrl(0).down,
        joined.ups(0),
        joined.ups(1),
        joined.down,
        work.ctrl(0).up,
        work.ctrl(0).down,
        split.up,
        split.downs(0),
        split.downs(1),
        feedback.ctrl(0).up,
        feedback.ctrl(0).down,
        feedback.ctrl(1).up,
        feedback.ctrl(1).down,
        sink.ctrl(0).up,
        sink.ctrl(0).down
      ).foreach(requestPayloadAndValid(_, VALUE))

      pipe.build()
    }).doSimUntilVoid { dut =>
      val cd = dut.clockDomain
      cd.forkStimulus(10)
      dut.up.valid #= false
      dut.down.ready #= false
      cd.waitSampling()

      pushAndExpect(cd, dut.up, dut.down, 2, 6)
      pushAndExpect(cd, dut.up, dut.down, 7, 7)
      simSuccess()
    }
  }
}

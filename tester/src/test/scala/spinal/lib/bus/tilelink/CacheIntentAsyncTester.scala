package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.coherent.{Cache, CacheParam}
import spinal.lib.bus.tilelink.sim.{Block, IdAllocator, IdCallback, MasterAgent, MemoryAgent, MonitorSubscriber, TransactionA, TransactionB, TransactionC, TransactionD}
import spinal.lib.sim.SparseMemory

import scala.collection.mutable.ArrayBuffer

class CacheIntentAsyncTester extends AnyFunSuite {
  private def cacheParam(): CacheParam = {
    val transfers = M2sTransfers(
      acquireT = SizeRange(64),
      acquireB = SizeRange(64),
      get = SizeRange.upTo(64),
      putFull = SizeRange.upTo(64),
      putPartial = SizeRange.upTo(64),
      hint = SizeRange(64)
    )
    val unp = NodeParameters(
      m = M2sParameters(
        addressWidth = 32,
        dataWidth = 64,
        masters = List(M2sAgent(
          name = null,
          mapping = List(M2sSource(SizeMapping(0, 8), transfers))
        ))
      ),
      s = S2mParameters(List(S2mAgent(
        name = null,
        sinkId = SizeMapping(0, 8),
        emits = S2mTransfers(probe = SizeRange(64))
      )))
    )
    CacheParam(
      unp = unp,
      downPendingMax = 8,
      cacheWays = 2,
      cacheBytes = 1024,
      blockSize = 64,
      withIntent = true,
      aBufferCount = 2,
      generalSlotCount = 8,
      generalSlotCountUpCOnly = 0,
      probeCount = 2,
      coherentRegion = _ => True,
      allocateOnMiss = (_, _, _, _, _) => True
    )
  }

  private class CacheIntentDut(param: CacheParam) extends Component {
    val cache = new Cache(param)
    val io = new Bundle {
      val up = slave(Bus(param.unp.toBusParameter()))
      val down = master(Bus(cache.io.down.p))
    }

    cache.io.up << io.up
    cache.io.down >> io.down

    cache.gs.slots.foreach { slot =>
      slot.valid.simPublic()
      slot.pending.flush.simPublic()
      slot.pending.maintenanceReady.simPublic()
      slot.cmoOrigin.simPublic()
    }
    cache.IntentCmoBackend.selectedOh.simPublic()
  }

  private lazy val compiled = {
    DebugId.setup(16)
    SimConfig.compile(new CacheIntentDut(cacheParam()))
  }

  private class PendingIntent(agent: MasterAgent, val source: Int, val address: Long, param: Int) {
    require(agent.callbackOnAtoD(source) == null, s"source $source already has a pending A request")

    private val debugId = agent.allocateDebugId()
    var response: TransactionD = null

    agent.callbackOnAtoD(source) = { d =>
      response = d
      agent.callbackOnAtoD(source) = null
      agent.freeDebugId(debugId)
    }

    private val request = TransactionA()
    request.opcode = Opcode.A.INTENT
    request.param = param
    request.source = source
    request.address = address
    request.size = log2Up(64)
    request.mask = Array.fill(64)(true)
    request.data = null
    request.corrupt = false
    request.debugId = debugId
    agent.driver.scheduleA(request)

    def done: Boolean = response != null
  }

  private def waitUntil(cd: ClockDomain, limit: Int, clue: String)(condition: => Boolean): Unit = {
    var cycles = 0
    while(!condition && cycles < limit) {
      cd.waitSampling()
      cycles += 1
    }
    assert(condition, s"Timed out after $limit cycles waiting for $clue")
  }

  private def assertNoResponse(cd: ClockDomain, request: PendingIntent, cycles: Int): Unit = {
    for(_ <- 0 until cycles) {
      cd.waitSampling()
      assert(!request.done, "CMO returned HintAck before its maintenance work completed")
    }
  }

  private def assertHintAck(d: TransactionD, source: Int): Unit = {
    assert(d != null)
    assert(d.opcode == Opcode.D.HINT_ACK)
    assert(d.source == source)
    assert(d.size == log2Up(64))
    assert(d.param == 0)
    assert(!d.denied)
    assert(!d.corrupt)
  }

  private def assertHintAck(request: PendingIntent, source: Int): Unit = {
    assertHintAck(request.response, source)
  }

  private case class TestContext(agent: MasterAgent,
                                 memoryAgent: MemoryAgent,
                                 memory: SparseMemory,
                                 probes: ArrayBuffer[TransactionB],
                                 probeResponses: ArrayBuffer[TransactionC],
                                 downstreamRequests: ArrayBuffer[TransactionA],
                                 upstreamRequests: ArrayBuffer[TransactionA],
                                 upstreamResponses: ArrayBuffer[TransactionD])

  private def setup(dut: CacheIntentDut)(implicit idAllocator: IdAllocator, idCallback: IdCallback): TestContext = {
    val memory = SparseMemory(seed = 42)
    val memoryAgent = new MemoryAgent(dut.io.down, dut.clockDomain, memArg = Some(memory))
    val agent = new MasterAgent(dut.io.up, dut.clockDomain)
    val probes = ArrayBuffer[TransactionB]()
    val probeResponses = ArrayBuffer[TransactionC]()
    val downstreamRequests = ArrayBuffer[TransactionA]()
    val upstreamRequests = ArrayBuffer[TransactionA]()
    val upstreamResponses = ArrayBuffer[TransactionD]()

    memoryAgent.driver.driver.noStall()
    agent.driver.driver.noStall()
    agent.monitor.add(new MonitorSubscriber {
      override def onA(a: TransactionA): Unit = upstreamRequests += a
      override def onB(b: TransactionB): Unit = probes += b
      override def onC(c: TransactionC): Unit = probeResponses += c
      override def onD(d: TransactionD): Unit = upstreamResponses += d
    })
    memoryAgent.monitor.add(new MonitorSubscriber {
      override def onA(a: TransactionA): Unit = downstreamRequests += a
    })

    TestContext(agent, memoryAgent, memory, probes, probeResponses, downstreamRequests, upstreamRequests, upstreamResponses)
  }

  private def makeDirtyOwner(context: TestContext, address: Long, value: Byte): (Block, Array[Byte]) = {
    val backing = context.memory.readBytes(address, 64)
    val block = context.agent.acquireBlock(0, Param.Grow.NtoT, address, 64)
    block.data(0) = value
    block.dirty = true
    context.probes.clear()
    context.probeResponses.clear()
    context.downstreamRequests.clear()
    block -> backing
  }

  private def assertNoWriteback(context: TestContext, address: Long): Unit = {
    assert(!context.downstreamRequests.exists(a =>
      a.address == address &&
        (a.opcode == Opcode.A.PUT_FULL_DATA || a.opcode == Opcode.A.PUT_PARTIAL_DATA)
    ))
  }

  test("CBOInval discards a cache-local dirty line without writeback") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val address = 0x200L
      val backing = context.memory.readBytes(address, 64)
      val dirty = backing.clone()
      dirty(0) = (dirty(0) ^ 0x55).toByte

      val put = context.agent.putFullData(0, address, dirty)
      assert(!put.denied)
      assert(context.memory.readBytes(address, 64).sameElements(backing))
      context.downstreamRequests.clear()

      assertHintAck(context.agent.cboInval(1, address, 64), 1)
      assert(context.memory.readBytes(address, 64).sameElements(backing))
      assertNoWriteback(context, address)

      context.downstreamRequests.clear()
      val get = context.agent.get(0, address, 64)
      assert(get.data.sameElements(backing))
      assert(context.downstreamRequests.exists(a => a.address == address && a.opcode == Opcode.A.GET))
    }
  }

  test("CBOInval removes a clean cache hit") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val address = 0x280L
      val backing = context.memory.readBytes(address, 64)

      assert(context.agent.get(0, address, 64).data.sameElements(backing))
      context.downstreamRequests.clear()

      assertHintAck(context.agent.cboInval(1, address, 64), 1)
      assert(context.memory.readBytes(address, 64).sameElements(backing))
      assertNoWriteback(context, address)

      context.downstreamRequests.clear()
      assert(context.agent.get(0, address, 64).data.sameElements(backing))
      assert(context.downstreamRequests.exists(a => a.address == address && a.opcode == Opcode.A.GET))
    }
  }

  test("CBOInval removes a clean owner without writeback") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val address = 0x300L
      val backing = context.memory.readBytes(address, 64)
      val block = context.agent.acquireBlock(0, Param.Grow.NtoT, address, 64)
      block.dirty = false
      context.downstreamRequests.clear()

      assertHintAck(context.agent.cboInval(1, address, 64), 1)
      assert(block.cap == Param.Cap.toN)
      assert(context.memory.readBytes(address, 64).sameElements(backing))
      assertNoWriteback(context, address)
    }
  }

  test("CBOInval waits for dirty owner ProbeAckData drain") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val address = 0x100L
      val (block, backing) = makeDirtyOwner(context, address, 0x35.toByte)

      val cDriver = context.agent.driver.driver.c.ctrl.driver
      context.agent.driver.driver.c.ctrl.driver = (_: ChannelC) => false

      val request = new PendingIntent(context.agent, 1, address, Param.Intent.CBO_INVAL)
      waitUntil(dut.clockDomain, 200, "CBOInval Probe-to-N") {
        context.probes.exists(b => b.address == address && b.param == Param.Cap.toN)
      }
      assertNoResponse(dut.clockDomain, request, 20)
      assert(context.probeResponses.isEmpty)

      context.agent.driver.driver.c.ctrl.driver = cDriver
      waitUntil(dut.clockDomain, 200, "CBOInval ProbeAckData") {
        context.probeResponses.exists(c => c.address == address && c.opcode == Opcode.C.PROBE_ACK_DATA)
      }
      waitUntil(dut.clockDomain, 200, "CBOInval HintAck") { request.done }

      assertHintAck(request, 1)
      assert(block.cap == Param.Cap.toN)
      assert(context.memory.readBytes(address, 64).sameElements(backing))
      assert(context.downstreamRequests.isEmpty)
    }
  }

  test("CBOClean waits for dirty owner drain and writeback completion") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val address = 0x180L
      val (block, backing) = makeDirtyOwner(context, address, 0x5a.toByte)
      val expected = block.data.clone()
      assert(!expected.sameElements(backing))

      val cDriver = context.agent.driver.driver.c.ctrl.driver
      val downDDriver = context.memoryAgent.driver.driver.d.ctrl.driver
      context.agent.driver.driver.c.ctrl.driver = (_: ChannelC) => false
      context.memoryAgent.driver.driver.d.ctrl.driver = (_: ChannelD) => false

      val request = new PendingIntent(context.agent, 1, address, Param.Intent.CBO_CLEAN)
      waitUntil(dut.clockDomain, 200, "CBOClean Probe-to-B") {
        context.probes.exists(b => b.address == address && b.param == Param.Cap.toB)
      }
      assertNoResponse(dut.clockDomain, request, 20)
      assert(context.probeResponses.isEmpty)

      context.agent.driver.driver.c.ctrl.driver = cDriver
      waitUntil(dut.clockDomain, 200, "CBOClean ProbeAckData") {
        context.probeResponses.exists(c => c.address == address && c.opcode == Opcode.C.PROBE_ACK_DATA)
      }
      waitUntil(dut.clockDomain, 200, "CBOClean downstream writeback") {
        context.downstreamRequests.exists(a => a.address == address && a.opcode == Opcode.A.PUT_FULL_DATA)
      }
      waitUntil(dut.clockDomain, 200, "CBOClean backing memory update") {
        context.memory.readBytes(address, 64).sameElements(expected)
      }
      assertNoResponse(dut.clockDomain, request, 20)

      context.memoryAgent.driver.driver.d.ctrl.driver = downDDriver
      waitUntil(dut.clockDomain, 200, "CBOClean HintAck") { request.done }

      assertHintAck(request, 1)
      assert(block.cap == Param.Cap.toB)
      assert(!block.dirty)
      assert(context.memory.readBytes(address, 64).sameElements(expected))
    }
  }

  test("CMO response selection wraps and does not starve older ready slots") {
    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val context = setup(dut)
      val readySlots = cacheParam().generalSlotCount - cacheParam().generalSlotCountUpCOnly
      val rounds = 3

      context.agent.driver.driver.d.factor = 0.0f
      waitUntil(dut.clockDomain, 20, "upstream D backpressure") {
        !dut.io.up.d.ready.toBoolean
      }

      def issue(source: Int, round: Int): PendingIntent = {
        val address = 0x400L + (round * readySlots + source) * 64L
        val request = new PendingIntent(context.agent, source, address, Param.Intent.CBO_FLUSH)
        waitUntil(dut.clockDomain, 100, f"Intent A source=$source address=0x$address%x") {
          context.upstreamRequests.exists(a => a.source == source && a.address == address)
        }
        request
      }

      def readyCmoCount: Int = dut.cache.gs.slots.count { slot =>
        slot.valid.toBoolean &&
          slot.pending.flush.toBoolean &&
          slot.cmoOrigin.toBoolean &&
          slot.pending.maintenanceReady.toBoolean
      }

      def releaseOne(request: PendingIntent): Unit = {
        waitUntil(dut.clockDomain, 100, s"selected CMO source ${request.source}") {
          dut.io.up.d.valid.toBoolean
        }
        assert(dut.io.up.d.opcode.toEnum == Opcode.D.HINT_ACK)
        assert(dut.io.up.d.source.toInt == request.source)
        assert(dut.cache.IntentCmoBackend.selectedOh.toBigInt == (BigInt(1) << request.source))
        assert(!request.done)

        val responsesBefore = context.upstreamResponses.count(_.opcode == Opcode.D.HINT_ACK)
        context.agent.driver.driver.d.factor = 1.0f
        waitUntil(dut.clockDomain, 20, s"CMO HintAck source ${request.source}") { request.done }
        context.agent.driver.driver.d.factor = 0.0f
        waitUntil(dut.clockDomain, 20, "upstream D backpressure restoration") {
          !dut.io.up.d.ready.toBoolean
        }

        assertHintAck(request, request.source)
        assert(request.response.address == request.address)
        assert(context.upstreamResponses.count(_.opcode == Opcode.D.HINT_ACK) == responsesBefore + 1)
      }

      var active = Array.tabulate(readySlots)(source => issue(source, 0))
      waitUntil(dut.clockDomain, 200, s"all $readySlots A-reachable GeneralSlots ready") {
        readyCmoCount == readySlots
      }

      for(round <- 0 until rounds; source <- 0 until readySlots) {
        val current = active(source)
        assert(current.address == 0x400L + (round * readySlots + source) * 64L)
        releaseOne(current)

        if(round != rounds - 1) {
          active(source) = issue(source, round + 1)
          waitUntil(dut.clockDomain, 200, s"all $readySlots CMO slots ready after refill") {
            readyCmoCount == readySlots
          }
        }

        for(olderSource <- source + 1 until readySlots) {
          assert(!active(olderSource).done, s"source $olderSource completed out of round-robin order")
        }
      }

      assert(active.forall(_.done))
      assert(context.upstreamResponses.count(_.opcode == Opcode.D.HINT_ACK) == readySlots * rounds)
    }
  }
}

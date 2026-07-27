package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.coherent.{Cache, CacheParam}
import spinal.lib.bus.tilelink.sim.{IdAllocator, IdCallback, MemoryAgent, MonitorSubscriber, TransactionA, TransactionB, TransactionC, TransactionD}
import spinal.lib.sim.SparseMemory

import scala.collection.mutable.ArrayBuffer

class CacheIntentTester extends AnyFunSuite {
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
          mapping = List(M2sSource(SizeMapping(0, 4), transfers))
        ))
      ),
      s = S2mParameters(List(S2mAgent(
        name = null,
        sinkId = SizeMapping(0, 4),
        emits = S2mTransfers(probe = SizeRange(64))
      )))
    )
    CacheParam(
      unp = unp,
      downPendingMax = 4,
      cacheWays = 2,
      cacheBytes = 256,
      blockSize = 64,
      withIntent = true,
      aBufferCount = 2,
      generalSlotCount = 4,
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
  }

  test("Cache terminates supported Intent and rejects reserved params locally") {
    DebugId.setup(16)
    val param = cacheParam()
    val compiled = SimConfig.compile(new CacheIntentDut(param))

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val memory = SparseMemory(seed = 42)
      val downAgent = new MemoryAgent(dut.io.down, dut.clockDomain, memArg = Some(memory))
      downAgent.driver.driver.noStall()
      val agent = new spinal.lib.bus.tilelink.sim.MasterAgent(dut.io.up, dut.clockDomain)
      val downA = ArrayBuffer[TransactionA]()
      val upB = ArrayBuffer[TransactionB]()
      val upC = ArrayBuffer[TransactionC]()

      downAgent.monitor.add(new MonitorSubscriber {
        override def onA(a: TransactionA): Unit = downA += a
      })
      agent.monitor.add(new MonitorSubscriber {
        override def onB(b: TransactionB): Unit = upB += b
        override def onC(c: TransactionC): Unit = upC += c
      })

      val expectations = Seq(
        Param.Intent.PREFETCH_READ -> false,
        Param.Intent.PREFETCH_WRITE -> false,
        2 -> true,
        3 -> true,
        4 -> true,
        Param.Intent.CBO_INVAL -> false,
        Param.Intent.CBO_CLEAN -> false,
        Param.Intent.CBO_FLUSH -> false
      )
      val responses = expectations.zipWithIndex.map { case ((intentParam, denied), index) =>
        val a = TransactionA()
        a.opcode = Opcode.A.INTENT
        a.param = intentParam
        a.source = 0
        a.address = index * 64
        a.size = 6
        a.mask = Array.fill(64)(true)
        a.data = null
        a.corrupt = false
        a.debugId = agent.allocateDebugId()
        agent.driver.scheduleA(a)
        val response = agent.waitAtoD(0)
        assert(response.opcode == Opcode.D.HINT_ACK)
        assert(response.source == 0)
        assert(response.size == 6)
        assert(response.param == 0)
        assert(response.denied == denied)
        assert(!response.corrupt)
        agent.freeDebugId(a.debugId)
        response
      }

      assert(responses.size == expectations.size)
      assert(downA.isEmpty)
      assert(upB.isEmpty)
      assert(upC.isEmpty)

      val before = memory.readBytes(0, 8)
      val getResponse = agent.get(0, 0, 8)
      assert(getResponse.data.sameElements(before))
      assert(downA.nonEmpty)
      assert(upB.isEmpty)
      assert(upC.isEmpty)
    }
  }

  test("Cache keeps CMO HintAck stable under D backpressure") {
    DebugId.setup(16)
    val param = cacheParam()
    val compiled = SimConfig.compile(new CacheIntentDut(param))

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      implicit val idCallback = new IdCallback
      val downAgent = new MemoryAgent(dut.io.down, dut.clockDomain)
      downAgent.driver.driver.noStall()
      val agent = new spinal.lib.bus.tilelink.sim.MasterAgent(dut.io.up, dut.clockDomain)
      agent.driver.driver.noStall()
      val requests = ArrayBuffer[TransactionA]()
      agent.monitor.add(new MonitorSubscriber {
        override def onA(a: TransactionA): Unit = requests += a
      })

      agent.cboFlush(0, 0x00, 64)

      agent.driver.driver.d.factor = 0.0f
      dut.clockDomain.waitSamplingWhere(!dut.io.up.d.ready.toBoolean)

      var firstResponse: TransactionD = null
      val firstWaiter = fork {
        firstResponse = agent.cboFlush(1, 0x40, 64)
      }
      dut.clockDomain.waitSamplingWhere(dut.io.up.d.valid.toBoolean)

      def dPayload = (
        dut.io.up.d.opcode.toEnum,
        dut.io.up.d.param.toInt,
        dut.io.up.d.source.toInt,
        dut.io.up.d.sink.toInt,
        dut.io.up.d.size.toInt,
        dut.io.up.d.denied.toBoolean,
        dut.io.up.d.corrupt.toBoolean
      )

      val heldPayload = dPayload
      assert(heldPayload._1 == Opcode.D.HINT_ACK)
      assert(heldPayload._3 == 1)

      var secondResponse: TransactionD = null
      val secondWaiter = fork {
        secondResponse = agent.cboFlush(2, 0x80, 64)
      }
      dut.clockDomain.waitSamplingWhere(requests.count(_.opcode == Opcode.A.INTENT) == 3)

      for(_ <- 0 until 20) {
        dut.clockDomain.waitSampling()
        assert(dut.io.up.d.valid.toBoolean)
        assert(dPayload == heldPayload)
      }

      agent.driver.driver.d.factor = 1.0f
      firstWaiter.join()
      secondWaiter.join()

      assert(firstResponse.source == 1)
      assert(secondResponse.source == 2)
      assert(firstResponse.opcode == Opcode.D.HINT_ACK)
      assert(secondResponse.opcode == Opcode.D.HINT_ACK)
    }
  }
}

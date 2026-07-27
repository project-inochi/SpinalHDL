package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.sim.{IdAllocator, MasterAgent, MonitorSubscriber, TransactionA}

import scala.collection.mutable.ArrayBuffer

class IntentMasterAgentTester extends AnyFunSuite {
  private class IntentMasterDut(p: BusParameter) extends Component {
    val bus = slave(Bus(p))
    val pending = RegInit(False)
    val source = Reg(UInt(p.sourceWidth bits))
    val size = Reg(UInt(p.sizeWidth bits))

    bus.a.ready := !pending
    bus.d.valid := pending
    bus.d.opcode := Opcode.D.HINT_ACK
    bus.d.param := 0
    bus.d.source := source
    bus.d.size := size
    bus.d.sink := 0
    bus.d.denied := False
    bus.d.corrupt := False
    bus.d.data.assignDontCare()

    when(bus.a.fire) {
      source := bus.a.source
      size := bus.a.size
      pending := True
    }
    when(bus.d.fire) {
      pending := False
    }
  }

  private def intentBusParameter(): BusParameter = {
    val transfers = M2sTransfers(
      get = SizeRange(1, 4),
      hint = SizeRange(64)
    )
    M2sParameters(
      addressWidth = 10,
      dataWidth = 32,
      masters = List(M2sAgent(
        name = null,
        mapping = List(M2sSource(SizeMapping(0, 4), transfers))
      ))
    ).toNodeParameters().toBusParameter()
  }

  test("MasterAgent generates Intent and completes on HintAck") {
    DebugId.setup(16)
    val p = intentBusParameter()
    val compiled = SimConfig.compile(new IntentMasterDut(p))

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      val agent = new MasterAgent(dut.bus, dut.clockDomain)
      val seen = ArrayBuffer[TransactionA]()
      agent.monitor.add(new MonitorSubscriber {
        override def onA(a: TransactionA): Unit = seen += a
      })

      val params = Seq(
        Param.Intent.PREFETCH_READ,
        Param.Intent.PREFETCH_WRITE,
        Param.Intent.CBO_INVAL,
        Param.Intent.CBO_CLEAN,
        Param.Intent.CBO_FLUSH
      )
      val responses = params.zipWithIndex.map { case (param, index) =>
        agent.intent(0, index * 64, 64, param)
      }
      assert(responses.forall(_.opcode == Opcode.D.HINT_ACK))
      assert(responses.forall(_.bytes == 64))
      assert(seen.map(_.opcode) == Seq.fill(5)(Opcode.A.INTENT))
      assert(seen.map(_.param) == params)
      assert(seen.map(_.address) == (0 until 5).map(_ * 64).map(BigInt(_)))
      assert(seen.forall(_.size == 6))
      assert(seen.forall(_.bytes == 64))
      assert(seen.forall(_.mask.length == p.dataBytes))
      assert(seen.forall(_.mask.forall(identity)))
      assert(seen.forall(!_.withData))
      assert(seen.forall(!_.corrupt))
    }
  }

  test("MasterAgent wrappers encode the five standard Intent parameters") {
    DebugId.setup(16)
    val p = intentBusParameter()
    val compiled = SimConfig.compile(new IntentMasterDut(p))

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      implicit val idAllocator = new IdAllocator(DebugId.width)
      val agent = new MasterAgent(dut.bus, dut.clockDomain)
      val responses = Seq(
        agent.prefetchRead(0, 0x000, 64),
        agent.prefetchWrite(0, 0x040, 64),
        agent.cboInval(0, 0x080, 64),
        agent.cboClean(0, 0x0c0, 64),
        agent.cboFlush(0, 0x100, 64)
      )
      assert(responses.map(_.opcode).forall(_ == Opcode.D.HINT_ACK))
    }
  }

}

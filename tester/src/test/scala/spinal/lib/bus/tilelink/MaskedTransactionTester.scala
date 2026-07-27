package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.sim.{TransactionA, TransactionAggregator}
import spinal.lib._
import scala.reflect.ClassTag

class MaskedTransactionTester extends AnyFunSuite {
  private def busParameter(): BusParameter = {
    val transfers = M2sTransfers(putFull = SizeRange(1, 64))
    val master = M2sAgent(
      name = null,
      mapping = List(M2sSource(
        id = SizeMapping(0, 1),
        emits = transfers
      ))
    )
    M2sParameters(
      addressWidth = 8,
      dataWidth = 32,
      masters = List(master)
    ).toNodeParameters().toBusParameter()
  }

  private class MaskedNoDataA extends TransactionA {
    override def withData = false
    override def withMask = true

    override def copyNoData()(implicit evidence: ClassTag[TransactionA]): TransactionA = {
      val ret = new MaskedNoDataA
      ret.copyNoDataFrom(this)
      ret.opcode = opcode
      ret.debugId = debugId
      ret
    }
  }

  test("masked non-data transactions round-trip mask and remain single beat") {
    val p = busParameter()
    def request(size: Int, mask: Array[Boolean]) = {
      val ret = new MaskedNoDataA
      ret.opcode = Opcode.A.PUT_FULL_DATA
      ret.source = 0
      ret.address = 0
      ret.size = size
      ret.mask = mask
      ret.corrupt = false
      ret.debugId = 0
      ret
    }

    val cases = Seq(
      request(2, Array(true, false, true, false)) -> Array(true, false, true, false),
      request(6, Array.fill(64)(true)) -> Array.fill(p.dataBytes)(true)
    )

    val compiled = SimConfig.compile(new Component {
      val a = slave Stream(ChannelA(p))
      a.ready := True
    })

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      for((request, physicalMask) <- cases) {
        val beats = request.serialize(p.dataBytes)
        assert(beats.length == 1)
        assert(beats.head.mask sameElements physicalMask)
        assert(beats.head.data == null)

        beats.head.write(dut.a.payload)
        dut.clockDomain.waitSampling()
        val readBack = new MaskedNoDataA
        readBack.read(dut.a.payload)
        assert(readBack.mask sameElements physicalMask)
        assert(readBack.data == null)
        assert(!readBack.corrupt)

        var aggregated: MaskedNoDataA = null
        val aggregator = new TransactionAggregator[TransactionA](p.dataBytes)(t => aggregated = t.asInstanceOf[MaskedNoDataA])
        aggregator.push(readBack)
        assert(aggregated != null)
        assert(aggregated.bytes == request.bytes)
        assert(aggregated.mask sameElements physicalMask)
        assert(aggregated.data == null)
        assert(aggregator.beat == 0)
      }
    }
  }
}

package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.fiber.Fiber
import spinal.core.sim.SimConfig
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.coherent.CacheFiber
import spinal.lib.bus.tilelink.fabric.{MasterBus, SlaveBus}
import spinal.lib.system.tag.PMA

class CacheFiberIntentCapabilityTester extends AnyFunSuite {
  private def compileCache(withIntent: Boolean, expectedHint: SizeRange): Unit = {
    SimConfig.compile(new Component {
      val master = new MasterBus(
        M2sParameters(
          addressWidth = 32,
          dataWidth = 64,
          masters = List(M2sAgent(
            name = null,
            mapping = List(M2sSource(
              id = SizeMapping(0, 4),
              emits = M2sTransfers(
                acquireT = SizeRange(64),
                acquireB = SizeRange(64),
                get = SizeRange.upTo(64),
                putFull = SizeRange.upTo(64),
                putPartial = SizeRange.upTo(64)
              )
            ))
          ))
        )
      )

      val cache = new CacheFiber()
      cache.parameter.withIntent = withIntent
      cache.up << master.node

      val slave = new SlaveBus(
        M2sSupport(
          transfers = M2sTransfers.all,
          dataWidth = 64,
          addressWidth = 32
        ),
        S2mParameters.none()
      )
      slave.node.addTag(PMA.MAIN)
      slave.node at 0 of cache.down

      Fiber check {
        assert(cache.parameter.blockSize == 64)
        assert(cache.up.m2s.supported.transfers.hint == expectedHint)
        assert(cache.down.m2s.proposed.transfers.hint == SizeRange.none)
        assert(cache.down.m2s.parameters.emits.hint == SizeRange.none)

        assert(cache.up.m2s.supported.transfers.acquireT == SizeRange(64))
        assert(cache.up.m2s.supported.transfers.acquireB == SizeRange(64))
        assert(cache.up.m2s.supported.transfers.get == SizeRange.upTo(64))
        assert(cache.up.m2s.supported.transfers.putFull == SizeRange.upTo(64))
        assert(cache.up.m2s.supported.transfers.putPartial == SizeRange.upTo(64))
      }
    })
  }

  test("CacheFiber does not advertise Intent by default") {
    compileCache(withIntent = false, expectedHint = SizeRange.none)
  }

  test("CacheFiber advertises only block-sized Intent when enabled") {
    compileCache(withIntent = true, expectedHint = SizeRange(64))
  }
}

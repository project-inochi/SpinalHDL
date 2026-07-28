package spinal.lib.bus.tilelink

import spinal.core._
import spinal.core.sim._
import spinal.lib.bus.tilelink
import tilelink._
import tilelink.fabric.sim._
import org.scalatest.funsuite.AnyFunSuite
import spinal.core.Component
import spinal.lib.slave
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.coherent.{CacheFiber, CacheParam, FlushBus, FlushParam, SelfFLush}
import spinal.lib.bus.tilelink.fabric.{MasterBus, SlaveBus}
import spinal.lib.bus.tilelink.sim.{Block, MasterAgent, MasterDebugTester, MasterDebugTesterElement, MasterTester, TransactionD}
import spinal.lib.system.tag.PMA
import spinal.sim.SimThread

import scala.collection.mutable.ArrayBuffer

class CacheTester extends AnyFunSuite{
  object CmoOp {
    val CLEAN = 0x1
    val FLUSH = 0x2
  }

  case class CmoCompatible(
    clean     : Boolean,
    flush     : Boolean,
  )

  def doTest(cp : CacheParam => Unit, flushParam : FlushParam = null, withIntent : Boolean = false): Unit = {
    val tester = new TilelinkTester(
      simConfig = SimConfig,
      cGen = new Component {
        val m0 = new MasterBus(
          M2sParameters(
            addressWidth = 32,
            dataWidth = 64,
            masters = List.tabulate(4)(mid => M2sAgent(
              name = null,
              mapping = List(M2sSource(
                id = SizeMapping(mid * 4, 4),
                emits = M2sTransfers(
                  acquireT = SizeRange(64),
                  acquireB = SizeRange(64),
                  get = SizeRange(1, 64),
                  putFull = SizeRange(1, 64),
                  putPartial = SizeRange(1, 64),
                  hint = if(withIntent) SizeRange(64) else SizeRange.none
                )
              ))
            ))
          )
        )


        val ctrl = new MasterBus(
          M2sParameters(
            addressWidth = 32,
            dataWidth = 32,
            masters = List.tabulate(4)(mid => M2sAgent(
              name = null,
              mapping = List(M2sSource(
                id = SizeMapping(mid, 1),
                emits = M2sTransfers(
                  get = SizeRange(4),
                  putFull = SizeRange(4)
                )
              ))
            ))
          )
        )
        ctrl.node.addTag(tilelinkTesterExcluded)

        val directory = new CacheFiber(withCtrl = true, flushBusParam = flushParam)
        directory.parameter.cacheWays = 4
        directory.parameter.cacheBytes = 4096
        directory.parameter.allocateOnMiss = (op, src, addr, size, param) => addr(6)
//        directory.parameter.selfFlush = SelfFLush(0x10000, 0x10000+0x400, 2000)
        directory.parameter.flushCompletionsCount = 4
        directory.parameter.withIntent = withIntent
        cp(directory.parameter)
        directory.up << m0.node
        directory.ctrl at (0x00, 0x100) of ctrl.node

        val flush = directory.withFlushBus generate slave(FlushBus(directory.parameter.flushBusParam))
        if(directory.withFlushBus) {
          directory.flush.cmd << flush.cmd
          flush.rsp << directory.flush.rsp
        }

        val ctrlInterrupt = out Bool()
        ctrlInterrupt := directory.interrupt.flag

        val s0 = new SlaveBus(
          M2sSupport(
            transfers = M2sTransfers.all,
            dataWidth = 64,
            addressWidth = 13
          ),
          S2mParameters.none()
        )
        s0.node at 0x10000 of directory.down
        s0.node.addTag(PMA.MAIN)

        val s1 = new SlaveBus(
          M2sSupport(
            transfers = M2sTransfers.all,
            dataWidth = 64,
            addressWidth = 10
          ),
          S2mParameters.none()
        )
        s1.node at 0x20000 of directory.down
      }
    )

    //    tester.noStall = true //for test only


    def doFlush(ctrl : MasterAgent, sourceId : Int, base : Int, size : Int, interrupt : Bool = null): Unit = {
      while(ctrl.getInt(sourceId, 0x08) != 0){ } // Reserve the flush hardware
      ctrl.putInt(sourceId, 0x10, base);
      ctrl.putInt(sourceId, 0x18, base + size - 1);
      ctrl.putInt(sourceId, 0x08, 3 | (sourceId << 8)); // Start the flush with completion ID = sourceId
      if(interrupt != null){
        ctrl.putInt(sourceId, 0x38, 1); //enable the flush idle interrupt
        ctrl.cd.waitSamplingWhere(interrupt.toBoolean) // Wait for the interrupt
        ctrl.putInt(sourceId, 0x38, 0);
      } else {
        while ((ctrl.getInt(sourceId, 0x00) & (1 << sourceId)) == 0) { // Wait until the sourceId completion register is high
          ctrl.cd.waitSampling(simRandom.nextInt(50))
        }
      }
    }

    def initFlushBus(flush : FlushBus): Unit = {
      flush.cmd.valid #= false
      flush.cmd.address #= 0
      flush.cmd.source #= 0
      flush.rsp.ready #= true
    }

    def doDirected(name : String)(body : MasterDebugTester => Unit): Unit = {
      tester.doSim(name) { tb =>
        if(flushParam != null) initFlushBus(tb.dut.flush)
        val directed = new MasterDebugTester(
          (tb.masterSpecs, tb.mastersStuff).zipped.map((s, t) => new MasterDebugTesterElement(s, t.agent))
        )
        body(directed)
      }
    }

    def cmoCheck[T <: Component](tb: TilelinkTestbenchBase[T], m0: MasterAgent, compatible: CmoCompatible, doCmo: (Int, Int, Int, Boolean) => Unit) = {
      val base = 0x10000

      if(compatible.flush) {
        for(address <- List(base, base + 0x40)) {
          val offset = address - 0x10000
          var block: Block = null

          // Dirty
          m0.putInt(0, address, 0x1)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) != 0x1)
          doCmo(0, address, CmoOp.FLUSH, false)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x1)

          // Clean
          m0.getInt(0, address)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x1)
          doCmo(0, address, CmoOp.FLUSH, false)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x1)

          // Clean probe
          block = m0.acquireBlock(0, Param.Grow.NtoT, address, 0x40)
          doCmo(0, address, CmoOp.FLUSH, true)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x1)

          // Clean probe2
          block = m0.acquireBlock(0, Param.Grow.NtoT, address, 0x40)
          m0.release(0, Param.Cap.toB, block)
          doCmo(0, address, CmoOp.FLUSH, true)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x1)

          // Dirty probe
          block = m0.acquireBlock(0, Param.Grow.NtoT, address, 0x40)
          block.data(0) = 0x02
          block.dirty = true
          doCmo(0, address, CmoOp.FLUSH, true)
          assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x2)

          assert(m0.getInt(0, address) == 0x2)
        }
      }

      if(compatible.clean) {
        val address = base + 0xc0
        val offset = address - 0x10000
        var block: Block = null

        // WC-CLEAN
        m0.putInt(0, address, 0x21)
        doCmo(0, address, CmoOp.CLEAN, false)
        assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x21)
        assert(m0.getInt(0, address) == 0x21)

        // CLEAN
        val cleanValue = m0.getInt(0, address)
        doCmo(0, address, CmoOp.CLEAN, false)
        assert(m0.getInt(0, address) == cleanValue)

        // Permission
        block = m0.acquireBlock(0, Param.Grow.NtoT, address, 0x40)
        block.dirty = false
        doCmo(0, address, CmoOp.CLEAN, false)
        assert(block.cap == Param.Cap.toB)
        assert(!block.dirty)

        // WC-CLEAN
        block = m0.acquireBlock(0, Param.Grow.NtoT, address, 0x40)
        block.data(0) = 0x22
        block.dirty = true
        doCmo(0, address, CmoOp.CLEAN, false)
        assert(tb.slavesStuff(0).model.mem.readInt(offset) == 0x22)
        assert(block.cap == Param.Cap.toB)
        assert(!block.dirty)
        assert(m0.getInt(0, address) == 0x22)
      }
    }

    if(withIntent) {
      tester.doSim("IntentPrefetch") { tb =>
        val m0 = tb.mastersStuff(0).agent
        tb.mastersStuff.foreach(_.agent.driver.driver.noStall())
        tb.slavesStuff.foreach(_.model.driver.driver.noStall())
        m0.prefetchRead(0, 0x10400, 64)
        m0.prefetchWrite(0, 0x10400, 64)
        tb.waitCheckers()
      }

      tester.doSim("CMOIntent") { tb =>
        val m0 = tb.mastersStuff(0).agent
        tb.mastersStuff.foreach(_.agent.driver.driver.noStall())
        tb.slavesStuff.foreach(_.model.driver.driver.noStall())
        def doCmo(sourceId: Int, address: Int, op: Int, needInterrupt: Boolean): Unit = {
          val response = op match {
            case CmoOp.CLEAN      => m0.cboClean(sourceId, address, 64)
            case CmoOp.FLUSH      => m0.cboFlush(sourceId, address, 64)
            case _ => ??? // No INVAL support
          }
          assert(!response.denied)
        }

        val compatible = CmoCompatible(
          clean      = true,
          flush      = true,
        )
        cmoCheck(tb, m0, compatible, doCmo)
        tb.waitCheckers()
      }
    }

    tester.doSim("manual") { tb =>
      disableSimWave()

      if(flushParam != null) initFlushBus(tb.dut.flush)

      periodically(10000) {
        tb.mastersStuff.foreach(_.agent.driver.driver.randomizeStallRate())
        tb.slavesStuff.foreach(_.model.driver.driver.randomizeStallRate())
      }


      val ctrl = new MasterAgent(tb.dut.ctrl.node.bus, tb.dut.ctrl.node.clockDomain)(tb.idAllocator)
      ctrl.bus.p.node.m.masters.indices.foreach(sourceId =>  fork {
        val cd = tb.dut.ctrl.node.clockDomain
        while(true) {
          cd.waitSampling(simRandom.nextInt(2000))
          val (base, size) = simRandom.nextInt(3) match{
            case 0 => (simRandom.nextInt(1000), simRandom.nextInt(1024))
            case 1 => (0x10000 + simRandom.nextInt(8192), simRandom.nextInt(1024))
            case 2 => (0x20000 + simRandom.nextInt(1024), simRandom.nextInt(1024))
          }
          doFlush(ctrl, sourceId, base, size)
        }
      })

      //      delayed(2147898761l-1000000)(enableSimWave())
      val testers = (tb.masterSpecs, tb.mastersStuff).zipped.map((s, t) => new MasterTester(s, t.agent))
      //      val globalLock = Some(SimMutex()) //for test only
      val globalLock = Option.empty[SimMutex]
      testers.foreach(_.startPerSource(10000, globalLock))
      testers.foreach(_.join())
      tb.waitCheckers()
      tb.assertCoverage()
    }

    //    tester.doSim("manual") { tb =>
    //      val agent = tb.mastersStuff(0).agent
    //      agent.
    //      for(i <- 64 until 4096 by 128) {
    //        agent.putFullData(0, 0x10000, Array.fill(16)(simRandom.nextInt.toByte))
    //      }
    //      tb.waitCheckers()
    //      tb.assertCoverage()
    //    }

    //    tester.doSimDirected("manual"){tb =>
    //      tb.coverAcquirePerm(32)
    //    }
    //
    //    tester.doSim("manual2"){tb =>
    //      val agent = tb.mastersStuff.head.agent
    //      val threads = ArrayBuffer[SimThread]()
    //      def doFork(body : => Unit) = threads += fork(body)
    //      def doJoin() = threads.foreach(_.join())
    //      def doBlock(name : String)(body : => Unit): Unit = { println(s"test $name"); body; doJoin(); agent.cd.waitSampling(20) }
    //
    //      for(address <- List(0x10000, 0x10040)) {
    //        doBlock("multiGet") {
    //          for (i <- 0 to 7) doFork(agent.get(i, address, 16))
    //        }
    //        doBlock("multiGetPut") {
    //          for (i <- 0 to 3) doFork(agent.get(i, address, 16))
    //          for (i <- 4 to 8) doFork(agent.putPartialData(i, address, Array.fill(16)(simRandom.nextInt.toByte), Array.fill(16)(simRandom.nextBoolean())))
    //        }
    //      }
    //    }
    //


    tester.doSim("flush") { tb =>
      val ctrl = new MasterAgent(tb.dut.ctrl.node.bus, tb.dut.ctrl.node.clockDomain)(tb.idAllocator)
      if(flushParam != null) initFlushBus(tb.dut.flush)
      val m0 = tb.mastersStuff(0).agent

      def doFlushWithCtrl(sourceId : Int, address : Int, op: Int, needInterrupt: Boolean): Unit = doFlush(ctrl, sourceId, address, 0x40, if (needInterrupt) tb.dut.ctrlInterrupt else null)

      val compatible = CmoCompatible(
        clean      = false,
        flush      = true,
      )
      cmoCheck(tb, m0, compatible, doFlushWithCtrl)
    }

    if(flushParam != null) tester.doSim("flushBus") { tb =>
      initFlushBus(tb.dut.flush)
      val flush = tb.dut.flush
      val m0 = tb.mastersStuff(0).agent

      def doFlushWithBus(sourceId : Int, address : Int, op: Int, needInterrupt: Boolean): Unit = {
        flush.cmd.valid #= true
        flush.cmd.address #= address
        flush.cmd.source #= sourceId
        m0.cd.waitSamplingWhere(flush.cmd.ready.toBoolean)
        flush.cmd.valid #= false
        m0.cd.waitSamplingWhere(flush.rsp.valid.toBoolean && flush.rsp.source.toBigInt == sourceId)
        m0.cd.waitSampling()
      }

      val compatible = CmoCompatible(
        clean      = false,
        flush      = true,
      )
      cmoCheck(tb, m0, compatible, doFlushWithBus)

      tb.waitCheckers()
    }

    doDirected("get"){_.coverGet(32)}
    doDirected("putFull") {_.coverPutFullData(32)}
    doDirected("putPartial") {_.coverPutPartialData(32)}
    doDirected("acquireB")(_.coverAcquireB(32))
    doDirected("acquireT")(_.coverAcquireT(32))
    doDirected("acquireBT")(_.coverAcquireBT(32))
    doDirected("acquireTB")(_.coverAcquireTB(32))
    doDirected("acquirePerm")(_.coverAcquirePerm(32))
    doDirected("coherencyBx2")(_.coverCoherencyBx2(32))
    doDirected("coherencyTx2")(_.coverCoherencyTx2(32))
    doDirected("coherencyT_B")(_.coverCoherencyT_B(32))
    doDirected("coherencyBx2_T_Bx2")(_.coverCoherencyBx2_T_Bx2(32))



    tester.checkErrors()
  }

  val cps = ArrayBuffer[(String, CacheParam => Unit)](
    "dp 1bank"          -> {p => },
    "dp 1bank non-pow2" -> {p => p.generalSlotCount = 9},
    "dp 2bank"          -> {p => p.cacheBanks = 2},
    "sp 1bank"          -> {p => p.withDualPortRam = false},
    "sp 2bank"          -> {p => p.withDualPortRam = false; p.cacheBanks = 2},
    "sp 4bank"          -> {p => p.withDualPortRam = false; p.cacheBanks = 4}
  )

  for ((name, cp) <- cps) {
    test(name) {
      doTest(cp, null)
    }

    test(name + " flush") {
      doTest(cp, FlushParam(32, 2))
    }
  }

  test("intent") {
    doTest({p => }, null, withIntent = true)
  }
}

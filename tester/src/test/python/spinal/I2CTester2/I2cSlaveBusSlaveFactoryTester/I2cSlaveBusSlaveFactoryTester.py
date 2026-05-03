import cocotb
from cocotb.decorators import coroutine

from cocotblib.misc import ClockDomainAsyncReset, SimulationTimeout, simulationSpeedPrinter, assertEquals
from spinal.I2CTester2.lib.misc import OpenDrainInterconnect, I2cSoftMaster


SLAVE_ADDRESS = 0x42


@coroutine
def send_slave_address(master, read):
    value = (SLAVE_ADDRESS << 1) | (1 if read else 0)
    yield master.sendByteCheck(value, value)
    yield master.sendBitCheck(True, False)


@coroutine
def write_bytes(master, register, values):
    yield master.sendStart()
    yield send_slave_address(master, read=False)
    yield master.sendByteCheck(register, register)
    yield master.sendBitCheck(True, False)
    for value in values:
        yield master.sendByteCheck(value, value)
        yield master.sendBitCheck(True, False)
    yield master.sendStop()


@coroutine
def read_bytes(master, register, count, result):
    yield master.sendStart()
    yield send_slave_address(master, read=False)
    yield master.sendByteCheck(register, register)
    yield master.sendBitCheck(True, False)
    yield master.sendRestart()
    yield send_slave_address(master, read=True)
    for index in range(count):
        buffer = [0]
        yield master.sendByte(0xFF, buffer)
        result.append(buffer[0])
        last = index == count - 1
        yield master.sendBitCheck(last, last)
    yield master.sendStop()


@coroutine
def read_current(master, count, result):
    yield master.sendStart()
    yield send_slave_address(master, read=True)
    for index in range(count):
        buffer = [0]
        yield master.sendByte(0xFF, buffer)
        result.append(buffer[0])
        last = index == count - 1
        yield master.sendBitCheck(last, last)
    yield master.sendStop()


@cocotb.test()
def test1(dut):
    cocotb.fork(ClockDomainAsyncReset(dut.clk, dut.reset, 100000))
    cocotb.fork(simulationSpeedPrinter(dut.clk))
    cocotb.fork(SimulationTimeout(2000 * 2.5e6))

    sclInterconnect = OpenDrainInterconnect()
    sclInterconnect.addHardDriver(dut.io_i2c_scl_write)
    sclInterconnect.addHardReader(dut.io_i2c_scl_read)

    sdaInterconnect = OpenDrainInterconnect()
    sdaInterconnect.addHardDriver(dut.io_i2c_sda_write)
    sdaInterconnect.addHardReader(dut.io_i2c_sda_read)

    master = I2cSoftMaster(
        sclInterconnect.newSoftConnection(),
        sdaInterconnect.newSoftConnection(),
        2500000,
        dut.clk
    )

    yield master.wait(10)

    data = []
    yield read_bytes(master, 0x00, 1, data)
    assertEquals(data, [0x12], "reg8 reset value mismatch")

    yield write_bytes(master, 0x00, [0x33])
    data = []
    yield read_bytes(master, 0x00, 1, data)
    assertEquals(data, [0x33], "reg8 write mismatch")

    data = []
    yield read_bytes(master, 0x40, 1, data)
    assertEquals(data, [0x01], "write event counter mismatch")

    data = []
    yield read_bytes(master, 0x10, 2, data)
    assertEquals(data, [0x56, 0x34], "reg16 reset value mismatch")

    yield write_bytes(master, 0x10, [0x78, 0x56])
    data = []
    yield read_bytes(master, 0x10, 2, data)
    assertEquals(data, [0x78, 0x56], "reg16 write mismatch")

    data = []
    yield read_bytes(master, 0x20, 4, data)
    assertEquals(data, [0xEF, 0xCD, 0xAB, 0x89], "reg32 reset value mismatch")

    yield write_bytes(master, 0x20, [0x10, 0x20, 0x30, 0x40])
    data = []
    yield read_bytes(master, 0x20, 4, data)
    assertEquals(data, [0x10, 0x20, 0x30, 0x40], "reg32 write mismatch")

    data = []
    yield read_current(master, 1, data)
    assertEquals(data, [0x66], "address pointer retention mismatch")

    data = []
    yield read_bytes(master, 0x30, 1, data)
    assertEquals(data, [0x5A], "read-only register mismatch")

    data = []
    yield read_bytes(master, 0x41, 1, data)
    assertEquals(data, [0x01], "read event counter mismatch")

    yield write_bytes(master, 0x30, [0x00])
    data = []
    yield read_bytes(master, 0x30, 1, data)
    assertEquals(data, [0x5A], "read-only register changed after write")

    data = []
    yield read_bytes(master, 0x31, 1, data)
    assertEquals(data, [0xA5], "clear-on-read initial value mismatch")
    data = []
    yield read_bytes(master, 0x31, 1, data)
    assertEquals(data, [0x00], "clear-on-read did not clear")

    data = []
    yield read_bytes(master, 0x32, 1, data)
    assertEquals(data, [0x0F], "clear-on-set initial value mismatch")
    yield write_bytes(master, 0x32, [0x05])
    data = []
    yield read_bytes(master, 0x32, 1, data)
    assertEquals(data, [0x0A], "clear-on-set behavior mismatch")

    data = []
    yield read_bytes(master, 0xF0, 1, data)
    assertEquals(data, [0x00], "unmapped read should return zero")

    yield write_bytes(master, 0xF0, [0x99])

    data = []
    yield read_bytes(master, 0x50, 1, data)
    assertEquals(data, [0x3C], "delayed read mismatch")

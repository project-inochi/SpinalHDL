include ../../common/Makefile.def

ifeq ($(TOPLEVEL_LANG),verilog)
	VERILOG_SOURCES += $(SPINALROOT)/I2cSlaveBusSlaveFactoryTester.v
	TOPLEVEL=I2cSlaveBusSlaveFactoryTester
endif

ifeq ($(TOPLEVEL_LANG),vhdl)
	VHDL_SOURCES += $(SPINALROOT)/I2cSlaveBusSlaveFactoryTester.vhd
	TOPLEVEL=i2cslavebusslavefactorytester
endif

MODULE=I2cSlaveBusSlaveFactoryTester

include ../../common/Makefile.sim

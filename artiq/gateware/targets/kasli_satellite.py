#!/usr/bin/env python3

import argparse

from migen import *
from migen.genlib.resetsync import AsyncResetSynchronizer
from migen.genlib.cdc import MultiReg
from migen.genlib.io import DifferentialOutput

from misoc.interconnect.csr import *
from misoc.cores import gpio
from misoc.cores.a7_gtp import *
from misoc.targets.kasli import (
    BaseSoC, MiniSoC, soc_kasli_args, soc_kasli_argdict)
from misoc.integration.builder import builder_args, builder_argdict

from artiq.gateware.amp import AMPSoC
from artiq.gateware import rtio
from artiq.gateware.rtio.phy import ttl_simple, ttl_serdes_7series, edge_counter
from artiq.gateware import eem
from artiq.gateware.drtio.transceiver import gtp_7series
from artiq.gateware.drtio.siphaser import SiPhaser7Series
from artiq.gateware.drtio.wrpll import WRPLL, DDMTDSamplerGTP
from artiq.gateware.drtio.rx_synchronizer import XilinxRXSynchronizer
from artiq.gateware.drtio import *
from artiq.build_soc import *
from artiq.gateware.targets.kasli import (MasterBase,SatelliteBase)


#class Satellite(SatelliteBase):
 #   def __init__(self, hw_rev=None, **kwargs):
  #      if hw_rev is None:
   #         hw_rev = "v2.0"
    #    SatelliteBase.__init__(self, hw_rev=hw_rev, rtio_clk_freq=125e6, **kwargs)

     #   platform = self.platform

      #  try:
       #     self.comb += platform_request("clk_sel").eq(1)
        #except ConstraintError:
         #   pass

        #self.rtio_channels = []

        #eem.DIO.add_std(self, 0,
         #   ttl_serdes_7series.InOut_8X, ttl_serdes_7series.Output_8X, edge_counter_cls=SimpleEdgeCounter)
        #eem.SUServo.add_std(self, eems_urukul=[[3, 4], [5, 6]], eems_sampler=[1, 2])    #eem4 and eem5 are blank, there is no second urukul.



        #self.config["HAS_RTIO_LOG"] = None
        #self.config["RTIO_LOG_CHANNEL"] = len(self.rtio_channels)
        #self.rtio_channels.append(rtio.LogChannel())

        #self.add_rtio(self.rtio_channels)



class Satellite(SatelliteBase):
    def __init__(self, hw_rev=None, **kwargs):
        if hw_rev is None:
            hw_rev = "v2.0"
        SatelliteBase.__init__(self, rtio_clk_freq=125e6, hw_rev=hw_rev, **kwargs)

        self.rtio_channels = []
        phy = ttl_simple.Output(self.platform.request("user_led", 0))
        self.submodules += phy
        self.rtio_channels.append(rtio.Channel.from_phy(phy))
        # matches Tester EEM numbers
        eem.DIO.add_std(self, 0,
            ttl_serdes_7series.InOut_8X, ttl_serdes_7series.Output_8X)
        eem.Urukul.add_std(self, 1, 2, ttl_serdes_7series.Output_8X, ttl_simple.ClockGen)
        eem.Urukul.add_std(self, 3, 4, ttl_serdes_7series.Output_8X, ttl_simple.ClockGen)
        eem.SUServo.add_std(self, eems_urukul=[[5, 6],[7, 8]], eems_sampler=[10, 11])    #eem10 and eem11 are blank, there is no second urukul for the suservo.
        # eem.SUServo.add_std(self, eems_urukul=[[2, 3], [4, 5]], eems_sampler=[0, 1])

        self.config["HAS_RTIO_LOG"] = None
        self.config["RTIO_LOG_CHANNEL"] = len(self.rtio_channels)
        self.config["SI5324_EXT_REF"] = None
        self.config["EXT_REF_FREQUENCY"] = "125.0"
        self.rtio_channels.append(rtio.LogChannel())

        self.add_rtio(self.rtio_channels)

VARIANTS = {cls.__name__.lower(): cls for cls in [Satellite]}


def main():
    parser = argparse.ArgumentParser(
        description="ARTIQ device binary builder for Kasli systems")
    builder_args(parser)
    soc_kasli_args(parser)
    parser.set_defaults(output_dir="artiq_kasli")
    parser.add_argument("-V", "--variant", default="satellite",
                        help="variant: {} (default: %(default)s)".format(
                            "/".join(sorted(VARIANTS.keys()))))
    parser.add_argument("--with-wrpll", default=False, action="store_true")
    parser.add_argument("--gateware-identifier-str", default=None,
                        help="Override ROM identifier")
    args = parser.parse_args()

    argdict = dict()
    if args.with_wrpll:
        argdict["with_wrpll"] = True
    argdict["gateware_identifier_str"] = args.gateware_identifier_str

    variant = args.variant.lower()
    try:
        cls = VARIANTS[variant]
    except KeyError:
        raise SystemExit("Invalid variant (-V/--variant)")

    soc = cls(**soc_kasli_argdict(args), **argdict)
    build_artiq_soc(soc, builder_argdict(args))


if __name__ == "__main__":
    main()

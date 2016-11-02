package cws.core.vmtypeselection;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class FastestVmTypeSelectionUnitTest {
    private VmTypeSelectionStrategy vmTypeSelectionStrategy;
    private final double FASTEST_VMTYPE_MIPS = 10;
    private final int FASTEST_VMTYPE_CORES = 10;
    private final double FASTEST_VMTYPE_PRICE = 10;

    @Before
    public void setUp() {
        vmTypeSelectionStrategy = new FastestVmTypeSelection();
    }

    @Test
    public void shouldSelectFastestAmongSetPassedAsParams() {
        //given
        Set<VMType> vmTypes = new HashSet<VMType>();
        vmTypes.add(VMTypeBuilder.newBuilder().mips(1).cores(1).price(1).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(2).cores(1).price(1).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(2).cores(2).price(1).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(FASTEST_VMTYPE_MIPS).cores(FASTEST_VMTYPE_CORES).price(FASTEST_VMTYPE_PRICE).build());

        //when
        VMType actualFastest = vmTypeSelectionStrategy.selectVmType(vmTypes);

        //then
        assertTrue(actualFastest.getCores() == FASTEST_VMTYPE_CORES);
        assertTrue(actualFastest.getMips() == FASTEST_VMTYPE_MIPS);
        assertTrue(actualFastest.getPriceForBillingUnit() == FASTEST_VMTYPE_PRICE);
    }
}

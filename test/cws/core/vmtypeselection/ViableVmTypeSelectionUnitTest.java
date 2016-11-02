package cws.core.vmtypeselection;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class ViableVmTypeSelectionUnitTest {
    private VmTypeSelectionStrategy vmTypeSelectionStrategy;
    private final double VIABLE_VMTYPE_MIPS = 10;
    private final int VIABLE_VMTYPE_CORES = 10;
    private final double VIABLE_VMTYPE_PRICE = 1;

    @Before
    public void setUp() {
        vmTypeSelectionStrategy = new ViableVmTypeSelection();
    }

    @Test
    public void shouldSelectMostViableAmongSetPassedAsParams() {
        //given
        Set<VMType> vmTypes = new HashSet<VMType>();
        vmTypes.add(VMTypeBuilder.newBuilder().mips(1).cores(1).price(10).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(2).cores(1).price(10).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(2).cores(2).price(10).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(VIABLE_VMTYPE_MIPS).cores(VIABLE_VMTYPE_CORES).price(VIABLE_VMTYPE_PRICE).build());

        //when
        VMType viableVmType = vmTypeSelectionStrategy.selectVmType(vmTypes);

        //then
        assertTrue(viableVmType.getCores() == VIABLE_VMTYPE_CORES);
        assertTrue(viableVmType.getMips() == VIABLE_VMTYPE_MIPS);
        assertTrue(viableVmType.getPriceForBillingUnit() == VIABLE_VMTYPE_PRICE);
    }
}

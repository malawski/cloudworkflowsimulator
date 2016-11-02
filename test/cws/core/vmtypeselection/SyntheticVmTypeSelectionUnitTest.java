package cws.core.vmtypeselection;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class SyntheticVmTypeSelectionUnitTest {
    private VmTypeSelectionStrategy vmTypeSelectionStrategy;
    private final double SYNTHETIC_VMTYPE_MIPS = 5;
    private final int SYNTHETIC_VMTYPE_CORES = 5;
    private final double SYNTHETIC_VMTYPE_PRICE = 5;

    @Before
    public void setUp() {
        vmTypeSelectionStrategy = new SyntheticVmTypeSelection();
    }

    @Test
    public void shouldSelectSyntheticAmongSetPassedAsParams() {
        //given
        Set<VMType> vmTypes = new HashSet<VMType>();
        vmTypes.add(VMTypeBuilder.newBuilder().mips(10).cores(10).price(10).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(1).cores(1).price(3).build());
        vmTypes.add(VMTypeBuilder.newBuilder().mips(SYNTHETIC_VMTYPE_MIPS).cores(SYNTHETIC_VMTYPE_CORES).price(SYNTHETIC_VMTYPE_PRICE).build());

        //when
        VMType actualSynthetic = vmTypeSelectionStrategy.selectVmType(vmTypes);

        //then
        assertTrue(actualSynthetic.getCores() == 1); // always 1 in synthetic
        assertTrue(actualSynthetic.getMips() == 42); // 42 = (10*10+1*1+5*5)/3
        assertTrue(actualSynthetic.getPriceForBillingUnit() == 6); // 6 = (10+5+3)/3
    }

}

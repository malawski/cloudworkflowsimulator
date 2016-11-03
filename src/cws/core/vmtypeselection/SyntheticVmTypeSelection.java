package cws.core.vmtypeselection;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;

import java.util.Set;

/**
 * VmTypeSelectionStrategy that returns new vm type created with mean params along all vm types from the set passed as param.
 */
public class SyntheticVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        double meanMips = 0, meanBillingPrice = 0;
        for (VMType vmType : vmTypes) {
            meanMips += vmType.getMips() * vmType.getCores();
            meanBillingPrice += vmType.getPriceForBillingUnit();
        }
        meanMips /= vmTypes.size();
        meanBillingPrice /= vmTypes.size();
        return VMTypeBuilder.newBuilder().mips(meanMips).cores(1).price(meanBillingPrice).build();
    }

    @Override
    public String toString() {
        return "SyntheticVmTypeSelection";
    }
}

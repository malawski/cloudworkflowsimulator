package cws.core;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;

import java.util.Set;

public class ViableVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        VMType mostViableVmTypeYet = vmTypes.iterator().next();
        for(VMType currentVmType : vmTypes) {
            if(currentVmType.getPriceForBillingUnit() < mostViableVmTypeYet.getPriceForBillingUnit()) {
                mostViableVmTypeYet = currentVmType;
            }
        }
        return mostViableVmTypeYet;
    }

    @Override
    public String toString() {
        return "ViableVmTypeSelection";
    }
}

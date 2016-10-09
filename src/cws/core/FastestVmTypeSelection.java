package cws.core;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;

import java.util.Set;

public class FastestVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        VMType fastestYet = vmTypes.iterator().next();
        for(VMType currentVmType : vmTypes) {
            if(currentVmType.getMips() > fastestYet.getMips()) {
                fastestYet = currentVmType;
            }
        }
        return fastestYet;
    }
}

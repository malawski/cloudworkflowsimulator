package cws.core.vmtypeselection;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;

import java.util.Set;

public class FastestVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        VMType fastestYet = vmTypes.iterator().next();
        double scoreYet = fastestYet.getCores()*fastestYet.getMips();
        for(VMType currentVmType : vmTypes) {
            double currentScore = currentVmType.getCores()*currentVmType.getMips();
            if(currentScore > scoreYet) {
                fastestYet = currentVmType;
                scoreYet = currentScore;
            }
        }
        return fastestYet;
    }

    @Override
    public String toString() {
        return "FastestVmTypeSelection";
    }
}

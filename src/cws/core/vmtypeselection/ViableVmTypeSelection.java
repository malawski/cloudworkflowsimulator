package cws.core.vmtypeselection;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;

import java.util.Set;

/**
 * VmTypeSelectionStrategy that returns most viable vm type along set passed as param.
 */
public class ViableVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        VMType mostViableVmTypeYet = vmTypes.iterator().next();
        double mostViableScoreYet = mostViableVmTypeYet.getCores() * mostViableVmTypeYet.getMips() / mostViableVmTypeYet.getPriceForBillingUnit();
        for (VMType currentVmType : vmTypes) {
            double currentScore = currentVmType.getCores() * currentVmType.getMips() / currentVmType.getPriceForBillingUnit();
            if (currentScore > mostViableScoreYet) {
                mostViableVmTypeYet = currentVmType;
                mostViableScoreYet = currentScore;
            }
        }
        return mostViableVmTypeYet;
    }

    @Override
    public String toString() {
        return "ViableVmTypeSelection";
    }
}

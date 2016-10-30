package cws.core;

import com.google.common.base.Preconditions;
import cws.core.core.VMType;

import java.util.Set;

public class SyntheticVmTypeSelection implements VmTypeSelectionStrategy {
    @Override
    public VMType selectVmType(Set<VMType> vmTypes) {
        Preconditions.checkArgument(!vmTypes.isEmpty());
        return vmTypes.iterator().next();
    }

    @Override
    public String toString() {
        return "SyntheticVmTypeSelection";
    }
}

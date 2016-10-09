package cws.core;

import cws.core.core.VMType;

import java.util.Set;

public interface VmTypeSelectionStrategy {
    VMType selectVmType(Set<VMType> vmTypes);
}

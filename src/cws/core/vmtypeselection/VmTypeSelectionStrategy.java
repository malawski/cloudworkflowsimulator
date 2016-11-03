package cws.core.vmtypeselection;

import cws.core.core.VMType;

import java.util.Set;

/**
 * Service which helps in selecting one representative vm type for a whole set.
 */
public interface VmTypeSelectionStrategy {
    VMType selectVmType(Set<VMType> vmTypes);
}

package knoelab.classification.init;

/**
 * Refer "Pushing the EL Envelope" technical report for description of
 * the completion rules represented here.
 * @author raghava
 *
 */
public enum AxiomDistributionType {

	// CR1_1 is of type1. A < B axioms
	CR_TYPE1_1,
	// A1 ^ A2 ^ ... ^ An < B axioms belong to this type
	CR_TYPE1_2,
	// A < 3r.B axioms are of this type
	CR_TYPE2,
	// CR4 is of type3_1 and type3_2. 3r.A < B
	CR_TYPE3_1,
	CR_TYPE3_2,
	// CR5 and CR6 are of type4, type5. r < s and r o s < t
	CR_TYPE4,
	CR_TYPE5,
	// this is CR5 in the tech report.
	CR_TYPE_BOTTOM,
	// this is CR6 and represents the squiggly arrow rule.
//	CR_TYPE_SQUIG_ARROW,
	// this type is used to represent String IDs for concepts/roles
	CONCEPT_ID,
	// this represents the node to which the results should go
	RESULT_TYPE;
}

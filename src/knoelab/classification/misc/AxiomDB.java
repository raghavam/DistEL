package knoelab.classification.misc;

/**
 * While storing axioms in key value store,
 * role axioms are stored in DB-1 and rest of
 * the axioms in DB-0
 * 
 * @author Raghava
 *
 */
public enum AxiomDB {
	// DB used to store non role axioms
	NON_ROLE_DB(0),
	// DB used to store role axioms
	ROLE_DB(1),
	// DB used to store the scores
	SCORE_DB(2),
	// DB used to store the index of conjunct axioms
	CONJUNCT_INDEX_DB(3),
	DB4(4);
	
	private int dbIndex;
	
	private AxiomDB(int dbIndex) {
		this.dbIndex = dbIndex;
	}
	
	public int getDBIndex() {
		return dbIndex;
	}
}

package knoelab.classification.init;

/**
 * 
 * @author Raghava
 *
 */
public enum EntityType {
	CLASS(0),
	INDIVIDUAL(1),
	ROLE(2),
	DATATYPE(3);
	
	private int typeID;
	
	private EntityType(int typeID) {
		this.typeID = typeID;
	}
	
	public int getTypeID() {
		return typeID;
	}
	
	public static EntityType getEntityType(int typeID) throws Exception {
		switch(typeID) {
			case 0: 
				return CLASS;
			case 1:
				return INDIVIDUAL;
			case 2:
				return ROLE;
			case 3:
				return DATATYPE;
			default:
				throw new Exception("Unknown type: " + typeID);
		}
	}
}

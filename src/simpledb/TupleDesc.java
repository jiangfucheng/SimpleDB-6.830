package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	private List<TDItem> tdItems;
	private boolean fieldIsNull = false;


	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return An tupleIterator which iterates over all the field TDItems
	 * that are included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		return tdItems.iterator();
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		this.tdItems = new ArrayList<>();
		if (fieldAr == null) fieldIsNull = true;
		// some code goes here
		for (int i = 0; i < typeAr.length; i++) {
			TDItem tdItem;
			if (fieldIsNull) {
				tdItem = new TDItem(typeAr[i], null);
			} else {
				tdItem = new TDItem(typeAr[i], fieldAr[i]);
			}
			tdItems.add(tdItem);
		}
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		this(typeAr, null);
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return tdItems.size();
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		if (i < 0 || i >= tdItems.size()) throw new NoSuchElementException();
		return tdItems.get(i).fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i The index of the field to get the type of. It must be a valid
	 *          index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		if (i < 0 || i >= tdItems.size()) throw new NoSuchElementException();
		return tdItems.get(i).fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		if (fieldIsNull) throw new NoSuchElementException("field name is null");
		for (int i = 0; i < tdItems.size(); i++) {
			if (tdItems.get(i).fieldName.equals(name)) {
				return i;
			}
		}
		throw new NoSuchElementException("no such element");
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 * Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int res = 0;
		for (TDItem item : tdItems) {
			res += item.fieldType.getLen();
		}
		return res;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 *
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		int size = td1.numFields() + td2.numFields();
		Type[] typeArr = new Type[size];
		String[] fieldNameArr = new String[size];
		int idx = 0;
		Iterator<TDItem> iterator = td1.iterator();
		while (iterator.hasNext()) {
			TDItem item = iterator.next();
			typeArr[idx] = item.fieldType;
			fieldNameArr[idx++] = item.fieldName;
		}
		iterator = td2.iterator();
		while (iterator.hasNext()) {
			TDItem item = iterator.next();
			typeArr[idx] = item.fieldType;
			fieldNameArr[idx++] = item.fieldName;
		}
		return new TupleDesc(typeArr, fieldNameArr);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they have the same number of items
	 * and if the i-th type in this TupleDesc is equal to the i-th type in o
	 * for every i.
	 *
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */

	public boolean equals(Object o) {
		if (o == null) return false;
		if(o.getClass() != this.getClass()) return false;
		TupleDesc other = (TupleDesc) o;
		if (other == this) return true;
		if (this.numFields() != other.numFields()) return false;
		List<TDItem> otherItems = other.tdItems;
		for (int i = 0; i < tdItems.size(); i++) {
			if(!this.tdItems.get(i).fieldType.equals(otherItems.get(i).fieldType)){
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 *
	 * @return String describing this descriptor.
	 */
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(TDItem item : tdItems){
			String type = "";
			if(item.fieldType.equals(Type.INT_TYPE)){
				type = "INT";
			}else {
				type = "STRING";
			}
			builder.append(type).append("(").append(item.fieldName).append(")").append(",");
		}
		if(builder.length() > 0)
			builder.deleteCharAt(builder.length() - 1);
		return builder.toString();
	}
}

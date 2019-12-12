package other;

import simpledb.DbException;
import simpledb.SimpleDb;
import simpledb.TransactionAbortedException;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * Date: 2019/12/12
 * Time: 14:50
 *
 * @author jiangfucheng
 */
public class Test {
	public static void main(String[] args) throws IOException, TransactionAbortedException, DbException {
		SimpleDb.main(new String[]{"parser","catalog.txt"});
	}
}

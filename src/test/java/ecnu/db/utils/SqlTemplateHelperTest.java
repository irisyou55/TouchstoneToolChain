package ecnu.db.utils;

import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.constraintchain.filter.Parameter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlTemplateHelperTest {
    @Test
    public void testTemplatizeSqlInt() throws TouchstoneToolChainException {
        String sql = "select * from test where a=5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "5", false, false));
        String modified = SqlTemplateHelper.templatizeSql("q1", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("select * from test where a='0,0'", modified);
    }
    @Test
    public void testTemplatizeSqlFloat() throws TouchstoneToolChainException {
        String sql = "select * from test where a=1.5";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "1.5", false, false));
        String modified = SqlTemplateHelper.templatizeSql("q2", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("select * from test where a='0,0'", modified);
    }
    @Test
    public void testTemplatizeSqlStr() throws TouchstoneToolChainException {
        String sql = "select * from test where a='5'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "5", true, false));
        String modified = SqlTemplateHelper.templatizeSql("q3", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("select * from test where a='0,0'", modified);
    }
    @Test
    public void testTemplatizeSqlDate() throws TouchstoneToolChainException {
        String sql = "select * from test where a='1998-12-12 12:00:00.000000'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "1998-12-12 12:00:00.000000", true, true));
        String modified = SqlTemplateHelper.templatizeSql("q4", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("select * from test where a='0,1'", modified);
    }
    @Test
    public void testTemplatizeSqlConflicts() throws TouchstoneToolChainException {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        parameters.add(new Parameter(0, "5", true, false));
        parameters.add(new Parameter(1, "5", true, false));
        String modified = SqlTemplateHelper.templatizeSql("q5", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("-- conflictArgs:{id:0,data:5,needQuote:1,isDate:0},{id:1,data:5,needQuote:1,isDate:0}\nselect * from test where a='5' or b='5'", modified);
    }
    @Test
    public void testTemplatizeSqlCannotFind() throws TouchstoneToolChainException {
        String sql = "select * from test where a='5' or b='5'";
        List<Parameter> parameters = new ArrayList<>();
        Parameter parameter = new Parameter(0, "6", true, false);
        parameters.add(parameter);
        String modified = SqlTemplateHelper.templatizeSql("q6", sql, JdbcConstants.MYSQL, parameters);
        assertEquals("-- cannotFindArgs:{id:0,data:6,needQuote:1,isDate:0}\nselect * from test where a='5' or b='5'", modified);
    }
}
package gammasupport;

import MDELite.ErrInt;
import PrologDB.TableSchema;

/** list of all Gamma Errors -- programs terminate via a runtime exception */
public enum GammaError implements ErrInt<GammaError> {

    callThreadListInit("ThreadList.init() not called!"),
    connectorConstructor("When creating connector NAME error MESSAGE"),
    fileReadError("Table TABLENAME read error MESSAGE"),
    mapHasIncorrectLength("map has incorrect length used: LENGTH != CORRECTLENGTH"),
    pipeCloseError("Pipe PIPENAME close error MESSAGE"),
    pipeReadError("Pipe PIPENAME read error MESSAGE"),
    pipeWriteError("Pipe PIPENAME write error MESSAGE"),
    tableSchemaNotSet("TableSchema not set in connector NAME"),
    tableSchemasNoMatch("TableSchemas do not match in connector array NAME"),
    threadException("Exception in ThreadList.run: MESSAGE"),
    tupleHasWrongNumCols("Tuple of schema SCHEMA has wrong number NUMBER of columns"),
    undefinedField("Field FIELD is undefined in box BOX on connector NAME"),
    ;

    String msg;

    private GammaError(String msg) {
        this.msg = msg;
    }

    @Override
    public String getMsg() {
        return msg;
    }

    @Override
    public ErrInt<GammaError>[] vals() {
        return GammaError.values();
    }

    public static RuntimeException toss(GammaError error, Object... args) {
        return new RuntimeException(EString(error,args));
    }

    public static String EString(GammaError error, Object... args) {
        return MDELite.Utils.makeString(error.msg, args);
    }
    
    public static void mismatchedSchemas(TableSchema ts) {
        throw GammaError.toss(tableSchemasNoMatch, ts.getName());
    }
}

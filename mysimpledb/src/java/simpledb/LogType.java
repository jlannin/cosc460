package simpledb;

/**
 * @author mhay
 * //todo: make this an enum?
 */
public class LogType {
    public static final int ABORT_RECORD = 1;
    public static final int COMMIT_RECORD = 2;
    public static final int UPDATE_RECORD = 3;
    public static final int BEGIN_RECORD = 4;
    public static final int CHECKPOINT_RECORD = 5;
    public static final int CLR_RECORD = 6;
}

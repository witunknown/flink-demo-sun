package sun.model;

/**
 * @author Sun
 * @version 1.0.0
 * @ClassName UserVisitInfo.java
 * @Description TODO
 * @createTime 2021年04月08日 23:27:00
 */
public class UserVisitInfo {

    private String uid;

    private String path;

    private String startDateTime;

    private String endDateTime;

    public UserVisitInfo(String uid, String path, String startDateTime, String endDateTime) {
        this.uid = uid;
        this.path = path;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(String startDateTime) {
        this.startDateTime = startDateTime;
    }

    public String getEndDateTime() {
        return endDateTime;
    }

    public void setEndDateTime(String endDateTime) {
        this.endDateTime = endDateTime;
    }

    @Override
    public String toString() {
        return "UserVisitInfo{" +
                "uid='" + uid + '\'' +
                ", path='" + path + '\'' +
                ", startDateTime='" + startDateTime + '\'' +
                ", endDateTime='" + endDateTime + '\'' +
                '}';
    }
}

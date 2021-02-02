package sun.model;

import java.io.Serializable;

/**
 * Created byX on 2021-02-03 00:22
 * Desc:
 */
public class UserInfo implements Serializable {

    private String id;
    private String name;
    private String sex;
    private String visitTime;
    private int source;

    public UserInfo() {
    }

    public UserInfo(String id, String name, String sex, String visitTime, int source) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.visitTime = visitTime;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(String visitTime) {
        this.visitTime = visitTime;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", source=" + source +
                '}';
    }
}
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
    private int score;
    private String visitPage;

    public UserInfo() {
    }

    public UserInfo(String id, String name, String sex, String visitTime, int source) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.visitTime = visitTime;
        this.score = source;
    }

    public UserInfo(String id, String name, String sex, String visitTime, int source, String visitPage) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.visitTime = visitTime;
        this.score = source;
        this.visitPage = visitPage;
    }

    public String getVisitPage() {
        return visitPage;
    }

    public void setVisitPage(String visitPage) {
        this.visitPage = visitPage;
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

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", source=" + score +
                '}';
    }
}

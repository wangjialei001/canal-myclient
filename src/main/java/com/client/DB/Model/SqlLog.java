package com.client.DB.Model;

import java.util.Date;

public class SqlLog {

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Date getCreatetime() {
        return createtime;
    }

    public void setCreatetime(Date createtime) {
        this.createtime = createtime;
    }

    public boolean isIsexecute() {
        return isexecute;
    }

    public void setIsexecute(boolean isexecute) {
        this.isexecute = isexecute;
    }
    private long id;
    private String body;
    private Date createtime;
    private boolean isexecute;

}

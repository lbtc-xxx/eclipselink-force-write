package entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;

@Entity
public class MyTable implements Serializable {
    @Id
    private String mycol;

    public String getMycol() {
        return mycol;
    }

    public void setMycol(final String mycol) {
        this.mycol = mycol;
    }
}

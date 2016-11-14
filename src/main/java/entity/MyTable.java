package entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;

@Entity
public class MyTable implements Serializable {
    @Id
    private Integer id;
    @Column
    private String myValue;

    public Integer getId() {
        return id;
    }

    public void setId(final Integer id) {
        this.id = id;
    }

    public String getMyValue() {
        return myValue;
    }

    public void setMyValue(final String myValue) {
        this.myValue = myValue;
    }
}

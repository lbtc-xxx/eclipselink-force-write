package main;

import entity.MyTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ConnectionDestinationTest {
    static EntityManagerFactory emf = null;
    EntityManager em;

    @BeforeClass
    public static void initClass() throws Exception {
        populateDatabase();
        emf = Persistence.createEntityManagerFactory("myPU");
    }

    private static void populateDatabase() throws SQLException {
        final String master = "jdbc:derby:memory:masterDB;create=true";
        final String slave = "jdbc:derby:memory:slaveDB;create=true";

        for (final String url : Arrays.asList(master, slave)) {
            createTable(url);
        }

        insert(master, 1, "master");
        insert(slave, 1, "slave");
    }

    private static void createTable(final String url) throws SQLException {
        final String ddl = "create table mytable (id int, myvalue varchar (255), primary key (id))";
        try(final Connection cn = DriverManager.getConnection(url);
            final Statement st = cn.createStatement()){
            st.executeUpdate(ddl);
        }
    }

    private static void insert(final String url, final int id, final String s) throws SQLException {
        final String sql = "insert into mytable (id, myvalue) values (?, ?)";
        try (final Connection cn = DriverManager.getConnection(url);
             final PreparedStatement ps = cn.prepareStatement(sql)) {
            ps.setInt(1, id);
            ps.setString(2, s);
            ps.executeUpdate();
        }
    }

    @Before
    public void init() {
        em = emf.createEntityManager();
    }

    @Test
    public void queryAfterTransactionFetchesFromSlave() throws Exception {
        em.getTransaction().begin();
        em.find(MyTable.class, 1).setMyValue("updated");
        em.getTransaction().commit(); // master を更新
        em.clear(); // 1次キャッシュ破棄

        final MyTable row = em.createQuery("SELECT m FROM MyTable m WHERE m.id = :id AND 1 = 1", MyTable.class).setParameter("id", 1).getSingleResult(); // AND 1 = 1 でクエリを強制的に実行させる
        final String myValue = row.getMyValue();
        assertThat(myValue, is("slave")); // クエリはスレーブに対して実行され、その値が帰る
    }

    @Test
    public void queryAfterTransactionUsesCache() throws Exception {
        em.getTransaction().begin();
        em.find(MyTable.class, 1).setMyValue("updated");
        em.getTransaction().commit(); // master を更新
        // em.clear(); // 1次キャッシュ破棄しない

        final MyTable row = em.createQuery("SELECT m FROM MyTable m WHERE m.id = :id AND 1 = 1", MyTable.class).setParameter("id", 1).getSingleResult(); // AND 1 = 1 でクエリを強制的に実行させる
        final String myValue = row.getMyValue();
        assertThat(myValue, is("updated")); // クエリはスレーブに対して実行されるものの、1次キャッシュの値が勝つ
    }

    @After
    public void clean() {
        em.close();
    }

    @AfterClass
    public static void cleanClass() {
        emf.close();
    }
}

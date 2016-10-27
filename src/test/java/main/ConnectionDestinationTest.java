package main;

import entity.MyTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.LockModeType;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
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

        insert(master, "master");
        insert(slave, "slave");
    }

    private static void createTable(final String url) throws SQLException {
        final String ddl = "create table mytable (mycol varchar (255), primary key (mycol))";
        try(final Connection cn = DriverManager.getConnection(url);
            final Statement st = cn.createStatement()){
            st.executeUpdate(ddl);
        }
    }

    private static void insert(final String url, final String value) throws SQLException {
        final String sql = "insert into mytable (mycol) values (?)";
        try (final Connection cn = DriverManager.getConnection(url);
             final PreparedStatement ps = cn.prepareStatement(sql)) {
            ps.setString(1, value);
            ps.executeUpdate();
        }
    }

    @Before
    public void init() {
        em = emf.createEntityManager();
    }

    @Test
    public void mostPreferableWayToForceMaster() {
        final String mycol = em.createQuery("select m from MyTable m", MyTable.class)
                .setHint("eclipselink.forceMaster", true) // this hint does not exist
                .getSingleResult().getMycol();

        assertThat(mycol, is("master")); // fail
    }

    @Test
    public void jpqlSelectUsesSlave() {
        final String mycol = jpqlSelect();

        assertThat(mycol, is("slave"));
    }

    @Test
    public void nativeSelectUsesSlave() {
        final String mycol = nativeSelect();

        assertThat(mycol, is("slave"));
    }

    @Test
    public void jpqlSelectWithTransactionUsesSlave() {
        em.getTransaction().begin();

        final String mycol = jpqlSelect();

        em.getTransaction().commit();
        assertThat(mycol, is("slave"));
    }

    @Test
    public void nativeSelectWithTransactionUsesMaster() {
        em.getTransaction().begin();

        final String mycol = nativeSelect();

        em.getTransaction().commit();
        assertThat(mycol, is("master"));
    }

    @Test
    public void jpqlSelectAfterNativeSelectWithTransactionUsesMaster() {
        em.getTransaction().begin();
        em.createNativeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1").getSingleResult(); // this makes the following query to use master

        final String mycol = jpqlSelect();

        em.getTransaction().commit();
        assertThat(mycol, is("master"));
    }

    @Test
    public void jpqlSelectAfterJpqlDeleteWithTransactionUsesMaster() {
        em.getTransaction().begin();
        em.createQuery("delete from MyTable m where 1 = 2").executeUpdate();

        final String mycol = jpqlSelect();

        em.getTransaction().commit();
        assertThat(mycol, is("master"));
    }

    @Test
    public void jpqlSelectWithPessimisticWriteUsesMaster() {
        em.getTransaction().begin();

        final String mycol = jpqlSelect(LockModeType.PESSIMISTIC_WRITE);

        em.getTransaction().commit();
        assertThat(mycol, is("master"));
    }

    @Test
    public void jpqlSelectWithPessimisticReadUsesMaster() {
        em.getTransaction().begin();

        final String mycol = jpqlSelect(LockModeType.PESSIMISTIC_READ);

        em.getTransaction().commit();
        assertThat(mycol, is("master"));
    }

    @Test
    public void findWithPessimisticWriteUsesMaster() {
        final String id = "master";
        em.getTransaction().begin();

        final String mycol = em.find(MyTable.class, id, LockModeType.PESSIMISTIC_WRITE).getMycol();

        em.getTransaction().commit();
        assertThat(mycol, is(id));
    }

    @Test
    public void findWithPessimisticReadUsesMaster() {
        final String id = "master";
        em.getTransaction().begin();

        final String mycol = em.find(MyTable.class, id, LockModeType.PESSIMISTIC_READ).getMycol();

        em.getTransaction().commit();
        assertThat(mycol, is(id));
    }

    private String nativeSelect() {
        return (String) em.createNativeQuery("select mycol from mytable").getSingleResult();
    }

    private String jpqlSelect() {
        return jpqlSelect(LockModeType.NONE);
    }

    private String jpqlSelect(final LockModeType lockModeType) {
        final TypedQuery<MyTable> query = em.createQuery("select m from MyTable m", MyTable.class).setLockMode(lockModeType);
        final MyTable result = query.getSingleResult();
        return result.getMycol();
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

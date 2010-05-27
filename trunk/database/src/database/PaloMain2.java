
package database;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.palo.api.Connection;
import org.palo.api.ConnectionConfiguration;
import org.palo.api.Database;
import org.palo.api.Dimension;
import org.palo.api.Hierarchy;
import org.palo.api.ConnectionFactory;
import org.palo.api.Element;
import org.palo.api.Consolidation;
import org.palo.api.Cube;

public class PaloMain2 {

    static String url_sursa_postgres1 = "jdbc:postgresql://gate.montebanato.ro:5432/pangram_week_2008";
    static String url_sursa_postgres2 = "jdbc:postgresql://192.168.61.205:5432/pangram_raport_2008";
    static String url_sursa_postgres  = url_sursa_postgres2;

static class Produs{
    private String denumire;
    private String clasa;
    private String grupa;
    private String categorie;
    public Produs(String denProd){
        this.denumire=denProd;
        this.clasa=null;
        this.categorie=null;
        this.grupa=null;
    }
    public Produs(String denProd, String denCls, String denCat, String denGru){
        this.denumire=denProd;
        this.clasa=denCls;
        this.categorie=denCat;
        this.grupa=denGru;
    }

    public void rename(String denProd){
        this.denumire=denProd;
    }
    String getDenumire(){
        return this.denumire;
    }
    public void addClasa(String denCls){
        this.clasa=denCls;
    }
    String getClasa(){
        return this.clasa;
    }
    public void addCategorie(String denCat){
        this.categorie=denCat;
    }
    String getCategorie(){
        return this.categorie;
    }
    public void addGrupa(String denGru){
        this.grupa=denGru;
    }
    String getGrupa(){
        return this.grupa;
    }
}

static class Client{
    private String id;
    private String denumire;
    private String clasa;
    private String grupa;
    private String categorie;
    public Client(String denCli){
        this.denumire=denCli;
        this.clasa=null;
        this.categorie=null;
        this.grupa=null;
    }
    public Client(String sid, String denCli, String denCls, String denCat, String denGru){
        this.denumire=denCli;
        this.clasa=denCls;
        this.categorie=denCat;
        this.grupa=denGru;
        this.id=sid;
    }

    public void rename(String denDep){
        this.denumire=denDep;
    }
    String getID(){
        return this.id;
    }
    String getDenumire(){
        return this.denumire;
    }
    public void addClasa(String denCls){
        this.clasa=denCls;
    }
    String getClasa(){
        return this.clasa;
    }
    public void addCategorie(String denCat){
        this.categorie=denCat;
    }
    String getCategorie(){
        return this.categorie;
    }
    public void addGrupa(String denGru){
        this.grupa=denGru;
    }
    String getGrupa(){
        return this.grupa;
    }
}


 static class Depozit{
    private String denumire;
    public ArrayList <String> listOfAgents;
    public Depozit(String denDep){
        this.denumire=denDep;
        this.listOfAgents = new ArrayList <String>();
    }
    public void rename(String denDep){
        this.denumire=denDep;
    }
    String getDen(){
        return this.denumire;
    }
    public void addAg(String denAg){
        // this.denumire=denDep;
        this.listOfAgents.add(denAg);
    }
}


static void SetDimProductByList1( List<Produs> ProductList, String mS_dimensiune ){
    Connection conn_dest = ConnectionFactory.getInstance().newConnection("127.0.0.1", "7777", "admin", "admin");
    Database odb = conn_dest.addDatabase("testu_olap6e");
try {
    Iterator vItr2 = ProductList.iterator();
    Dimension[] dimensions = new Dimension[1];
    Dimension dim1 = odb.addDimension(mS_dimensiune);
    Hierarchy hie1 = dim1.getDefaultHierarchy();
    dimensions[0]=dim1;
    String illegal_char = " ";
    String legal_char = "";
    String mS_denumire;
    ArrayList <String> listOfProducts;
    Element parent, child;
    int index2=0, size=0;
    Consolidation[] consolidations ;
    Iterator vItr = null;
    while (vItr2.hasNext()){
        Produs iProd;
        iProd = (Produs) vItr2.next();
        mS_denumire = iProd.getDenumire();
        if(!mS_denumire.substring(0, 1).equals("0")){
            mS_denumire = mS_denumire.replaceAll(illegal_char, legal_char);
            System.out.println(" __PRODUS__ " + mS_denumire);
            hie1.addElement( mS_denumire , Element.ELEMENTTYPE_NUMERIC);
        }
    }
} catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
}
}


  static void CreateDimProduct002(){
    String SQL_Product = " " +
     " select nomstoc.stoc_id, nomstoc.simbol, nomstoc.denumire, " +
     " clasa.denumire as clasa, grupa.denumire as grupa, " +
     " categorie.denumire as categorie from nomstoc, categorie, grupa, clasa  " +
     " where nomstoc.sw_0='a' AND  " +
     " nomstoc.categorie_id=categorie.categ_id  " +
     " AND nomstoc.grupa_id=grupa.grupa_id  " +
     " AND nomstoc.clasa_id=clasa.clasa_id  " +
     " ORDER BY clasa, grupa, categorie  " ;
    String SQL_Sursa = SQL_Product;
    List<Produs> ProductList = new ArrayList<Produs>();
    ProductList  =  getProductListBySQL (SQL_Sursa);
    SetDimProductByList1( ProductList, "Produse" );
}

  static List<Produs> getProductListBySQL(String SQL_sursa) {
    List<Produs> ProdusList = new ArrayList<Produs>();
    Produs iProd = null;
    //String url_sursa = "jdbc:postgresql://gate.montebanato.ro:5432/pangram_week_2008";
    String url_sursa = "jdbc:postgresql://192.168.61.205:5432/pangram_raport_2009";
    url_sursa = url_sursa_postgres;
    String username_sursa = "postgres";
    String password_sursa = "telinit";
    java.sql.Connection conn_sursa = null;
    java.sql.Statement stmt_sursa = null;
    try {
        Class.forName("org.postgresql.Driver");
        System.out.println("Driver PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Failed to load Postgres Driver");
        System.err.println(e);
    }
    try {
        conn_sursa = DriverManager.getConnection(url_sursa,username_sursa,password_sursa);
        stmt_sursa = conn_sursa.createStatement();
        System.out.println("Connection to " + url_sursa + " OK");
    } catch (Exception e) {
        System.err.println("Connection to Postgres failed");
        System.err.println(e);
    }
try {
    ResultSet rs_sursa = null;
    rs_sursa = stmt_sursa.executeQuery(SQL_sursa);
    String illegal_char = " ";
    String legal_char = "_";
    String mS_denCli =" ", mS_denCls =" ", mS_denCat =" ", mS_denGru =" ";
    Client iClient = null;
    while (rs_sursa.next()) {
        mS_denCli = rs_sursa.getString("denumire").replaceAll(illegal_char, legal_char);
        mS_denCat = rs_sursa.getString("categorie").replaceAll(illegal_char, legal_char);
        mS_denGru = rs_sursa.getString("grupa").replaceAll(illegal_char, legal_char);
        mS_denCls = rs_sursa.getString("clasa").replaceAll(illegal_char, legal_char);
        iProd = new Produs(mS_denCli);
        ProdusList.add(iProd);
        System.out.println(mS_denCli + "  " + mS_denCls + "  " + mS_denGru + "  " + mS_denCat);
    }
    System.out.println("Transfer OK");
    rs_sursa.close();
    stmt_sursa.close();
    conn_sursa.close();
    } catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
    }
    return ProdusList;
    }



  static void CreateDimCustomer(){
    String SQL_Customer = " " +
        " SELECT tert_id, terti.denumire as client, " +
        " clasa.denumire as clasa, " +
        " grupa.denumire as grupa, " +
        " categorie.denumire as categorie " +
        " FROM  terti, categorie, grupa, clasa " +
        " WHERE terti.categorie_id=categorie.categ_id " +
	    " AND terti.grupa_id=grupa.grupa_id " +
	    " AND terti.clasa_id=clasa.clasa_id " +
	    " AND terti.sw_0='a' " +
        " ORDER BY clasa, grupa, categorie ";
    String SQL_Sursa = SQL_Customer;
    List<Client> CustomerList = new ArrayList<Client>();
    CustomerList  =  getCustomerListBySQL (SQL_Sursa);
    SetDimCustomerByList(CustomerList, "Customer");
}


  static void SetDimCustomerByList( List<Client> CustomerList, String mS_dimensiune ){
    // Connection conn_dest = ConnectionFactory.getInstance().newConnection("127.0.0.1", "7777", "admin", "admin");
      // ConnectionConfiguration cc = new ConnectionConfiguration("localhost","7777");
      ConnectionConfiguration cc = new ConnectionConfiguration("localhost","7921");
      cc.setUser("admin");
      cc.setPassword("admin");
      Connection conn_dest = ConnectionFactory.getInstance().newConnection(cc);
      Database odb = conn_dest.addDatabase("testu_olap7b");

try {
    Iterator vItr2 = CustomerList.iterator();
    Integer iNoCust = new Integer(CustomerList.size());
    // Integer iIndex = new Integer(1);
    Dimension[] dimensions = new Dimension[1];
    Dimension dim1 = odb.addDimension(mS_dimensiune);
    Hierarchy hie1 = dim1.getDefaultHierarchy();
    dimensions[0]=dim1;
    String illegal_char = " ";
    String legal_char = "";
    String mS_denumire;
    String mS_id;
    Element parent, child;
    int index1=0, size=0;
    Consolidation[] consolidations ;
    Iterator vItr = null;
    // 2008-02.08 ~ new variable iClient;
    System.out.println("   NR.Clienti =  " + iNoCust.toString());
    while (vItr2.hasNext()){
        index1=index1+1;
        Client iClient;
        iClient = (Client) vItr2.next();
        mS_denumire = iClient.getDenumire();
        mS_id= iClient.getID();
        //if(!mS_denumire.substring(0, 1).equals("0")){
            mS_denumire = mS_denumire.replaceAll(illegal_char, legal_char);
            mS_id = mS_id.replaceAll(illegal_char, legal_char);
            System.out.println(" __Client_" + index1 + "  " + mS_denumire);
            mS_denumire = mS_denumire.concat(mS_id);
            hie1.addElement( mS_denumire , Element.ELEMENTTYPE_NUMERIC);
        //}
    }
    System.out.println("   GATA cu "  + iNoCust.toString() );
} catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
}
}


  static List<Client> getCustomerListBySQL(String SQL_sursa) {
    List<Client> CustomerList = new ArrayList<Client>();
    // String url_sursa = "jdbc:postgresql://gate.montebanato.ro:5432/pangram_week_2008";
    String url_sursa = "jdbc:postgresql://192.168.61.205:5432/pangram_raport_2009";
    // String url_sursa = "jdbc:postgresql://192.168.1.26:5432/pangram_week_2008";
    url_sursa = url_sursa_postgres;
    String username_sursa = "postgres";
    String password_sursa = "telinit";
    java.sql.Connection conn_sursa = null;
    java.sql.Statement stmt_sursa = null;
    try {
        Class.forName("org.postgresql.Driver");
        System.out.println("Driver PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Failed to load Postgres Driver");
        System.err.println(e);
    }
    try {
        conn_sursa = DriverManager.getConnection(url_sursa,username_sursa,password_sursa);
        stmt_sursa = conn_sursa.createStatement();
        System.out.println("Connection to source PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Connection to Postgres failed");
        System.err.println(e);
    }
try {
    ResultSet rs_sursa = null;
    rs_sursa = stmt_sursa.executeQuery(SQL_sursa);
    String illegal_char = " ";
    String legal_char = "";
    String mS_iD= " ", mS_denCli =" ", mS_denCls =" ", mS_denCat =" ", mS_denGru =" ";
    Client iClient = null;
    while (rs_sursa.next()) {
        mS_iD = rs_sursa.getString("tert_id").replaceAll(illegal_char, legal_char);
        mS_denCli = rs_sursa.getString("client").replaceAll(illegal_char, legal_char);
        mS_denCat = rs_sursa.getString("categorie").replaceAll(illegal_char, legal_char);
        mS_denGru = rs_sursa.getString("grupa").replaceAll(illegal_char, legal_char);
        mS_denCls = rs_sursa.getString("clasa").replaceAll(illegal_char, legal_char);
        System.out.println(mS_denCli + "  " + mS_denCls + "  " + mS_denGru + "  " + mS_denCat);
        iClient = new Client(mS_iD,mS_denCli,mS_denCls,mS_denCat,mS_denGru);
        CustomerList.add(iClient);
    }
    System.out.println("Transfer OK");
    rs_sursa.close();
    stmt_sursa.close();
    conn_sursa.close();
    } catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
    }
    return CustomerList;
    }



  static void CreateDimDepoAg(){
    // SQL4 AGENTI & DEPOZITE
    String SQL_AgByDep = " SELECT numere_lucru.nrlc_id, numere_lucru.nick, numere_lucru.denumire, " +
             " numere_lucru.categorie_id, categorie.denumire as categorie_denumire, " +
             " numere_lucru.grupa_id, numere_lucru.clasa_id "+
             " FROM numere_lucru, categorie " +
             " WHERE numere_lucru.categorie_id=categorie.categ_id AND numere_lucru.sw_0='a' " +
             " ORDER BY categorie.denumire, numere_lucru.denumire ";
    String SQL_Sursa = SQL_AgByDep;
    List<Depozit> DepoAgList = new ArrayList<Depozit>();
    DepoAgList  =  getDepoAgListBySQL (SQL_Sursa);
    SetDimDepoAgByList(DepoAgList, "DepoAg");
}


  static List<Depozit> getDepoAgListBySQL(String SQL_sursa) {
    List<Depozit> listOfDepos = new ArrayList<Depozit>();
    // String url_sursa = "jdbc:postgresql://gate.montebanato.ro:5432/pangram_week_2008";
    String url_sursa = "jdbc:postgresql://192.168.61.205:5432/pangram_raport_2009";
    // String url_sursa = "jdbc:postgresql://192.168.1.26:5432/pangram_week_2008";
    url_sursa = url_sursa_postgres;
    String username_sursa = "postgres";
    String password_sursa = "telinit";
    java.sql.Connection conn_sursa = null;
    java.sql.Statement stmt_sursa = null;
    try {
        Class.forName("org.postgresql.Driver");
        System.out.println("Driver PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Failed to load Postgres Driver");
        System.err.println(e);
    }
    try {
        conn_sursa = DriverManager.getConnection(url_sursa,username_sursa,password_sursa);
        stmt_sursa = conn_sursa.createStatement();
        System.out.println("Connection to source PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Connection to Postgres failed");
        System.err.println(e);
    }
try {
    ResultSet rs_sursa = null;
    rs_sursa = stmt_sursa.executeQuery(SQL_sursa);
    String illegal_char = " ";
    String legal_char = "";
    String mS_denDep =" ", mS_denAg =" ";
    Depozit iDepo = null;
    while (rs_sursa.next()) {
        mS_denAg = rs_sursa.getString("nick").replaceAll(illegal_char, legal_char);
        mS_denDep = rs_sursa.getString("categorie_denumire").replaceAll(illegal_char, legal_char);
        if ( (iDepo !=null) && ( iDepo.getDen().equals(mS_denDep)) ){
            iDepo.listOfAgents.add(mS_denAg);
            if(mS_denDep.trim().equals("ARAD")){
                System.out.println(mS_denAg + "  " + Integer.toString(iDepo.listOfAgents.size()));
            }
        }
        else{
            iDepo = new Depozit(" ");
            iDepo.rename(mS_denDep.trim());
            listOfDepos.add(iDepo);
            // System.out.println(iDepo.getDen());
        }
    }
    System.out.println("Transfer OK");
    rs_sursa.close();
    stmt_sursa.close();
    conn_sursa.close();
    } catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
    }
    return listOfDepos;
    }

static void SetDimDepoAgByList( List<Depozit> DepoAgList, String mS_dimensiune ){
    // Connection conn_dest = ConnectionFactory.getInstance().newConnection("127.0.0.1", "7777", "admin", "admin");
    Connection conn_dest = ConnectionFactory.getInstance().newConnection("localhost", "7777", "admin", "admin");
    Database odb = conn_dest.addDatabase("pangram_olap6");
try {
    Iterator vItr2 = DepoAgList.iterator();
    Dimension[] dimensions = new Dimension[1];
    Dimension dim1 = odb.addDimension(mS_dimensiune);
    Hierarchy hie1 = dim1.getDefaultHierarchy();
    dimensions[0]=dim1;
    String illegal_char = " ";
    String legal_char = "";
    String mS_denumire, mS_DenDep;
    ArrayList <String> listOfAgents;
    Element parent, child;
    int index2=0, size=0;
    Consolidation[] consolidations ;
    Iterator vItr = null;
    while (vItr2.hasNext()){
        Depozit iDepo;
        iDepo = (Depozit) vItr2.next();
        mS_denumire = iDepo.getDen();
        if(!mS_denumire.substring(0, 1).equals("0")){
            mS_denumire = mS_denumire.replaceAll(illegal_char, legal_char);
            System.out.println(" __DEPOZIT__ " + mS_denumire);
            hie1.addElement( mS_denumire , Element.ELEMENTTYPE_NUMERIC);
            index2=0;
            parent = hie1.getElementByName(mS_denumire);
            listOfAgents = iDepo.listOfAgents;
            size = listOfAgents.size();
            System.out.println(" __NR.Agenti = " + Integer.toString(size));
            consolidations = new Consolidation[size];
            vItr = listOfAgents.iterator();
            while (vItr.hasNext()){
                mS_DenDep = (String) vItr.next();
                System.out.println(" agent : " + mS_DenDep);
                child = hie1.addElement(mS_DenDep, Element.ELEMENTTYPE_NUMERIC);
                consolidations[index2] = hie1.newConsolidation(child, parent, 1);
                index2=index2+1;
            }
            parent.updateConsolidations(consolidations);
            // break;
        }
    }
} catch (Exception e) {
    System.err.println("An error has occurred during transfer");
    System.err.println(e);
}
}

static void CreateTheCube(){

    String url_sursa = url_sursa_postgres2;
    String username_sursa = "postgres";
    String password_sursa = "telinit";
    java.sql.Connection conn_sursa = null;
    java.sql.Statement stmt_sursa = null;
    try {
        Class.forName("org.postgresql.Driver");
        System.out.println("Driver PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Failed to load Postgres Driver");
    }
    try {
        conn_sursa = DriverManager.getConnection(url_sursa,username_sursa,password_sursa);
        stmt_sursa = conn_sursa.createStatement();
        System.out.println("Connection to source PostgreSQL OK");
    } catch (Exception e) {
        System.err.println("Connection to Postgres failed");
    }
    org.palo.api.Connection conn_dest = ConnectionFactory.getInstance().newConnection("127.0.0.1", "7921", "admin", "admin");
    Database odb = conn_dest.addDatabase("pangram_olap");
    try {
        // SELECT categ_id,denumire FROM categorie WHERE fisier='numere_lucru' ORDER BY denumire ;
        // String SQL_sursa = "select nrdoc, data, valoare, reclamant, cant, um, simb, denmat, cls "+
        // "from depit_aviz_2008";
        String SQL_sursa = " SELECT categ_id,denumire FROM categorie WHERE fisier='numere_lucru' ORDER BY denumire ";
        ResultSet rs_sursa = null;
        rs_sursa = stmt_sursa.executeQuery(SQL_sursa);
        org.palo.api.Dimension dim1 = odb.addDimension("dimWarehouse");
        org.palo.api.Dimension dim2 = odb.addDimension("dimYears");
        org.palo.api.Dimension dim3 = odb.addDimension("dimCustomers");
        String illegal_char = " ";
        String legal_char = "";
        while (rs_sursa.next()) {
            String mS_denumire = rs_sursa.getString("denumire");
            String mS_den2 = mS_denumire.replaceAll(illegal_char, legal_char);
            // String mS_den3 = mS_den2;
            System.out.println(mS_denumire);
            dim1.addElement(mS_den2, Element.ELEMENTTYPE_NUMERIC);
        }
        System.out.println("Transfer OK");
        // m_vector.addElement(new StockData(symbol, name, last, open, change, changePr, volume));
        rs_sursa.close();
        stmt_sursa.close();
        conn_sursa.close();
        dim2.addElement("2003", Element.ELEMENTTYPE_NUMERIC);
        dim2.addElement("2004", Element.ELEMENTTYPE_NUMERIC);
        dim2.addElement("2005", Element.ELEMENTTYPE_NUMERIC);
        dim3.addElement("Metro", Element.ELEMENTTYPE_NUMERIC);
        dim3.addElement("Kaufland", Element.ELEMENTTYPE_NUMERIC);
        dim3.addElement("Real", Element.ELEMENTTYPE_NUMERIC);
        Dimension[] dimensions = new Dimension[3];
        dimensions[0]=dim1;
        dimensions[1]=dim2;
        dimensions[2]=dim3;
        Cube cube = odb.addCube("cubeSales", dimensions);
        String COORDINATES[] = {"Timisoara","2004","Metro"};
        cube.setData(COORDINATES, new Double(247.26));
        System.out.println("Gata1");
    }
    catch (Exception e) {
        System.err.println("An error has occurred during transfer");
        System.err.println(e);
    }
}

public static void main(String[] args) {
        //CreateDimDepoAg();
    CreateTheCube();
    //CreateDimCustomer();
        //CreateDimProduct002();
    System.out.println("Gata2");
}
}


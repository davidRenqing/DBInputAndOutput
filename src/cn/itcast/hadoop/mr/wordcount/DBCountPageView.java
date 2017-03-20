package cn.itcast.hadoop.mr.wordcount;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.server.Server;  //这个数据库链接的包我的项目还没有导进来呢

/**
 * This is a demonstrative program, which uses DBInputFormat for reading
 * the input data from a database, and DBOutputFormat for writing the data 
 * to the database. 这是一个使用DBInputFormat 将文件从数据库当中读出来。
 * 使用DBInputFormat将数据读入到数据库当中的例子
 * <br>
 * The Program first creates the necessary tables, populates the input table 
 * and runs the mapred job. 
 * <br> 
 * 这里描述输出数据的格式 The input data is a mini access log, with a <code>&lt;url,referrer,time&gt;
 * </code> schema.这里描述输出数据的格式 The output is the number of pageviews of each url in the log, 
 * having the schema <code>&lt;url,pageview&gt;</code>.  
 * 
 * When called with no arguments the program starts a local HSQLDB server, and 
 * uses this database for storing/retrieving the data. 
 * <br>
 * This program requires some additional configuration relating to HSQLDB.  
 * The the hsqldb jar should be added to the classpath:
 * <br>
 * <code>export HADOOP_CLASSPATH=share/hadoop/mapreduce/lib-examples/hsqldb-2.0.0.jar</code>
 * <br>
 * And the hsqldb jar should be included with the <code>-libjars</code> 
 * argument when executing it with hadoop:
 * <br>
 * <code>-libjars share/hadoop/mapreduce/lib-examples/hsqldb-2.0.0.jar</code>
 */
public class DBCountPageView extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(DBCountPageView.class);
  
  private Connection connection;
  private boolean initialized = false;
  private boolean isOracle = false;

  private static final String[] AccessFieldNames = {"url", "referrer", "time"};
  private static final String[] PageviewFieldNames = {"url", "pageview"};
  
  private static final String DB_URL = 
    "jdbc:hsqldb:hsql://localhost/URLAccess";
  private static final String DRIVER_CLASS = "org.hsqldb.jdbc.JDBCDriver";
  
  private Server server;
  
  private void startHsqldbServer() { //开始Hsql的Server
    server = new Server();
    server.setDatabasePath(0, 
        System.getProperty("test.build.data", "/tmp") + "/URLAccess");
    server.setDatabaseName(0, "URLAccess");
    server.start();
  }
  
  private void createConnection(String driverClassName  //创建连接数据库
      , String url) throws Exception {
    if(StringUtils.toLowerCase(driverClassName).contains("oracle")) { //先将driverClassName都转换成小写字母。之后判断是不是含有oracle 。如果含有，就说明是要链接oracle的数据库驱动
      isOracle = true;
    }
    Class.forName(driverClassName);
    connection = DriverManager.getConnection(url);
    connection.setAutoCommit(false);
  }

  private void shutdown() { //关闭数据库链接的函数
    try {
      connection.commit();
      connection.close();
    }catch (Throwable ex) {
      LOG.warn("Exception occurred while closing connection :"
          + StringUtils.stringifyException(ex));
    } finally {
      try {
        if(server != null) {
          server.shutdown();
        }
      }catch (Throwable ex) {
        LOG.warn("Exception occurred while shutting down HSQLDB :"
            + StringUtils.stringifyException(ex));
      }
    }
  }

  
  
  private void initialize(String driverClassName, String url)  //这个函数就是进行数据库的初始化的工作
    throws Exception {
    if(!this.initialized) { //如果数据库的连接还没有进行初始化
      if(driverClassName.equals(DRIVER_CLASS)) {
        startHsqldbServer();  //就打开数据库的初始化的连接
      }
      createConnection(driverClassName, url); //进行链接
      dropTables(); //删除这次的job要使用的表。如果这些表已经存在了
      createTables();  //重新创建这次job中要用到那些表
      populateAccess();  //为了测试这个程序，在表中随机添加了一些数据
      this.initialized = true;  //为什么要设定这个初始化的标定啊？？
    }
  }
  
  private void dropTables() { //删除数据库的表的操作
    String dropAccess = "DROP TABLE HAccess";
    String dropPageview = "DROP TABLE Pageview";
    Statement st = null;
    try {
      st = connection.createStatement();
      st.executeUpdate(dropAccess);
      st.executeUpdate(dropPageview);
      connection.commit();
      st.close();
    }catch (SQLException ex) {
      try { if (st != null) { st.close(); } } catch (Exception e) {}
    }
  }
  
  private void createTables() throws SQLException {//创建一个数据库的表的操作
	String dataType = "BIGINT NOT NULL";
	if(isOracle) {
	  dataType = "NUMBER(19) NOT NULL";
	}
	//创建数据库的表的操作
    String createAccess = 
      "CREATE TABLE " +
      "HAccess(url      VARCHAR(100) NOT NULL," +
            " referrer VARCHAR(100)," +
            " time     " + dataType + ", " +
            " PRIMARY KEY (url, time))";

    String createPageview = 
      "CREATE TABLE " +
      "Pageview(url      VARCHAR(100) NOT NULL," +
              " pageview     " + dataType + ", " +
               " PRIMARY KEY (url))";
    
    //st是建立数据库的链接
    Statement st = connection.createStatement();
    try {
      st.executeUpdate(createAccess);
      st.executeUpdate(createPageview);
      connection.commit();
    } finally {
      st.close();
    }
  }

  /**
   * Populates the Access table with generated records.
   */
  //这个函数的作用我还没有看懂
  //我去这个函数不会是作为测试用例，在这个新建的表当中随机添加了一些信息吧
  private void populateAccess() throws SQLException {

    PreparedStatement statement = null ;
    try {
      statement = connection.prepareStatement(
          "INSERT INTO HAccess(url, referrer, time)" +
          " VALUES (?, ?, ?)");

      Random random = new Random();

      int time = random.nextInt(50) + 50;

      final int PROBABILITY_PRECISION = 100; //  1 / 100 
      final int NEW_PAGE_PROBABILITY  = 15;  //  15 / 100


      //Pages in the site :
      //这里定义这些String类型的字符串是什么意思？？？
      String[] pages = {"/a", "/b", "/c", "/d", "/e", 
                        "/f", "/g", "/h", "/i", "/j"};
      
      
      //linkMatrix[i] is the array of pages(indexes) that page_i links to.  
      int[][] linkMatrix = {{1,5,7}, {0,7,4,6,}, {0,1,7,8}, 
        {0,2,4,6,7,9}, {0,1}, {0,3,5,9}, {0}, {0,1,3}, {0,2,6}, {0,2,6}};

      
      //a mini model of user browsing a la pagerank
      int currentPage = random.nextInt(pages.length); 
      String referrer = null;

      
      
      for(int i=0; i<time; i++) {

        statement.setString(1, pages[currentPage]);
        statement.setString(2, referrer);
        statement.setLong(3, i);
        statement.execute();

        int action = random.nextInt(PROBABILITY_PRECISION);

        // go to a new page with probability 
        // NEW_PAGE_PROBABILITY / PROBABILITY_PRECISION
        if(action < NEW_PAGE_PROBABILITY) { 
          currentPage = random.nextInt(pages.length); // a random page
          referrer = null;
        }
        else {
          referrer = pages[currentPage];
          action = random.nextInt(linkMatrix[currentPage].length);
          currentPage = linkMatrix[currentPage][action];
        }
      }
      
      connection.commit();
      
    }catch (SQLException ex) {
      connection.rollback();
      throw ex;
    } finally {
      if(statement != null) {
        statement.close();
      }
    }
  }
  
  /**Verifies the results are correct */
  //验证最后的结果是正确的
  private boolean verify() throws SQLException {
    //check total num pageview
    String countAccessQuery = "SELECT COUNT(*) FROM HAccess";
    String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
    Statement st = null;
    ResultSet rs = null;
    try {
      st = connection.createStatement();//获得数据库的链接
      rs = st.executeQuery(countAccessQuery);//进行数据的查询，将查询的结果存放在rs当中
      rs.next();//查看rs是不是有下一个元素
      long totalPageview = rs.getLong(1); //将当前行的第1列取出来

      rs = st.executeQuery(sumPageviewQuery);//查询第二个表的元素
      rs.next();//将rs指定到下一个的位置
      long sumPageview = rs.getLong(1); //取出元素的总的个数

      LOG.info("totalPageview=" + totalPageview);  //使用这个log对象将元素的个数输出来
      LOG.info("sumPageview=" + sumPageview);

      return totalPageview == sumPageview && totalPageview != 0;//这里为什么起到了这个作用
    }finally {
      if(st != null)
        st.close();
      if(rs != null)
        rs.close();
    }
  }
  
  /** Holds a &lt;url, referrer, time &gt; tuple */
  static class AccessRecord implements Writable, DBWritable {
    String url;
    String referrer;
    long time;
    
    //我怎么感觉这个函数是hadoop的反序列化的机制啊
    @Override
    public void readFields(DataInput in) throws IOException {
      this.url = Text.readString(in); //这是从数据库对象in，当中读到的字符串当中读出 url的字符串
      this.referrer = Text.readString(in); //这是从数据库对象in，当中读出 referrer这个数据的对象
      this.time = in.readLong(); //这是将 time 这个字符串读取出来
    }
    
    //这个在网络之间进行传输的序列化的机制
    @Override
    public void write(DataOutput out) throws IOException { //这个是hadoop的序列化的机制吧
      Text.writeString(out, url);  //将url用out这个对象写入到数据库当中
      Text.writeString(out, referrer);
      out.writeLong(time);
    }
    
    
    
    //这两个函数才是从数据库中读出一个字段，和将你的字段写入到数据库当中
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      this.url = resultSet.getString(1);
      this.referrer = resultSet.getString(2);
      this.time = resultSet.getLong(3);
    }
    
    @Override
    public void write(PreparedStatement statement) throws SQLException {
      statement.setString(1, url);
      statement.setString(2, referrer);
      statement.setLong(3, time);
    }
  }
  
  
  
  
  //这是写的另外一个表的操作的情况。上面的是上一个表的操作的情况
  /** Holds a &lt;url, pageview &gt; tuple */
  static class PageviewRecord implements Writable, DBWritable {
    String url;
    long pageview;
   
    public PageviewRecord(String url, long pageview) {
      this.url = url;
      this.pageview = pageview;
    }
    
    //这个readFields函数就是序列化的从类当中读出数据和写入数据
    @Override
    public void readFields(DataInput in) throws IOException {
      this.url = Text.readString(in);
      this.pageview = in.readLong();
    }
    
    //writable的序列化写的操作
    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      out.writeLong(pageview);
    }
    
    //这是从数据库当中读取出数据
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      this.url = resultSet.getString(1);
      this.pageview = resultSet.getLong(2);
    }
    
    @Override
    public void write(PreparedStatement statement) throws SQLException {
      statement.setString(1, url);
      statement.setLong(2, pageview);
    }
    
    //Reduce阶段往文件中写数据的时候用什么格式的去写
    @Override
    public String toString() {
      return url + " " + pageview;
    }
  }
  
  
  
  
  //这里是map的操作。
  //他的value的格式是AccessRecord
  /**
   * Mapper extracts URLs from the AccessRecord (tuples from db), 
   * and emits a &lt;url,1&gt; pair for each access record. 
   */
  static class PageviewMapper extends 
      Mapper<LongWritable, AccessRecord, Text, LongWritable> {
    
	  //
    LongWritable ONE = new LongWritable(1L);  //定义了一个值为 1 的longWritable的格式的数
    @Override
    public void map(LongWritable key, AccessRecord value, Context context)
        throws IOException, InterruptedException {
    	//这个value值是将数据库当中的一行读取了出来。然后在map阶段只是使用了url这个属性。我看过老师讲过的自己写的
    	//流量统计的算法的
      Text oKey = new Text(value.url);
      context.write(oKey, ONE);
    }
  }
  
  
  
  //将所有的value值的个数加一
  /**
   * Reducer sums up the pageviews and emits a PageviewRecord, 
   * which will correspond to one tuple in the db.
   */
  //哦。其实是只是从一个表中读取数据。之后将读取到的数据存到数据库的另外一个表当中
  static class PageviewReducer extends 
      Reducer<Text, LongWritable, PageviewRecord, NullWritable> {
    
    NullWritable n = NullWritable.get();
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, 
        Context context) throws IOException, InterruptedException {
      
      long sum = 0L;
      for(LongWritable value: values) {
        sum += value.get();
      }
      context.write(new PageviewRecord(key.toString(), sum), n);
    }
  }
  
  
  
  @Override
  //Usage DBCountPageView [driverClass dburl]
  public int run(String[] args) throws Exception {
    
	  //这两个值是定义连接数据库的驱动。使用MySql的驱动
    String driverClassName = DRIVER_CLASS;
    String url = DB_URL; //使用数据库的连接方式
    
    
    //如果在参数中传进了参数。将数据库的驱动传进来。
    if(args.length > 1) {
      driverClassName = args[0];
      url = args[1];
    }
    
    //使用driverClassName，url。对数据库的链接进行初始化
    initialize(driverClassName, url);
    
    //得到本地的hdfs的配置文件
    Configuration conf = getConf();

    //设置数据库的配置文件
    DBConfiguration.configureDB(conf, driverClassName, url); //这里才是真正的配置数据库的使用

    //设置job的对象
    Job job = Job.getInstance(conf);
        
    //设定job的名字
    job.setJobName("Count Pageviews of URLs");
    
    //设定job的主函数所在的类
    job.setJarByClass(DBCountPageView.class);
    
    //设定Map所在的类
    job.setMapperClass(PageviewMapper.class);
    
    //这里还设置了一个Combiner的类
    job.setCombinerClass(LongSumReducer.class);
    
    //设定reduce所在的类
    job.setReducerClass(PageviewReducer.class);

    //设置DB的数据的输入路径
    //	setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions, String orderBy, String... fieldNames)
    DBInputFormat.setInput(job, AccessRecord.class, "HAccess"
        , null, "url", AccessFieldNames);  //这个函数的作用还是不太明白

    
    //这里设置了输出路径。没有使用FileOutputFormat.setoutput 在这里就是告诉系统我想使用数据库作为输出
    DBOutputFormat.setOutput(job, "Pageview", PageviewFieldNames);//设定输出路径的表
    
    //设定Map输出的key的格式
    job.setMapOutputKeyClass(Text.class);
    
    //设定Map的Value的格式
    job.setMapOutputValueClass(LongWritable.class);

    //设定Reduce的输出key的格式
    job.setOutputKeyClass(PageviewRecord.class);
    
    //设定Reduce的输出的value的格式
    job.setOutputValueClass(NullWritable.class);
    
    int ret;//这就是获得job的最后的状态的。这个例子可以用来执行多个作业啊。
    try {
      ret = job.waitForCompletion(true) ? 0 : 1;
      
      boolean correct = verify();
      if(!correct) {
        throw new RuntimeException("Evaluation was not correct!");
      }
    } finally {
      shutdown();    
    }
    return ret;
  }

  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new DBCountPageView(), args);
    System.exit(ret);
  }

}
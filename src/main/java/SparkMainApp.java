import java.io.*;
import java.lang.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import com.databricks.spark.xml.XmlReader;
import java.util.Scanner;

public class SparkMainApp {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();

        SQLContext sqlContext = new SQLContext(spark);
        //************************* xml to parquet *******************
        Dataset<Row> df = (new XmlReader()).withRowTag("product").xmlFile(sqlContext, "/tmp/gov_old.xml");//read xml file into dataframe
        df.write().format("parquet").save("test.parquet"); //covert to parquet , write once , saved in local
        Dataset<Row> parquetFileDF = spark.read().parquet("test.parquet");//load parquet file in dataframe
        parquetFileDF.createOrReplaceTempView("parquetFile");//register df as temp sql view

        Dataset<Row> namesDF = spark.sql("SELECT * FROM parquetFile ");//query
        namesDF.show();
        String query;


        //********** the program that runs according user input*****************************************************

        System.out.println("Enter a number to run one of the following queries/options\n1.Select * From ParquetFile\n" +
                "2.Count of records\n3.Enter your query as a string\n Other keys will stop the program\n");
        Scanner s = new Scanner(System.in);
        int input = s.nextInt();
        while(input>0 && input<4)
        {
            switch (input){
                case 1:
                    namesDF = spark.sql("SELECT * FROM parquetFile ");
                    namesDF.show();
                    break;

                case 2: namesDF = spark.sql("SELECT * FROM parquetFile ");
                    System.out.println("Number of records in file = " + namesDF.count());
                break;

                case 3: System.out.println("Enter a query ..\nE.g  SELECT * FROM parquetFile \n      SELECT a001 FROM parquetFile where b394>4\n ");
                    Scanner sc = new Scanner(System.in);
                    query  = sc.nextLine();
                      System.out.println("Query is "+query);
                     try
                     {
                         namesDF = spark.sql(query);
                         namesDF.show();
                     }
                     catch(Exception e){
                         System.out.println("Exception occurred");
                     }

                    break;

            }
            System.out.println("Enter a number to run one of the following queries/options\n1.Select * From ParquetFile\n" +
                    "2.Count of records\n3.Enter your query as a string\n Other keys will stop the program\n");

            input = s.nextInt();
            if(input<1 || input>3)
                System.out.println("Bye World!!");
        }

        spark.stop();
    }
}

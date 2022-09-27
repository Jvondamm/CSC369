package com.example;

import java.io.*;
import java.util.*;
import com.opencsv.*;
import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.DecimalFormat;

public class App
{
    public static final String ADDRESS_CSV = "lab1/maven-datagen/src/main/java/com/example/addresses.csv";
    public static final String NAMES_CSV = "lab1/maven-datagen/src/main/java/com/example/names.csv";
    public static final String PRODUCT_CSV = "lab1/maven-datagen/src/main/java/com/example/products.csv";
    public static final String STORENAMES_TXT = "lab1/maven-datagen/src/main/java/com/example/storenames.txt";

    public static final int STORE_NUM = 100;
    public static final int CUSTOMER_NUM = 1000;
    public static final int SALES_NUM = 2000;
    public static final int PRODUCT_NUM = 100;
    public static final int LINEITEM_NUM = 4000;

    public static void main(String[] args) throws FileNotFoundException
    {
        List<String[]> addresses = readData(ADDRESS_CSV);
        List<String[]> names = readData(NAMES_CSV);
        List<String[]> products = readData(PRODUCT_CSV);

        ArrayList<String[]> store = new ArrayList<String[]>(STORE_NUM);
        ArrayList<String[]> customer = new ArrayList<String[]>(CUSTOMER_NUM);
        ArrayList<String[]> sales = new ArrayList<String[]>(SALES_NUM);
        ArrayList<String[]> product = new ArrayList<String[]>(PRODUCT_NUM);
        ArrayList<String[]> lineItem = new ArrayList<String[]>(LINEITEM_NUM);
        Map<String,Integer> oldStoreNames = new HashMap<String,Integer>();

        for(int i = 0; i < STORE_NUM; ++i)
        {
            String s = choose(new File(STORENAMES_TXT));
            if(!oldStoreNames.containsKey(s))
            {
                oldStoreNames.put(s, 0);

                Random rand = new Random();
                int randAddr = rand.nextInt(827879-1);
                String[] temp = {String.valueOf(i),
                                s,
                                addresses.get(randAddr)[3],
                                "Portland",
                                addresses.get(randAddr)[8],
                                "OR",
                                phoneNum()};
                store.add(temp);
            }
            else
            {
                --i;
            }

        }
        write("store.csv", store);

        for(int i = 0; i < CUSTOMER_NUM; ++i)
        {
            Random rand = new Random();
            int randName = rand.nextInt(1023-1);
            int randAddr = rand.nextInt(827879-1);
            String[] temp = {String.valueOf(i),
                            names.get(randName)[1],
                            birthDate(),
                            addresses.get(randAddr)[3],
                            "Portland",
                            addresses.get(randAddr)[8],
                            "OR",
                            phoneNum()};
            customer.add(temp);
        }
        write("customer.csv", customer);

        int c = 0;
        int s = 0;
        for(int i = 0; i < SALES_NUM; ++i)
        {
            String[] temp = {String.valueOf(i),
                            recentDate(),
                            time(),
                            String.valueOf(s),
                            String.valueOf(c)};
            ++s;
            ++c;
            if (s >= 100)
            {
                s = 0;
            }
            if (c >= 1000)
            {
                c = 0;
            }
            sales.add(temp);
        }
        write("sales.csv", sales);

        for(int i = 0; i < PRODUCT_NUM; ++i)
        {
            Random rand = new Random();
            int randProduct = rand.nextInt(731-1);
            String[] temp = {String.valueOf(i),
                            products.get(randProduct)[21],
                            products.get(randProduct)[1]};
            product.add(temp);
        }
        write("product.csv", product);

        s = 0;
        int p = 0;
        for(int i = 0; i < LINEITEM_NUM; ++i)
        {
            Random rand = new Random();
            int randQuantity = rand.nextInt(10-1);
            String[] temp = {String.valueOf(i),
                            String.valueOf(s),
                            String.valueOf(p),
                            String.valueOf(randQuantity)
                            };
            ++s;
            ++p;
            if (s >= 2000)
            {
                s = 0;
            }
            if (p >= 100)
            {
                p = 0;
            }
            lineItem.add(temp);
        }
        write("lineItem.csv", lineItem);
    }

    public static List<String[]> readData(String file)
    {
        List<String[]> data = new ArrayList<String[]>();
        try {
            FileReader filereader = new FileReader(file);
            CSVReader csvReader = new CSVReaderBuilder(filereader).build();
            data = csvReader.readAll();
            return data;
        }
        catch (Exception e) {
            e.printStackTrace();
            return data;
        }
    }

    public static String choose(File f) throws FileNotFoundException
    {
        String result = null;
        Random rand = new Random();
        int n = 0;
        for(Scanner sc = new Scanner(f); sc.hasNext(); )
        {
            ++n;
            String line = sc.nextLine();
            if(rand.nextInt(n) == 0)
            result = line;
        }
        return result;
    }

    public static String phoneNum()
    {
        Random rand = new Random();
        int num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8);
        int num2 = rand.nextInt(743);
        int num3 = rand.nextInt(10000);
        DecimalFormat df3 = new DecimalFormat("000");
        DecimalFormat df4 = new DecimalFormat("0000");
        return df3.format(num1) + "-" + df3.format(num2) + "-" + df4.format(num3);
    }

    public static String birthDate()
    {

        GregorianCalendar gc = new GregorianCalendar();
        int year = randBetween(1900, 2010);
        gc.set(gc.YEAR, year);
        int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
        gc.set(gc.DAY_OF_YEAR, dayOfYear);
        return(gc.get(gc.YEAR) + "/" + (gc.get(gc.MONTH) + 1) + "/" + gc.get(gc.DAY_OF_MONTH));
    }

    public static String recentDate()
    {
        GregorianCalendar gc = new GregorianCalendar();
        int year = randBetween(2022, 2022);
        gc.set(gc.YEAR, year);
        int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
        gc.set(gc.DAY_OF_YEAR, dayOfYear);
        return(gc.get(gc.YEAR) + "/" + (gc.get(gc.MONTH) + 1) + "/" + gc.get(gc.DAY_OF_MONTH));
    }

    public static String time()
    {
        Random random = new Random();
        int millisInDay = 24*60*60*1000;
        DateFormat format = new SimpleDateFormat("HH:mm:ss");
        return format.format(random.nextInt(millisInDay));
    }

    public static int randBetween(int start, int end)
    {
        return start + (int)Math.round(Math.random() * (end - start));
    }

    public static void write(String f, ArrayList<String[]> data)
    {
        try
        {
            FileWriter file = new FileWriter(f);
            PrintWriter write = new PrintWriter(file);
            for (String[] list: data)
            {
                for (String item: list)
                {
                    write.print(item + ",");
                }
                write.println();
        }
        write.close();
        }
        catch(IOException exe){
            System.out.println("Cannot create file");
        }
    }
}

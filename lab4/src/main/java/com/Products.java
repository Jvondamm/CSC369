package main.java.com;

import java.io.*;
import java.util.*;

public class Products {
    public static void main(String[] args) throws Exception
    {
        String line = "";
        try
        {
            BufferedReader br = new BufferedReader(new FileReader("./product.csv"));
            while ((line = br.readLine()) != null)
            {
                String[] items = line.split(",");
                System.out.println(items[2].trim());
            }
            br.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
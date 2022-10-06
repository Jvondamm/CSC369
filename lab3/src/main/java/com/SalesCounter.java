package main.java.com;

import java.io.*;
import java.util.*;

public class SalesCounter {
    public static void main(String[] args) throws Exception
    {
        long startTime = System.nanoTime();
        HashMap<String, Integer> sales = new HashMap<>();
        String line = "";
        int count = 0;

        try
        {
            BufferedReader br = new BufferedReader(new FileReader("./sales.csv"));
            while ((line = br.readLine()) != null)
            {
                String[] items = line.split(",");
                if (sales.containsKey(items[1]))
                {
                    count = sales.get(items[1]) + 1;
                }
                else
                {
                    count = 1;
                }
                sales.put(items[1], count);
            }
            br.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        ArrayList<String> SalesCount =  new ArrayList<>(sales.keySet());
        Collections.sort(SalesCount);
        FileWriter file = new FileWriter("sales.txt");
        for (String index : SalesCount) {
            file.write(index + " " + sales.get(index) + "\n");
        }
        file.close();
        System.out.println(System.nanoTime() - startTime);
    }
}

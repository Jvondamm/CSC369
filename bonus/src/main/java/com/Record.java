public class Record implements Comparable <Record>{
    private String name;
    private String city;
    private int price;

    public Record(String name, String city, int price) {
        this.name = name;
        this.city = city;
        this.price = price;
    }
    public String toString() {
        return "("+name+" "+city+" $" +price +" )";
    }
    public int compareTo(Record other) {
        if(this.price > other.price) return -1;
        if(this.price < other.price) return 1;
        return 0;
    }
}
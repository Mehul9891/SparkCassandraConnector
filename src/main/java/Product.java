import java.io.Serializable;
import java.util.List;

public class Product implements Serializable{

    int i ;
    String name;
    List<Integer> list;

    public Product(int i, String name, List<Integer> list) {
        this.i = i;
        this.name = name;
        this.list = list;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getList() {
        return list;
    }

    public void setList(List<Integer> list) {
        this.list = list;
    }
}

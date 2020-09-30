package ex05;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class customKey implements WritableComparable<customKey> {

    private String ano, unit, category;

    public customKey() {
    }

    public customKey(String ano, String unit, String category) {
        this.ano = ano;
        this.unit = unit;
        this.category = category;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        // escrever n e soma, nesta ordem!
        out.writeUTF(ano);
        out.writeUTF(unit);
        out.writeUTF(category);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Seguindo a mesma ordem do write, isto eh, n e depois soma

        ano = in.readUTF();
        unit = in.readUTF();
        category = in.readUTF();
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        customKey that = (customKey) o;
        return Objects.equals(ano, that.ano) &&
                Objects.equals(unit, that.unit) &&
                Objects.equals(category, that.category);
    }
    @Override
    public int hashCode() {
        return Objects.hash(ano, unit, category);
    }


    @Override
    public int compareTo(customKey o) {
        return 0;
    }
}

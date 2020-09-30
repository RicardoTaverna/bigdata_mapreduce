package exteste;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Auxiliar implements Writable {

    private Integer n;
    private Float preco;



    public Auxiliar(int n, float preco) {
        this.n = n;
        this.preco = preco;
    }

    public Auxiliar() {
    }

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }

    public Float getPreco() {
        return preco;
    }

    public void setPreco(Float preco) {
        this.preco = preco;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(n.toString());
        dataOutput.writeUTF(preco.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.n = Integer.parseInt(dataInput.readUTF());
        this.preco = Float.parseFloat(dataInput.readUTF());
    }

}

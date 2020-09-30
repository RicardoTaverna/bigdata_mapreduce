package ex05;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class mediaAnoComm2 implements Writable {
    //Atributos
    private int n;
    private float soma;

    public mediaAnoComm2() {
    }

    public mediaAnoComm2(int n, float soma) {
        this.n = n;
        this.soma = soma;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getSoma() {
        return soma;
    }

    public void setSoma(float soma) {
        this.soma = soma;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Seguindo a mesma ordem do write, isto eh, n e depois soma
        n = Integer.parseInt(in.readUTF());
        soma = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {

        // escrever n e soma, nesta ordem!
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(soma));

    }
}
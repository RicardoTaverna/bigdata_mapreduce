package ex05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class average_price {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/transactions.csv");

        // arquivo de saida
        Path output = new Path("output/ex05");

        // criacao do job e seu nome
        Job j = new Job(c, "most-comm-commodity");

        // Registro das classes
        j.setJarByClass(average_price.class); //classe do main
        j.setMapperClass(MapEx05.class); // classe do mapper
        j.setReducerClass(ReduceEx05.class); // classe do reduce

        // Definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx05 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Obter valor da linha
            String linha = value.toString();

            // Split por ;
            String[] colunas = linha.split(";");

            // Ignora cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // pegar ano como chave
            String flow = colunas[4];
            Text ano = new Text(colunas[1]);
            String pais = colunas[0];

            // valor de saida
            IntWritable valorSaida = new IntWritable(1);

            if(flow.equals("Export") && pais.equals("Brazil")){
                con.write(ano, valorSaida);
            }
        }

    }

    public static class ReduceEx05 extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text ano, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // Loop para somar todas as ocorrências

            int soma = 0;

            for (IntWritable vlr : values) {
                soma += vlr.get();
            }

            // Escreve os resultados finais no arquivo

            con.write(ano, new IntWritable(soma));

        }
    }
}

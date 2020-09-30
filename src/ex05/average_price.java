package ex05;


import ex04.average_commodities;
import ex04.mediaAnoComm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
        j.setCombinerClass(Combiner.class);

        // Definicao dos tipos de saida
        j.setOutputKeyClass(customKey.class);
        j.setOutputValueClass(mediaAnoComm2.class);
        j.setOutputKeyClass(customKey.class);
        j.setOutputValueClass(mediaAnoComm2.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx05 extends Mapper<LongWritable, Text, customKey, mediaAnoComm2> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Obter valor da linha
            String linha = value.toString();

            // Split por ;
            String[] colunas = linha.split(";");

            // Ignora cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // pegar ano como chave
            String flow = colunas[4];
            String pais = colunas[0];

            String ano = (colunas[1]);
            String unit = (colunas[7]);
            String category = (colunas[9]);


            float trade_usd = Float.parseFloat(colunas[5]);


            if( pais.equals("Brazil")){
                con.write(new customKey(ano,unit,category), new mediaAnoComm2(1,trade_usd));
            }
        }

    }

    public static class ReduceEx05 extends Reducer<customKey, mediaAnoComm2, Text, FloatWritable> {
        public void reduce(customKey key, Iterable<mediaAnoComm2> values, Context con) throws IOException, InterruptedException {
            // Loop para somar todas as ocorrências

            int somaN = 0;
            float somaSomas = 0.0f;

            for(mediaAnoComm2 obj:values){
                somaN += obj.getN();
                somaSomas += obj.getSoma();
            }
            // calculando a media
            float media = somaSomas / somaN;

            // emitir o resultado final (media = X)
            con.write(new Text(String.valueOf(key.getAno())), new FloatWritable(media));

        }
    }
    public static class Combiner extends Reducer<customKey, mediaAnoComm2, customKey, mediaAnoComm2> {

        public void reduce(customKey key, Iterable<mediaAnoComm2> valores, Context con) throws IOException, InterruptedException{
            // agrupar os valores em um objeto unico (n = soma dos ns vistos no map,

            int somaNs = 0;
            float somaTemps = 0.0f;

            for(mediaAnoComm2 obj : valores){
                somaNs += obj.getN();
                somaTemps += obj.getSoma();
            }

            // emitir <cahve, (somaNs, somaTemps>
            con.write(key,new mediaAnoComm2(somaNs, somaTemps));
        }
    }
}

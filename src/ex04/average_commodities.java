package ex04;


import advanced.customwritable.AverageTemperature;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class average_commodities {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/transactions.csv");

        // arquivo de saida
        Path output = new Path("output/ex04");

        // criacao do job e seu nome
        Job j = new Job(c, "average-commodities");

        // Registro das classes
        j.setJarByClass(average_commodities.class); //classe do main
        j.setMapperClass(MapEx04.class); // classe do mapper
        j.setReducerClass(ReduceEx04.class); // classe do reduce
        j.setCombinerClass(Combiner.class);


        // Definir os tipos de saida (Map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(mediaAnoComm.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx04 extends Mapper<LongWritable, Text, Text, mediaAnoComm> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Obter valor da linha
            String linha = value.toString();

            // Split por ;
            String[] colunas = linha.split(";");

            // Ignora cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // pegar ano como chave

            Text ano = new Text(colunas[1]);
            float trade_usd = Float.parseFloat(colunas[5]);

            // valor de saida
            con.write(ano, new mediaAnoComm(1, trade_usd));


        }

    }

    public static class ReduceEx04 extends Reducer<Text, mediaAnoComm, Text, FloatWritable> {
        public void reduce(Text ano, Iterable<mediaAnoComm> values, Context con) throws IOException, InterruptedException {
            // Loop para somar todas as ocorrências

            int somaN = 0;
            float somaSomas = 0.0f;

            for(mediaAnoComm obj:values){
                somaN += obj.getN();
                somaSomas += obj.getSoma();
            }
            // calculando a media
            float media = somaSomas / somaN;

            // emitir o resultado final (media = X)
            con.write(new Text(ano), new FloatWritable(media));

        }
    }

    public static class Combiner extends Reducer<Text, mediaAnoComm, Text, mediaAnoComm> {

        public void reduce(Text key, Iterable<mediaAnoComm> valores, Context con) throws IOException, InterruptedException{
            // agrupar os valores em um objeto unico (n = soma dos ns vistos no map,

            int somaNs = 0;
            float somaTemps = 0.0f;

            for(mediaAnoComm obj : valores){
                somaNs += obj.getN();
                somaTemps += obj.getSoma();
            }

            // emitir <cahve, (somaNs, somaTemps>
            con.write(key,new mediaAnoComm(somaNs, somaTemps));
        }
    }
}

package ex06;


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

public class price_type {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/teste.csv");

        // arquivo de saida
        Path output = new Path("output/ex06");

        // criacao do job e seu nome
        Job j = new Job(c, "price_type");

        // Registro das classes
        j.setJarByClass(price_type.class); //classe do main
        j.setMapperClass(MapEx06.class); // classe do mapper
        j.setReducerClass(ReduceEx06.class); // classe do reduce

        // Definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEx06 extends Mapper<LongWritable, Text, Text, Float> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // Obter valor da linha
            String linha = value.toString();

            // Split por ;
            String[] colunas = linha.split(";");

            // Ignora cabeçalho
            if (linha.startsWith("country_or_area")) return;

            // pegar ano como chave
            Text type = new Text(colunas[7]);
            String ano = colunas[1];
            float trade_usd = Float.parseFloat(colunas[5]);



            // valor de saida
            IntWritable valorSaida = new IntWritable(1);

            if(ano.equals("2016")){
                con.write(type, trade_usd);
            }



        }

    }

    public static class ReduceEx06 extends Reducer<Text, Float, Text, Float> {
        public void reduce(Text type, Float values, Context con) throws IOException, InterruptedException {
            // Loop para somar todas as ocorrências

            int soma = 0;
            String auxType = "Number of items";
            String auxType2 = "No Quantity";
            String auxType3 = "Weight in kilograms";
            String typeTxt = type.toString();

            float price = 0;
            if(typeTxt.equals(auxType)){
                if(price < values){
                    price = values;
                }
            }else if(typeTxt.equals(auxType2)){
                if(price < values){
                    price = values;
                }
            }else if(typeTxt.equals(auxType3)){
                if(price < values){
                    price = values;
                }
            }



            // Escreve os resultados finais no arquivo

            con.write(type, price);

        }
    }
}

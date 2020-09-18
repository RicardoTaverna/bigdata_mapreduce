package basic;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-stengrat");

        // Registro das classes
        j.setJarByClass(WordCount.class); //classe do main
        j.setMapperClass(MapForWordCount.class); // classe do mapper
        j.setReducerClass(ReduceForWordCount.class); // classe do reduce
        j.setCombinerClass(CombinerForWordCount.class); // classe do combiner!

        // Definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
    /**
     CLASSE de map
     4 tipos:
     - tipo da chave de entrada
     - tipo do valor da entrada
     - tipo da chave de saida
     - tipo da saida
     **/
    /**
     Lembrete: LongWritable, intwritable, text (floatwritable,etc) sao wrappers.
     wrappers permitem que o hadoop seja capaz de transferir os objetos pela rede e tambem salva-los em disco.

     nota: nos podemos usar os writables do hadoop, ou criar os nossos proprios writables ( vamo fazer isso na sequencia)

     **/

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //Lembrete: o map eh executado por bloco de arquivo
            //contudo, nos vamos ver que na realidade, o map eh executado por LINHA do arquivo


            //Minha linha esta em "value"
            // Converter a linha em string
            String linha = value.toString();

            // Quebrando em palavras
            String[] palavras = linha.split(" ");

            // Loop -> gerar as tuplas no formato <chave= palavra, valor = ocorrencia = 1>
            for(String p : palavras){
                // criando chave de saida
                Text chaveSaida = new Text(p);
                // criando valor de saida
                IntWritable valorSaida = new IntWritable(1);

                // emitir a tupla (chave, valor)
                con.write(chaveSaida,valorSaida);
            }


        }
    }

    /**
     classe de reduce
     4 tipos:
     - tipo da chave
     - tipo do valor de entrada
     - tipo da chave de saida
     - tipo do valor de saida
     **/
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        // A entrada do reduce sera (chave = palavra, lista de valores = lista de ocorrencias 1s)
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
                // Objetivo do reduce: fazer um loop para somar todas as ocorrencias de uma palavra
                // E no final, emitir a palavra com a ocorrencia ja somada
                int soma = 0;
                for(IntWritable vlr : values){
                    soma += vlr.get(); //get retorna um int
                }
                // criando tupla com o resultado final (palavra, soma)
                con.write(word, new IntWritable(soma));
                // Lembrete: no reduce, o write escreve em arquivo!

        }
    }

    public static class CombinerForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>{

        // O combiner eh muito parecido com um reduce, ele eh executado por CHAVE apos a execução de cada MAP
        // O combiner é executado por BLOCO, assim como o MAP
        public void reduce(Text word, Iterable<IntWritable> valores, Context con) throws IOException, InterruptedException {

            // o combiner vai somar as ocorrencias das palavras naquele bloco
            int soma = 0;
            for(IntWritable vlr : valores){
                soma += vlr.get();
            }

            con.write(word, new IntWritable(soma));
        }
    }

}

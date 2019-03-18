package job;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import mapRed.RA1Mapper;
import mapRed.RA1Reducer;


public class RA1 {
	
	
	public final static String METODO_PAGO = "filtrar_por_metodo_de_pago";
	public final static String VALOR_PAGO = "filtrar_por_valor_pagado";
	public final static String HORA = "filtrar_por_hora";
	public final static String DIA = "filtrar_por_dia";
	public final static String CANT_PAS = "filtrar_por_cantidad_de_pasajeros";
	public final static String LONGITUD = "filtrar_por_longitud";

	public static void main(String[] args)  {
		if(args.length<8){
			System.out.println("Se necesitan siete argumentos: \n 1. Carpeta de entrada,\n 2. Carpeta de salida,"
					+ " \n 3. Filtrar por medio de pago, \n 4. Filtrar por valor pagado, \n 5. Filtrar por hora,"
					+ " \n 6. Filtrar por día de la semana, \n 7. Filtrar por cantidad de pasajeros, \n 8. Filtrar por Longitud del viaje");
			System.exit(-1);
		}
		String entrada = args[0]; //carpeta de entrada
		String salida = args[1];//La carpeta de salida no puede existir
		String medPag = args[2];
		String valPag = args[3];
		String hora = args[4];
		String dia = args[5];
		String cantPas = args[6];
		String longitud = args[7];
		try {
			ejecutarJob(entrada, salida, medPag, valPag, hora, dia, cantPas, longitud);
		} catch (Exception e) { //Puede ser IOException, ClassNotFoundException o InterruptedException
			e.printStackTrace();
		} 
		
	}
	public static void ejecutarJob(String entrada, String salida, String medPag, String valPag, String hora, String dia, String cantPas, String longitud) throws IOException,ClassNotFoundException, InterruptedException
	{
		/**
		 * Objeto de configuración, dependiendo de la versión de Hadoop 
		 * uno u otro es requerido. 
		 * */
		Configuration conf = new Configuration();	
		Job wcJob=Job.getInstance(conf, "RA1 Job");
		conf.set(METODO_PAGO, medPag);
		conf.set(VALOR_PAGO, valPag);
		conf.set(HORA, hora);
		conf.set(DIA, dia);
		conf.set(CANT_PAS, cantPas);
		conf.set(LONGITUD, longitud);
		wcJob.setJarByClass(RA1.class);
		//////////////////////
		//Mapper
		//////////////////////
		
		wcJob.setMapperClass(RA1Mapper.class);	
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(IntWritable.class);
		///////////////////////////
		//Reducer
		///////////////////////////
		wcJob.setReducerClass(RA1Reducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		
		///////////////////////////
		//Input Format
		///////////////////////////
		//Advertencia: Hay dos clases con el mismo nombre, 
		//pero no son equivalentes. 
		//Se usa, en este caso, org.apache.hadoop.mapreduce.lib.input.TextInputFormat
		TextInputFormat.setInputPaths(wcJob, new Path(entrada));
		wcJob.setInputFormatClass(TextInputFormat.class); 
		
		////////////////////
		///Output Format
		//////////////////////
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		wcJob.waitForCompletion(true);
		System.out.println(wcJob.toString());
	}
}
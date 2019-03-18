package mapRed;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import job.RA1;

public class RA1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(RA1Mapper.class.getName());

	private Text linea = new Text();

	private HashMap<Integer,String> validar;   
	private Configuration con ;


	private String distancia = "Distancia";
	private String tipo_pago = "Tipo_Pago";
	private String Costo_Viaje = "Costo_Viaje";
	private String num_pasajeros = "Num_Pasajeros";

	public enum Dias{
		DOM, LUN, MAR, MIE, JUE, VIE, SAB 
	}


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		validar = new HashMap<Integer, String>();


		linea.set(value);
		String lin = linea.toString();
		String[] head = lin.split(",");
		con = context.getConfiguration();
		FileSplit fs = ( (FileSplit)context.getInputSplit());
		String file=fs.getPath().toString();
		if(lin.contains("endor")||lin.contains("ispatc")) {
			for(int i = 0; i<head.length; i++) {
				validateOrigen(head[i],i);
				validateFin(head[i],i);
				try {
					if(!con.get(RA1.LONGITUD).equals("-1")){
						validateLongitud(head[i],i);
					}
					if(!con.get(RA1.METODO_PAGO).equals("-1")) {
						validatePayType(head[i],i);
					}
					if(!con.get(RA1.VALOR_PAGO).equals("-1")) {
						validatePayment(head[i],i);
					}
					if(!con.get(RA1.CANT_PAS).equals("-1")) {
						validatePasajeros(head[i],i);
					}
				}catch(Exception e) {
					System.out.println(e);
				}
			}



			String rta = "";
			for(Integer k: validar.keySet()) {
				rta+=validar.get(k) + k +";";
			}
			con.set(file, rta);
			context.write(new Text("Head"), new IntWritable(1));

		}
		else {

			try {
				String par = con.get(file);
				if(par!=null) {
					//System.out.println("PARSER: "+par);


					String[] kv = par.split(";");

					for (int i = 0; i < kv.length; i++) {
						String[] kv1 = kv[i].split(": ");
						validar.put(Integer.parseInt(kv1[1]), kv1[0]);
					}
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

					//filas[i-1]="";
					Text tex = new Text();
					try {
						Date date =sdf.parse(head[1]);

						Calendar calendar = Calendar.getInstance();
						calendar.setTime(date);
						//DiaLocalizacion dl= new DiaLocalizacion(calendar.DAY_OF_WEEK,Integer.parseInt(cols[4]) );
						String[] str = file.split("_")[0].split("/");
						String s = str[str.length-1];
						System.out.println("Dia: "+calendar.get(Calendar.DAY_OF_WEEK));
						System.out.println(Dias.values()[calendar.get(Calendar.DAY_OF_WEEK)-1]);
						String ans = s+" Día: "+Dias.values()[calendar.get(Calendar.DAY_OF_WEEK)-1]+ " Hora: " +calendar.get(Calendar.HOUR);
						for(Integer k: validar.keySet()) {
							String temp = head[k];

							if(validar.get(k).equals(distancia)) {
								if(con.get(RA1.LONGITUD).equals("0")) {
									ans+=validar.get(k)+": "+temp+";";
								}else
								if(!temp.equals( con.get(RA1.LONGITUD))|| con.get(RA1.LONGITUD).equals("-1") ) {
									return;
								}
							}

							else if(validar.get(k).equals(tipo_pago)) {
								if(con.get(RA1.METODO_PAGO).equals("0")) {
									ans+=validar.get(k)+": "+temp+";";
								}else 
								if(!temp.equals( con.get(RA1.METODO_PAGO))|| con.get(RA1.METODO_PAGO).equals("-1")) {
									return;
								}
							}else if(validar.get(k).equals(Costo_Viaje)) {
								if(con.get(RA1.VALOR_PAGO).equals("0")) {
									ans+=validar.get(k)+": "+temp+";";
								}else
								if(!temp.equals( con.get(RA1.VALOR_PAGO))|| con.get(RA1.VALOR_PAGO).equals("-1")) {
									return;
								}
							}else if(validar.get(k).equals(num_pasajeros)) {
								if(con.get(RA1.CANT_PAS).equals("0")) {
									ans+=validar.get(k)+": "+temp+";";
								}else
								if(!temp.equals( con.get(RA1.CANT_PAS))|| con.get(RA1.CANT_PAS).equals("-1")) {
									return;
								}
							}else {

								if((head[k].contains("-7")||head[k].contains("4"))&&(head[k].length()>7)) {
									if(head[k].contains("-")) {
										temp = head[k].substring(0, 6);

									}else {
										temp = head[k].substring(0, 5);

									}
								}
								ans+=validar.get(k)+": "+temp+";";

							}






						}
						tex.set(ans);

						context.write(tex, new IntWritable(1));
					}catch(Exception e) {
						e.printStackTrace();
					}


				}
			}catch(Exception e) {
			}
		}
	}

	private  void validateOrigen(String linea, int i) {
		if((linea.contains("PU")||(linea.equalsIgnoreCase("LocationID")))){
			validar.put(i, "OrigenID: ");

		}
		else {
			if((linea.contains("lon")||linea.contains("Lon"))&&(linea.contains("Pickup")||linea.contains("pickup")||linea.contains("Start"))) {

				validar.put(i, "OrigenLon: ");

			}

			if((linea.contains("lat")||linea.contains("Lat"))&&(linea.contains("Pickup")||linea.contains("pickup")||linea.contains("Start"))) {

				validar.put(i, "OrigenLat: ");
			}
		}
	}

	private  void validateFin(String linea, int i) {
		if(linea.contains("DO")){
			validar.put(i, "DestinoID: ");

		}
		else {
			if((linea.contains("lon")||linea.contains("Lon"))&&(linea.contains("Dropoff")||linea.contains("dropoff")||linea.contains("End"))) {

				validar.put(i, "DestinoLon: ");
			}

			if((linea.contains("lat")||linea.contains("Lat"))&&(linea.contains("Dropoff")||linea.contains("dropoff")||linea.contains("End"))) {

				validar.put(i, "DestinoLat: ");
			}
		}
	}


	private  void validateLongitud(String linea, int i) {
		if((linea.equalsIgnoreCase(" TRIP_DISTANCE")||linea.equalsIgnoreCase("TRIP_DISTANCE"))){
			validar.put(i, distancia+": ");
		}

	}
	private  void validatePayType(String linea, int i) {
		if(linea.equalsIgnoreCase("PAYMENT_TYPE")||linea.equalsIgnoreCase(" PAYMENT_TYPE")) {
			validar.put(i, tipo_pago+": ");
		}
		
	}
	private  void validatePayment(String linea, int i) {
		if(linea.equalsIgnoreCase("FARE_AMOUNT")||linea.equalsIgnoreCase(" FARE_AMOUNT")||linea.equalsIgnoreCase(" FARE_AMT")) {
			validar.put(i, Costo_Viaje+": ");
		}
		
	}
	private  void validatePasajeros(String linea, int i) {
		if((linea.equalsIgnoreCase(" PASSENGER_COUNT")||linea.equalsIgnoreCase("PASSENGER_COUNT"))){
			validar.put(i, num_pasajeros+": ");
		}
		
	}
}
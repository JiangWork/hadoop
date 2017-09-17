package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;

/**
 * Execute the shell script to get node information.
 * 
 * @author jiangzhao
 *
 */
public class NodeInformationService extends AuxiliaryService {

	private static final Log LOG = LogFactory
			.getLog(NodeInformationService.class);

	public static final String NODE_INFORMATION_SERVICEID = "node_information";
	private int port;

	private volatile boolean stop = false;
	private Thread worker;

	public NodeInformationService() {
		super(NODE_INFORMATION_SERVICEID);
	}

	public NodeInformationService(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void initializeApplication(
			ApplicationInitializationContext initAppContext) {
		// TODO Auto-generated method stub

	}

	@Override
	public void stopApplication(ApplicationTerminationContext stopAppContext) {
		// TODO Auto-generated method stub

	}

	/**
	 * Serialize the shuffle port into a ByteBuffer for use later on.
	 * 
	 * @param port
	 *            the port to be sent to the ApplciationMaster
	 * @return the serialized form of the port.
	 */
	public static ByteBuffer serializeMetaData(int port) throws IOException {
		// TODO these bytes should be versioned
		DataOutputBuffer port_dob = new DataOutputBuffer();
		port_dob.writeInt(port);
		return ByteBuffer.wrap(port_dob.getData(), 0, port_dob.getLength());
	}

	@Override
	public ByteBuffer getMetaData() {
		// TODO Auto-generated method stub
		try {
			return serializeMetaData(port);
		} catch (IOException e) {
			LOG.error("Error during getMeta", e);
			return null;
		}
	}

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		port = 32100;
		stop = false;
		worker = new Thread() {
			public void run() {
				setName("NodeInformationService");
				LOG.info("Starting " + getName());
				ServerSocket serverSocket;
				try {
					serverSocket = new ServerSocket(port);
				} catch (IOException e) {
					LOG.error(e);
					return;
				}
				while (!stop) {
					Socket clientSocket = null;
					String result = null;
					try {
						clientSocket = serverSocket.accept();
					} catch (Exception e) {
						LOG.error("Error inital socket connection.");
						continue;
					}
					try {
						InputStream is = clientSocket.getInputStream();
						ObjectInputStream ois = new ObjectInputStream(is);
						String applicationId = (String) ois.readObject();
						File temp = File.createTempFile("bash", ".sh");
						BufferedWriter bw = new BufferedWriter(new FileWriter(
								temp));
						bw.write(shellScript(applicationId));
						bw.close();
						StringBuilder sb = new StringBuilder();
						ProcessBuilder builder = new ProcessBuilder(
								"/bin/bash", temp.getAbsolutePath());
						Process proc = builder.start();
						BufferedReader br = new BufferedReader(
								new InputStreamReader(proc.getInputStream()));
						String line = null;
						while ((line = br.readLine()) != null) {
							sb.append(line);
							sb.append("\n");
						}
						sb.append("\n\n\n");
						br = new BufferedReader(new InputStreamReader(
								proc.getErrorStream()));
						line = null;
						while ((line = br.readLine()) != null) {
							sb.append(line);
							sb.append("\n");
						}
						proc.waitFor();
						result = sb.toString();
					} catch (Exception e) {
						LOG.error(e.getMessage(), e);
						result = e.getMessage();
					}
					ObjectOutputStream oos;
					try {
						oos = new ObjectOutputStream(
								clientSocket.getOutputStream());
						oos.writeObject(result);
					} catch (IOException e) {
						LOG.error(e.getMessage(), e);
					}
					try {
						clientSocket.close();
					} catch (IOException e) {
						LOG.error(e.getMessage(), e);
					}
					LOG.info("Processed request from "
							+ clientSocket.getInetAddress());
				}
				LOG.info("Exiting " + getName());
			}
		};
		super.serviceInit(new Configuration(conf));
	}

	@Override
	protected void serviceStart() throws Exception {
		worker.start();
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
		stop = true;
		worker.interrupt();
		worker.join();
		super.serviceStop();
	}

	private String shellScript(String applicationId) {
		BashBuilder bb = new BashBuilder();
		bb.addLine("#!/bin/bash");
		bb.addLine("show() {");
		bb.addLine(" echo \"\" ");
		bb.addLine(" echo \"#####################################${1} `date`######################################\"");
		bb.addLine("}");
		bb.addLine("show \"Disk Usage\"");
		bb.addLine("df -ha 2>&1");
		bb.addLine("show \"Top threads\"");
		bb.addLine("top -b -n 1 2>&1");
		bb.addLine("show \"Process\"");
		bb.addLine("ps -ef 2>&1");
		bb.addLine("");
		bb.addLine(String.format(
				"PIDLIST=`jps -lv | grep %s | awk  '{print $1}'`",
				applicationId));
		bb.addLine("for pid in $PIDLIST");
		bb.addLine("do");
		bb.addLine("show \"PS $pid\"");
		bb.addLine("ps -ef | grep $pid | grep -v grep ");
		bb.addLine("show \"Java Stacks $pid\"");
		bb.addLine("timeout 10s jstack $pid 2>&1");
		bb.addLine("show \"Living Objects $pid\"");
		bb.addLine("timeout 10s jmap -histo:live $pid |  head -n 20 ");
		bb.addLine("show \"GC Counts $pid\"");
		bb.addLine("timeout 10s jstat  -gcutil $pid 100 10 ");
		bb.addLine("done");
		return bb.collect();
	}

	public static class BashBuilder {
		private StringBuilder sb = new StringBuilder();

		public BashBuilder addLine(String line) {
			sb.append(line);
			sb.append("\n");
			return this;
		}

		public String collect() {
			return sb.toString();
		}
	}
}

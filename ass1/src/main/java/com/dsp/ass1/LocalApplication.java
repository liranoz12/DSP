package com.dsp.ass1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class LocalApplication {

    private static final Logger logger = Logger.getLogger(LocalApplication.class.getName());

    private static void WriteToFile(String fileName, String info) throws FileNotFoundException {
        PrintWriter out;

        try {
            out = new PrintWriter(fileName);
        } catch (FileNotFoundException e) {
            logger.severe(e.getMessage());
            return;
        }

        out.println(info);
        out.close();
        logger.info("results: " + fileName);
    }


    private static String StringToHTMLString(String info) {

        StringBuilder content = new StringBuilder();
        content.append("<!DOCTYPE html>\n");
        content.append("<html>\n");
        content.append("<head>\n");
        content.append("<title>Results</title>\n");
        content.append("</head>\n");
        content.append("<body>\n");

        String[] split = info.split("\n");
        for (int i=0 ; i<split.length; i++) {
            content.append(split[i]);
            content.append("\n<br>\n");
        }

        content.append("</body>\n");
        content.append("</html>\n");
        return content.toString();
    }


    // Reads a file and returns its content as a string.
    private static String readFileAsString(String filePath) throws IOException {
        StringBuffer fileData = new StringBuffer();
        BufferedReader reader;
        char[] buf = new char[1024];
        int numRead = 0;

        try {
            reader = new BufferedReader(new FileReader(filePath));
        }
        catch (FileNotFoundException e) {
            logger.severe(e.getMessage());
            return null;
        }

        while((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }

        try {
            reader.close();
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }

        return fileData.toString();
    }


    public static void main (String[] args) throws IOException {

        Utils.setLogger(logger);
        logger.info("starting.");

        // Exit if no arguments were given.
        if (args.length == 0) {
            logger.severe("no input: closing.");
            return;
        }

        // Read input file.
        String info = readFileAsString(args[0]);
        if (info == null) {
            return;
        }

        // Load credentials (needed for connecting to AWS).
        AWSCredentials creds = Utils.loadCredentials();
        if (creds == null) {
            return;
        }

        String uploadLink,
               resultContent,
               finishLink;

        AmazonS3 s3 = new AmazonS3Client(creds);
        AmazonSQS sqsLocal = new AmazonSQSClient(creds);

        uploadLink = Utils.uploadFileToS3(s3, "input.txt", info);
        Utils.sendMessage(sqsLocal, Utils.localUrl, "new task\t" + uploadLink);
        ReceiveMessageRequest req = new ReceiveMessageRequest(Utils.localUrl);

        List<Message> msgs;
        Message msg;

        // Process messages indefinitely until manager is finished working for us.
        msgs = Utils.getMessages(req, sqsLocal);
        while (true) {
            // Sleep if no messages arrived, and retry to re-fetch new ones afterwards.
            if (msgs == null || msgs.size() == 0) {
                logger.info("no messages, sleeping.");

                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch(InterruptedException e) {
                    logger.severe(e.getMessage());
                    return;
                }

            // Process top message in queue if there are any messages.
            } else {
                msg = msgs.get(0);
                String body = msg.getBody();
                logger.info("received: " + body);

                // TODO put this in ProcessMessage() function or something,
                // shouldn't be in main.
                String[] parts = msg.getBody().split("\t");
                if (parts.length > 0 && parts[0].equals("done task")){
                    finishLink = parts[1];
                    break;
                }
                else {
                    logger.info("ignoring: " + body);
                }
            }

            msgs = Utils.getMessages(req, sqsLocal);
        }

        // Create <html> summary file.
        resultContent = Utils.LinkToString(finishLink);
        if (resultContent == null) {
            return;
        }

        WriteToFile("results.html", StringToHTMLString(resultContent));
        logger.info("finishing.");
    }
}

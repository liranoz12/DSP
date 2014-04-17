package com.dsp.ass1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.net.MalformedURLException;
import java.net.URL;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSCredentials;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class LocalApplication {

    private static final Logger logger = Logger.getLogger(LocalApplication.class.getName());

    private static void WriteToFile(String fileName, String info) {
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


    // Downloads a URL and returns it's content wrapped in an <html> String.
    private static String LinkToHTMLString(String link) {
        URL url;
        String line;
        StringBuilder content = new StringBuilder();
        InputStream is;
        BufferedReader br;

        try {
            url = new URL(link);
        } catch (MalformedURLException e) {
            logger.severe(e.getMessage());
            return null;
        }

        try {
            is = url.openStream();
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return null;
        }

        br = new BufferedReader(new InputStreamReader(is));

        content.append("<!DOCTYPE html>\n");
        content.append("<html>\n");
        content.append("<head>\n");
        content.append("<title>Results</title>\n");
        content.append("</head>\n");
        content.append("<body>\n");

        try {
            while ( (line = br.readLine()) != null) {
                content.append(line);
                content.append("<br \\>\n");
            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return null;
        }

        content.append("</body>\n");
        content.append("</html>\n");

        try {
            br.close();
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }

        try {
            is.close();
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }

        return content.toString();
    }


    // Reads a file and returns its content as a string.
    private static String readFileAsString(String filePath) {
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

        try {
            while((numRead = reader.read(buf)) != -1) {
                String readData = String.valueOf(buf, 0, numRead);
                fileData.append(readData);
            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return null;
        }

        try {
            reader.close();
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }

        return fileData.toString();
    }


    public static String handleMessage(Message msg, AmazonS3 s3) {
        String body = msg.getBody();

        logger.info("received: " + body);

        String[] parts = msg.getBody().split("\t");

        if (parts[0].equals("done task") && parts.length >= 1) {
            return parts[1];
        } else {
            logger.info("ignoring: " + body);
            return null;
        }
    }


    public static Instance getOrCreateManager(AWSCredentials creds) {
        // Get manager if it exists, or create a new one if not.
        AmazonEC2 ec2 = new AmazonEC2Client(creds);
        DescribeInstancesRequest instanceReq = new DescribeInstancesRequest();
        Filter managerFilter = new Filter("tag:Name").withValues("manager"),
               activeFilter = new Filter("instance-state-code").withValues("0", "16");  // 0||16 == pending||running
        DescribeInstancesResult result = ec2.describeInstances(instanceReq.withFilters(managerFilter, activeFilter));
        List<Reservation> reservations = result.getReservations();
        List<Instance> instances;

        if (reservations.size() > 0) {
            for (Reservation reservation : reservations) {
                instances = reservation.getInstances();
                logger.info(instances.get(0).getTags().get(0).getValue());
                if (instances.size() > 0) {
                    logger.info("Existing manager found.");
                    return instances.get(0);
                }
            }
        }

        logger.info("No manager exists, creating new one.");
        RunInstancesResult instanceResults = Utils.createAmiFromSnapshot(1, "manager", creds);
        if (instanceResults == null) {
            logger.severe("Couldn't create manager.");
            return null;
        }

        Instance manager = instanceResults.getReservation().getInstances().get(0);

        // Tag manager with name "manager".
        CreateTagsRequest tagReq = new CreateTagsRequest();
        tagReq.withResources(manager.getInstanceId())
            .withTags(new Tag("Name", "manager"));

        ec2.createTags(tagReq);

        // TODO execute manager code (run java app)

        return manager;
    }


    // TODO turn off manager if given as args[1] or something.
    public static void main (String[] args) {
        Utils.setLogger(logger);

        logger.info("starting.");

        // Exit if no arguments were given.
        if (args.length == 0) {
            logger.severe("no input: closing.");
            return;
        }

        // Read input.
        String info = readFileAsString(args[0]);
        if (info == null) {
            return;
        }

        // Load credentials (needed for connecting to AWS).
        AWSCredentials creds = Utils.loadCredentials();
        if (creds == null) {
            return;
        }

        // Start manager.
        Instance manager = getOrCreateManager(creds);
        if (manager == null) {
            return;
        }

        // Upload new mission and inform manager.
        AmazonS3 s3 = new AmazonS3Client(creds);
        AmazonSQS sqs = new AmazonSQSClient(creds);

        String uploadLink = Utils.uploadFileToS3(s3, "input.txt", info),
               finishedLink;

        Utils.sendMessage(sqs, Utils.localUrl, "new task\t" + uploadLink);

        // Process messages indefinitely until manager is finished working for us.
        ReceiveMessageRequest req = new ReceiveMessageRequest(Utils.localUrl);
        List<Message> msgs = Utils.getMessages(req, sqs);;
        Message msg;
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
                finishedLink = handleMessage(msg, s3);
                if (finishedLink != null) {
                    // Only handled messages are deleted.
                    Utils.deleteTaskMessage(msg, Utils.localUrl, sqs);
                    break;
                }
            }

            msgs = Utils.getMessages(req, sqs);
        }

        // Create <html> summary file.
        String resultContent = LinkToHTMLString(finishedLink);
        if (resultContent == null) {
            return;
        }

        WriteToFile("results.html", resultContent);
        logger.info("finishing.");
    }
}

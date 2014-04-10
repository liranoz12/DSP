package com.dsp.ass1;

import java.util.List;
import java.util.logging.Logger;

import java.net.URL;

import java.io.IOException;
import java.io.FileNotFoundException;

import java.io.File;
import java.io.PrintWriter;
import java.awt.image.BufferedImage;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.pdfbox.util.PDFText2HTML;

import org.apache.commons.io.FilenameUtils;

import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Worker {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());

    public static PDDocument getDocument(String url) {
        PDDocument doc;

        try {
            doc = PDDocument.load(new URL(url));
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return null;
        }

        return doc;
    }


    public static void toImage(PDPage page, String base) {
        String fileName = base + ".png";
        logger.info("image: " + fileName);

        try {
            BufferedImage img = page.convertToImage();
            File outputFile = new File(fileName);
            ImageIO.write(img, "png", outputFile);
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }
    }


    public static void toHTML(PDDocument doc, String base) {
        String fileName = base + ".html";
        logger.info("html: " + fileName);

        String result;
        PrintWriter out;
        PDFTextStripper stripper;

        try {
            stripper = new PDFText2HTML("utf-8");
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return;
        }

        try {
            result = stripper.getText(doc).trim();
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return;
        }

        try {
            out = new PrintWriter(fileName);
        } catch (FileNotFoundException e) {
            logger.severe(e.getMessage());
            return;
        }

        out.println(result);
        out.close();
    }


    public static void toText(PDDocument doc, String base) {
        String fileName = base + ".txt";
        logger.info("text: " + fileName);

        PDFTextStripper stripper;
        String pageText;
        PrintWriter out;

        try {
            stripper = new PDFTextStripper();
            stripper.setStartPage(1);
            stripper.setEndPage(1);
            pageText = stripper.getText(doc);
        } catch (IOException e) {
            logger.severe(e.getMessage());
            return;
        }

        try {
            out = new PrintWriter(fileName);
        } catch (FileNotFoundException e) {
            logger.severe(e.getMessage());
            return;
        }

        out.println(pageText);
        out.close();
    }


    public static void handleDocument(String action, String url) {
        logger.info("handling: " + action + "\t" + url);

        String base = FilenameUtils.getBaseName(url);  // url file base name.
        PDDocument doc = getDocument(url);

        if (doc == null) {
            return;
        } else {
            if (action.equals("ToImage")) {
                PDPage page = (PDPage) doc.getDocumentCatalog().getAllPages().get(0);
                toImage(page, base);
            }
            else if (action.equals("ToText")) {
                toText(doc, base);
            }
            else if (action.equals("ToHTML")) {
                toHTML(doc,base);
            }
        }

        try {
            doc.close();
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }

        logger.info("finished handling: " + action + "\t" + url);
    }


    public static void deleteTaskMessage(Message msg, String sqsUrl, AmazonSQS sqs) {
        String handle = msg.getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(sqsUrl, handle));
        logger.info("deleted: " + msg.getBody());
    }


    public static void sendFinishedMessage(Message msg, String sqsUrl, AmazonSQS sqs) {
        String[] splitter= msg.getBody().split("\t");
        String action = splitter[0],
               url = splitter[1],
               reply = "done PDF task " + action + " " + url;  // TODO need to add s3 location of result, according to instructions.

        sqs.sendMessage(new SendMessageRequest(sqsUrl, reply));
        logger.info(reply);
    }


    public static boolean handleMessage(Message msg) {
        String body = msg.getBody();
        logger.info("received: " + body);

        String[] parts = msg.getBody().split("\t");
        String type, action, url;

        if (parts.length == 3) {
            type = parts[0];
            action = parts[1];
            url = parts[2];
        } else {
            logger.info("ignoring: " + body);
            return false;
        }

        if (type.equals("new PDF task")) {
            handleDocument(action, url);
            return true;
        } else {
            logger.info("ignoring: " + body);
            return false;
        }
    }


    public static List<Message> getMessages(ReceiveMessageRequest req, AmazonSQS sqs) {
        List<Message> msgs = sqs.receiveMessage(req).getMessages();
        logger.info("sqs messages received.");
        return msgs;
    }


    public static void main(String[] args) throws Exception {
        // Use Ireland region.
        String missionsUrl = "https://sqs.eu-west-1.amazonaws.com/340657073537/tasks",
               finishedUrl = "https://sqs.eu-west-1.amazonaws.com/340657073537/finished",
               bucketName = "dsp-ass1";

        AWSCredentials creds = new PropertiesCredentials(
                Worker.class.getResourceAsStream("/AWSCredentials.properties"));

        AmazonSQS sqsMissions = new AmazonSQSClient(creds),
                  sqsFinished = new AmazonSQSClient(creds);

        AmazonS3 s3 = new AmazonS3Client(creds);

        ReceiveMessageRequest req;

        List<Message> msgs;

        try {
            req = new ReceiveMessageRequest(missionsUrl);
        } catch (AmazonServiceException ase) {
            logger.severe(ase.getMessage());
            return;
        } catch (AmazonClientException ace) {
            logger.severe(ace.getMessage());
            return;
        }

        msgs = getMessages(req, sqsMissions);
        while (msgs.size() > 0) {
            Message msg = msgs.get(0);
            if (handleMessage(msg)) {
                deleteTaskMessage(msg, missionsUrl, sqsMissions);
                sendFinishedMessage(msg, finishedUrl, sqsFinished);
            }

            msgs = getMessages(req, sqsMissions);
        }

        logger.info("finished messages.");
    }
}

import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.util.List;
import java.util.Random;

public class CarRecognition {

    public static void main(String[] args) {


        String[] myList;

        Random random = new Random();
        final String USAGE = "\n" +
                "To run this example, supply the name of a bucket to list!\n" +
                "\n" +
                "Ex: ListObjects <bucket-name>\n";

//        if (args.length < 1) {
//            System.out.println(USAGE);
//            System.exit(1);
//        }

//        String bucket_name = args[0];
//        String bucket_name = "testing-bucket-brian";
        String bucket_name = "njit-cs-643";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/843440522682/testing.fifo";


        System.out.format("Objects in S3 bucket %s:\n", bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
        List<S3ObjectSummary> objects = result.getObjectSummaries();


        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion("us-east-1").build();

        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("us-east-1").build();



        for (S3ObjectSummary os : objects) {
            System.out.println(os.getKey());
//            System.out.println("hello");
            String photo = os.getKey();

            DetectLabelsRequest request = new DetectLabelsRequest()
                    .withImage(new Image()
                            .withS3Object(new S3Object()
                                    .withName(photo).withBucket(bucket_name)))
                    .withMaxLabels(10)
                    .withMinConfidence(80F);

            try {
                DetectLabelsResult resultRek = rekognitionClient.detectLabels(request);
                List <Label> labels = resultRek.getLabels();

                System.out.println("Detected labels for " + photo);
                for (Label label: labels) {
                    if( label.getName().equals("Car")){
                        System.out.println(label.getName() + ": " + label.getConfidence().toString());

                        // SQS message

                        float f= random.nextFloat();

                        SendMessageRequest send_msg_request = new SendMessageRequest().withMessageGroupId("cs643").withMessageDeduplicationId(photo + f)
                                .withQueueUrl(queueUrl)
                                .withMessageBody(photo)
                                .withDelaySeconds(0);
                        sqs.sendMessage(send_msg_request);

                    }

                    else{
//                    System.out.println(label.getName().toString());
//                    System.out.println(label.getName() + ": " + label.getConfidence().toString());
//                        System.out.println("Not a Car!");
                        continue;
                    }


                }
            } catch(AmazonRekognitionException e) {
                e.printStackTrace();
            }

        }

        float j = random.nextFloat();

        SendMessageRequest send_msg_request = new SendMessageRequest().withMessageGroupId("cs643").withMessageDeduplicationId("a" + j)
                .withQueueUrl(queueUrl)
                .withMessageBody("done")
                .withDelaySeconds(0);
        sqs.sendMessage(send_msg_request);



    }
}

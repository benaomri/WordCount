import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.log4j.BasicConfigurator;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

public class ExtractLinesFromS3 {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().getProperties().setProperty("fs.s3a.access.key", credentialsProvider.getCredentials().getAWSAccessKeyId());
        pigServer.getPigContext().getProperties().setProperty("fs.s3a.secret.key", credentialsProvider.getCredentials().getAWSSecretKey());
        pigServer.registerQuery("data = LOAD 's3a://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data' USING TextLoader() AS (line:chararray);");        TupleFactory tupleFactory = TupleFactory.getInstance();
        pigServer.registerQuery("DUMP data;");
        DataBag data = (DataBag) pigServer.openIterator("data").next().get(0);
        for (Tuple tuple : data) {
            String field1 = (String) tuple.get(0);
            int field2 = (int) tuple.get(1);
            float field3 = (float) tuple.get(2);
            // Process the field values as needed
        }

        // Set the bucket and file names
        String bucketName = "datasets.elasticmapreduce";
        String fileName = "ngrams/books/20090715/eng-us-all/3gram/data";
    }

}

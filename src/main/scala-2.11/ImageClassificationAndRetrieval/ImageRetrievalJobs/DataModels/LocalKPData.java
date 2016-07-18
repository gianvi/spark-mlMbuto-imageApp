package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;


import ImageClassificationAndRetrieval.Utilities.OpenImaj.ImageIO;
import org.openimaj.feature.local.list.LocalFeatureList;
import org.openimaj.image.feature.dense.gradient.dsift.ByteDSIFTKeypoint;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class LocalKPData extends LocalImageData implements KPData {
    protected LocalFeatureList<ByteDSIFTKeypoint> keypoints;

    public LocalKPData() {
    }

    public LocalKPData(String path, String type, LocalFeatureList<ByteDSIFTKeypoint> keypoints) {
        super(path, type);
        this.keypoints = keypoints;
    }

    @Override
    public void readASCII(Scanner in) throws IOException {
        this.path = in.next();
        this.type = in.next();

        this.keypoints = ImageIO.readKeypoints(in.nextLine().trim());
    }

    @Override
    public void writeASCII(PrintWriter out) throws IOException {
        out.write(this.path + " ");
        out.write(this.type + " ");

        ImageIO.writeASCII(this.keypoints, out);

        out.println();
    }

    public static List<KPData> readList(String path) throws IOException {
        List<KPData> keypoints = new ArrayList<KPData>();

        Scanner in = new Scanner(new File(path));
        while (in.hasNext()) {
            KPData data = new LocalKPData();
            data.readASCII(in);
            keypoints.add(data);
        }
        in.close();

        return keypoints;
    }

    @Override
    public LocalFeatureList<ByteDSIFTKeypoint> getKeypoints() {
        return this.keypoints;
    }
}

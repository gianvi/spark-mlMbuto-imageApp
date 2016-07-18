package ImageClassificationAndRetrieval.Utilities.OpenImaj;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.openimaj.feature.DoubleFV;
import org.openimaj.feature.local.list.LocalFeatureList;
import org.openimaj.feature.local.list.MemoryLocalFeatureList;
import org.openimaj.image.feature.dense.gradient.dsift.ByteDSIFTKeypoint;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Contains various methods to help with image I/O tasks
 */
public class ImageIO {
    public static void writeASCII(LocalFeatureList<ByteDSIFTKeypoint> keypoints, PrintWriter out) {
        for (ByteDSIFTKeypoint point : keypoints) {
            out.write(point.x + " " + point.y + " " + point.energy + " ");
            out.write(Hex.encodeHex(point.descriptor));
            out.write("\t");
        }
    }

    public static void writeASCII(DoubleFV features, PrintWriter out) {
        for (int i = 0; i < features.values.length; i++) {
            out.print(features.values[i] + " ");
        }
    }

    public static LocalFeatureList<ByteDSIFTKeypoint> readKeypoints(String str) {
        List<String> strings = Arrays.asList(str.split("\t"));

        List<ByteDSIFTKeypoint> points = new ArrayList<ByteDSIFTKeypoint>();
        for (String string : strings) {
            try {
                Scanner keypointScanner = new Scanner(string.trim());
                float x = Float.parseFloat(keypointScanner.next());
                float y = Float.parseFloat(keypointScanner.next());
                float energy = Float.parseFloat(keypointScanner.next());

                byte[] descriptor = Hex.decodeHex(keypointScanner.next().toCharArray());

                points.add(new ByteDSIFTKeypoint(x, y, descriptor, energy));
            } catch (DecoderException e) {
                throw new RuntimeException("Could not decode hex data " + string);
            }
        }

        return new MemoryLocalFeatureList<ByteDSIFTKeypoint>(points);
    }


}

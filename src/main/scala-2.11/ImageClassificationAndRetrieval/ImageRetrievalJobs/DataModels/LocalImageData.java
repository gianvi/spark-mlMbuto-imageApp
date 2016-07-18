package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;

import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;
import org.openimaj.io.Writeable;

import java.io.*;
import java.util.List;

public abstract class LocalImageData implements ImageData {
    protected String path;
    protected String type;

    public LocalImageData() {
    }

    public LocalImageData(String path, String type) {
        this.path = path;
        this.type = type;
    }


    @Override
    public String asciiHeader() {
        return this.getClass().getName() + " ";
    }

    @Override
    public byte[] binaryHeader() {
        return (this.getClass().getName().substring(0, 2) + "FV").getBytes();
    }


    @Override
    public void readBinary(DataInput in) throws IOException {
        throw new IOException("Not actually implemented...");
    }

    @Override
    public void writeBinary(DataOutput out) throws IOException {
        throw new IOException("Not actually implemented...");
    }

    public FImage getImage() {
        try {
            return ImageUtilities.readF(new File(this.path));
        } catch (IOException e) {
            throw new RuntimeException("Could not find image " + this.path);
        }
    }

    public String getType() {
        return this.type;
    }

    public String getPath() {
        return this.path;
    }

    public static void writeList(String path, List<? extends ImageData> list) throws IOException {

        File file = new File(path);
        file.getParentFile().mkdirs();

        PrintWriter printWriter = new PrintWriter(file);

        for (Writeable item : list) {
            item.writeASCII(printWriter);
        }
        printWriter.close();
    }
}

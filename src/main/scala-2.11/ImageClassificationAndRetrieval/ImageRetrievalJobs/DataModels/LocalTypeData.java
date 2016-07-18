package ImageClassificationAndRetrieval.ImageRetrievalJobs.DataModels;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class LocalTypeData extends LocalImageData implements TypeData {

    public LocalTypeData() {
    }

    public LocalTypeData(String path, String type) {
        super(path, type);
    }

    @Override
    public void readASCII(Scanner in) throws IOException {
        this.path = in.next();
        this.type = in.nextLine();
    }

    @Override
    public void writeASCII(PrintWriter out) throws IOException {
        out.write(this.path + " ");
        out.write(this.type + " ");

        out.println();
    }

    public static List<TypeData> readList(String path) throws IOException {
        List<TypeData> trainingData = new ArrayList<TypeData>();

        Scanner in = new Scanner(new File(path));
        while (in.hasNext()) {
            TypeData data = new LocalTypeData();
            data.readASCII(in);
            trainingData.add(data);
        }
        in.close();

        return trainingData;
    }
}

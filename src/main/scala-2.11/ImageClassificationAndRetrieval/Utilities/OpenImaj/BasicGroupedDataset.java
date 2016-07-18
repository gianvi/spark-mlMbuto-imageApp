package ImageClassificationAndRetrieval.Utilities.OpenImaj;

import org.openimaj.data.dataset.Dataset;
import org.openimaj.data.dataset.GroupedDataset;
import org.openimaj.data.dataset.ListDataset;

import java.util.*;


public class BasicGroupedDataset<CLASS> extends HashMap<String, ListDataset<CLASS>>
        implements GroupedDataset<String, ListDataset<CLASS>, CLASS> {

    public List<CLASS> getValues() {
        Vector<CLASS> values = new Vector<CLASS>();

        for (Dataset<CLASS> dataset : this.values()) {
            for (CLASS item : dataset) {
                values.add(item);
            }
        }

        return values;
    }

    @Override
    public ListDataset<CLASS> getInstances(String s) {
        return this.get(s);
    }

    @Override
    public Set<String> getGroups() {
        return this.keySet();
    }

    @Override
    public CLASS getRandomInstance(String s) {
        return this.get(s).getRandomInstance();
    }

    @Override
    public CLASS getRandomInstance() {
        List<CLASS> values = this.getValues();

        int idx = new Random().nextInt(values.size());
        return values.get(idx);
    }

    @Override
    public int numInstances() {
        return this.getValues().size();
    }

    @Override
    public Iterator<CLASS> iterator() {
        return this.getValues().iterator();
    }
}

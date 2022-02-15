import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SetMatcher implements DataMatcher {

    List<String> entries;

    SetMatcher(String setPath) throws IOException, URISyntaxException {
        this.entries = Files.readAllLines(Paths.get(new URI(new File(setPath).toURI().toString())));
    }

    Boolean findMatch(String data) {
        for (String entry : entries) {
            if (data.equals(entry)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean isMatch(String data) {
        return findMatch(data);
    }
}

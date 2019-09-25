import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUtil {

    /**
     * Searches for files with specified beginning and end in the title.
     * @param dirPath path to the directory to search
     * @param begins beginning of the string
     * @param ends end of the string
     * @return list of files that were found
     */
    public static File[] searchFiles(String dirPath, String begins, String ends) {
    File dir = new File(dirPath);

    File[] fileList = dir.listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return name.startsWith(begins) && name.endsWith(ends);
        }
    });
    return fileList;
    }

    /**
     * Reads a file and returns the lines of the file as a list of strings.
     * @param filePath path to the file
     * @return list containing the lines of the file as strings
     */
    public static List<String> readFileHelper(String filePath) {
        List<String> list = null;
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            list = lines.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Invalid file path while trying to read file.");
        }
        return list;
    }

}

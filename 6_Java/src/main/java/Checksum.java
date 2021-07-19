import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Checksum {
    public static String getFileCheckSum(String path) {
        String sha256 = null;
        try (InputStream is = Files.newInputStream(Paths.get(path))) {
            sha256 = DigestUtils.sha256Hex(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sha256;
    }


}

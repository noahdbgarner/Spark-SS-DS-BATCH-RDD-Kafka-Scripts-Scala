import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.*;

public class StaticMethods {


    public void p(BigInteger s) {
        System.out.println(s);
    }

    public void p(String s) {
        System.out.println(s);
    }

    public static void p(int printMe) {
        System.out.println(printMe);
    }

    public static void p(double printMe) {
        System.out.println(printMe);
    }

    public static void p(long printMe) {
        System.out.println(printMe);
    }

}

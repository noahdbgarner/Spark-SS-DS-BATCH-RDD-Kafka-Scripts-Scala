import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Random;

public class LogGenerator {

    //These are for the logFile random sentence generator portion
    final static int NO_WORDS = 7;
    final static String SPACE = " ";
    final static String PERIOD = ".";

    //generates psuedo-random log data for spark scripts to work with on streaming
    //sends to a TCP port and writes to a file so that you can either work with
    //live streaming random data, or work with solid state files w/ DS or RDDs
    public static void logGenerator(String IP, int PORT, int runtime, int delay) throws IOException, InterruptedException {

        //set up file and list to hold random generated data
        Path file = Paths.get("src/main/resources/javaLogGenLogs.txt");
        ArrayList<String> lines = new ArrayList<>();

        //7 random data items to generalize for rand function helper method
        String article[] = { "the", "a", "one", "some", "any", "an", "my"};
        String noun[] = { "boy", "girl", "dog", "town", "car", "Google", "toilet"};
        String verb[] = { "drove", "jumped", "ran", "walked", "skipped", "hiked", "fell"};
        String preposition[] = { "to", "from", "over", "under", "on", "below", "near"};
        String adjective[] = {"aggressive", "agreeable", "ambitious", "brave", "calm", "delightful", "ugly"};
        String sentence;
        int [] statusCodes =  {4000, 2801, 3420, 8008, 1907, 5801, 4266};
        int code;

        long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < runtime) {

            //init a new line
            StringBuilder completeLine = new StringBuilder();

            //get our random sentence, length 8 words. Very simple sentences.
            sentence = article[rand()];
            sentence = sentence.substring(0, 1).toUpperCase() + sentence.substring(1);
            sentence += SPACE + adjective[rand()];
            sentence += SPACE + noun[rand()] + SPACE;
            sentence += (verb[rand()] + SPACE + preposition[rand()]);
            sentence += (SPACE + article[rand()] + SPACE + adjective[rand()] + SPACE + noun[rand()]);
            sentence += PERIOD;

            //get our random code
            code = statusCodes[rand()];

            //create our date/time info
            LocalDate todaysDate = LocalDate.now();
            LocalTime theTime = LocalTime.now();

            //append all data to a line delimited with spaces
            completeLine.append("<"+todaysDate+" ");
            completeLine.append(theTime+"> ");
            completeLine.append(code+" ");
            completeLine.append(sentence);

            //add one piece to our list as a String
            lines.add(completeLine.toString());
        }

        //so we only open it once, last time we were repeatedly open/closing it
        //When conenction is refused, try:
        Socket socket = new Socket(IP, PORT);
        int lineNum = 1;
        //loop through all lines of data and send them to the socket
        for (String line : lines) {
            OutputStreamWriter osw;
            try {
                osw =new OutputStreamWriter(socket.getOutputStream(), "UTF-8");
                osw.write(line, 0, line.length());
                osw.append("\n");
                osw.flush();
            } catch (IOException e) {
                //if this runs too long and u stop execution it won't print to file.
                System.err.print(e);
            }
            //this is to slow the output to the socket so that our JVM doesn't crash
            System.out.println("Line "+lineNum+" "+line);
            lineNum += 1;
            Thread.sleep(delay);
        }

        try {
            Files.write(file, lines, StandardCharsets.UTF_8);
        }

        catch (Exception e) {
            System.out.println("Couldn't write to file");
        }

        //This means there was data to be read
        if (socket != null) {
            socket.close();
        }
    }

    //helper rand gen for logGenerator method
    static int rand(){
        Random r = new Random();
        int ri = r.nextInt() % NO_WORDS;
        if ( ri < 0 )
            ri += NO_WORDS;
        return ri;
    }

    //we need to say that main will throw exceptions to enable logGenerator to run
    public static void main(String[] args) throws IOException, InterruptedException{

        logGenerator("127.0.0.1", 9997, 10000, 50);

    }

}

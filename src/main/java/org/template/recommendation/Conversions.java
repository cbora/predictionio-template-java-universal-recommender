package org.template.recommendation;
import org.slf4j.Logger;
import scala.Tuple2;
import java.util.List;

/** Utility conversions for IndexedDatasetSpark */
public class Conversions {

    /**
     * Print stylized ActionML title
     * @param logger Logger to print with
     */
    public static void drawActionML(Logger logger) {
        String actionML = "" +
                "\n\t" +
                "\n\t               _   _             __  __ _" +
                "\n\t     /\\       | | (_)           |  \\/  | |" +
                "\n\t    /  \\   ___| |_ _  ___  _ __ | \\  / | |" +
                "\n\t   / /\\ \\ / __| __| |/ _ \\| '_ \\| |\\/| | |" +
                "\n\t  / ____ \\ (__| |_| | (_) | | | | |  | | |____" +
                "\n\t /_/    \\_\\___|\\__|_|\\___/|_| |_|_|  |_|______|" +
                "\n\t" +
                "\n\t";

        logger.info(actionML);
    }

    /**
     * Print informational chart
     * @param title title of information chart
     * @param dataMap list of (key, value) pairs to print as rows
     * @param logger Logger to use for printing
     */
    public static void drawInfo(String title, List<Tuple2<String, Object>> dataMap, Logger logger) {
        String leftAlignFormat = "║ %-30s%-28s ║";

        String line = strMul("═", 60);

        String preparedTitle = String.format("║ %-58s ║", title);

        StringBuilder data = new StringBuilder();
        for (Tuple2<String, Object> t : dataMap) {
            data.append(String.format(leftAlignFormat, t._1, t._2));
            data.append("\n\t");
        }

        String info = "" +
                "\n\t╔" + line + "╗" +
                "\n\t"  + preparedTitle +
                "\n\t"  + data.toString().trim() +
                "\n\t╚" + line + "╝" +
                "\n\t";
        logger.info(info);
    }

    /**
     * Append n copies of str to create new String
     * @param str String to copy
     * @param n How many times to copy
     * @return String created by combining n copies of str
     */
    private static String strMul(String str, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(str);
        return sb.toString();
    }
}

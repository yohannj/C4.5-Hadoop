/**
 * This file is part of an implementation of C4.5 by Yohann Jardin.
 * 
 * This implementation of C4.5 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This implementation of C4.5 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this implementation of C4.5. If not, see <http://www.gnu.org/licenses/>.
 */

package cutFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Scanner;

public class Main {
    private static final int MAX_LINE_PER_FILE = 1670000;
    private static final String SPLIT_CHAR = "\t";

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: cutFile/Main <input path> <output path>");
            System.exit(-1);
        }

        Scanner in = new Scanner(new File(args[0]));
        int file_index = 0;

        while (in.hasNextLine()) {
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1] + "/" + (++file_index)), "utf-8"))) {
                String[] splitted_line;
                int nb_line = 0;

                while (in.hasNextLine() && nb_line < MAX_LINE_PER_FILE) {
                    splitted_line = in.nextLine().split(",");
                    if (nb_line != 0) {
                        writer.write("\n");
                    }
                    ++nb_line;

                    writer.write(splitted_line[1] + SPLIT_CHAR + splitted_line[2] + SPLIT_CHAR + splitted_line[3] + SPLIT_CHAR
                                 + splitted_line[6] + SPLIT_CHAR + splitted_line[11] + SPLIT_CHAR + splitted_line[20] + SPLIT_CHAR
                                 + splitted_line[21] + SPLIT_CHAR + splitted_line[41].replace(".", ""));
                }

            }
        }

        in.close();

    }

}

package com.jd.binlog.util;

/**
 * Created by jingdi on 18-2-6.
 */
public class CharUtils {

    public static String toUpperCase(String arg) {
        char[] vals = arg.toCharArray();
        for (int index = 0; index < arg.length(); index++) {
            char val = arg.charAt(index);
            switch (val) {
                case 'a':
                    vals[index] = 'A';
                    break;
                case 'b':
                    vals[index] = 'B';
                    break;
                case 'c':
                    vals[index] = 'C';
                    break;
                case 'd':
                    vals[index] = 'D';
                    break;
                case 'e':
                    vals[index] = 'E';
                    break;
                case 'f':
                    vals[index] = 'F';
                    break;
                case 'g':
                    vals[index] = 'G';
                    break;
                case 'h':
                    vals[index] = 'H';
                    break;
                case 'i':
                    vals[index] = 'I';
                    break;
                case 'j':
                    vals[index] = 'J';
                    break;
                case 'k':
                    vals[index] = 'K';
                    break;
                case 'l':
                    vals[index] = 'L';
                    break;
                case 'm':
                    vals[index] = 'M';
                    break;
                case 'n':
                    vals[index] = 'N';
                    break;
                case 'o':
                    vals[index] = 'O';
                    break;
                case 'p':
                    vals[index] = 'P';
                    break;
                case 'q':
                    vals[index] = 'Q';
                    break;
                case 'r':
                    vals[index] = 'R';
                    break;
                case 's':
                    vals[index] = 'S';
                    break;
                case 't':
                    vals[index] = 'T';
                    break;
                case 'u':
                    vals[index] = 'U';
                    break;
                case 'v':
                    vals[index] = 'V';
                    break;
                case 'w':
                    vals[index] = 'W';
                    break;
                case 'x':
                    vals[index] = 'X';
                    break;
                case 'y':
                    vals[index] = 'Y';
                    break;
                case 'z':
                    vals[index] = 'Z';
                    break;
                default:
                    break;
            }
        }
        return new String(vals);
    }


    public static String toLowerCase(String arg) {
        char[] vals = arg.toCharArray();
        for (int index = 0; index < arg.length(); index++) {
            char val = arg.charAt(index);
            switch (val) {
                case 'A':
                    vals[index] = 'a';
                    break;
                case 'B':
                    vals[index] = 'b';
                    break;
                case 'C':
                    vals[index] = 'c';
                    break;
                case 'D':
                    vals[index] = 'd';
                    break;
                case 'E':
                    vals[index] = 'e';
                    break;
                case 'F':
                    vals[index] = 'f';
                    break;
                case 'G':
                    vals[index] = 'g';
                    break;
                case 'H':
                    vals[index] = 'h';
                    break;
                case 'I':
                    vals[index] = 'i';
                    break;
                case 'J':
                    vals[index] = 'j';
                    break;
                case 'K':
                    vals[index] = 'k';
                    break;
                case 'L':
                    vals[index] = 'l';
                    break;
                case 'M':
                    vals[index] = 'm';
                    break;
                case 'N':
                    vals[index] = 'n';
                    break;
                case 'O':
                    vals[index] = 'o';
                    break;
                case 'P':
                    vals[index] = 'p';
                    break;
                case 'Q':
                    vals[index] = 'q';
                    break;
                case 'R':
                    vals[index] = 'r';
                    break;
                case 'S':
                    vals[index] = 's';
                    break;
                case 'T':
                    vals[index] = 't';
                    break;
                case 'U':
                    vals[index] = 'u';
                    break;
                case 'V':
                    vals[index] = 'v';
                    break;
                case 'W':
                    vals[index] = 'w';
                    break;
                case 'X':
                    vals[index] = 'x';
                    break;
                case 'Y':
                    vals[index] = 'y';
                    break;
                case 'Z':
                    vals[index] = 'z';
                    break;
                default:
                    break;
            }
        }
        return new String(vals);
    }

    public static void main(String[] args) {
        String abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZXYZNOYOUSEE";
        System.err.println(toLowerCase(abc));
    }
}
